"""
Shared pieces of the M2 stream producers (dtc, qc, line): config, delays, per-minute control totals and the file
sink. The telemetry producer keeps its own copies (its behaviour is frozen at M1); the shapes are the same:

  control   one evx.control message per (event minute, topic): total sent, distinct keys (VINs for dtc, inspected
            units for qc, stations for line), producer run; sent once none of the minute's messages can still
            follow (control_hold_s exceeds the producer's largest delay)
  files     <out>/<topic>/part-<yyyymmddHH>-<producer>.jsonl, one Kafka record per line ({"key", "timestamp"
            (send time, ms), "value" (the message as a JSON string)}); the producer suffix keeps each run's files
            new to a file-source stream that already read the telemetry producer's files

All data is synthetic.
"""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CFG = json.loads((ROOT / "config" / "streaming.json").read_text())
LH = json.loads((ROOT / "config" / "lakehouse.json").read_text())
TOPIC = {k: v["name"] for k, v in CFG["topics"].items()}
HISTORY_END = LH["calendar"]["history_end"]
KEY_FIELDS = ("vin", "station_id", "unit_id", "topic")


def iso_utc(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


def parse_ts(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def minute_utc(s: str) -> str:
    """The UTC event minute of an ISO timestamp (Z or offset), in the control message's format."""
    return parse_ts(s).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:00.000Z")


def record_key(msg: dict) -> str | None:
    """Kafka key: the VIN, station or inspected unit; the topic for control messages."""
    return next((msg[k] for k in KEY_FIELDS if msg.get(k)), None)


def delay(r, p: dict) -> float:
    """Seconds between the event and its send time: network latency, plus late / very late / reordered tails."""
    lat = r.uniform(*p["latency_s"])
    u = r.random()
    if u < p["late_fraction"]:
        return lat + r.uniform(*p["late_seconds"])
    if u < p["late_fraction"] + p["very_late_fraction"]:
        return lat + r.uniform(*p["very_late_seconds"])
    if u < p["late_fraction"] + p["very_late_fraction"] + p["out_of_order_fraction"]:
        return lat + r.uniform(1.0, 4.0)
    return lat


def refuse_history(day) -> None:
    """Live dates only: history dates are already in the batch sources (dtc_history, cell_qc, line_event)."""
    if day.isoformat() <= HISTORY_END:
        raise ValueError(f"{day} is a history date (<= {HISTORY_END}); the batch sources already hold it")


class Control:
    """Counts per (event minute, topic) of one producer; a minute's control message goes out once none of its
    messages can still be sent (`hold_s` after the minute closes)."""

    def __init__(self, sink, run_id: str, hold_s: float, time_field: str):
        self.sink, self.run_id, self.hold, self.time_field = sink, run_id, hold_s, time_field
        self.counts = defaultdict(lambda: [0, set()])

    def count(self, topic: str, msg: dict) -> None:
        c = self.counts[(minute_utc(msg[self.time_field]), topic)]
        c[0] += 1
        c[1].add(record_key(msg))

    def flush(self, now_ts: float, final: bool = False) -> int:
        n = 0
        for key in sorted(self.counts):
            minute, topic = key
            mts = datetime.strptime(minute, "%Y-%m-%dT%H:%M:%S.000Z").replace(tzinfo=timezone.utc).timestamp()
            if final or mts + 60 + self.hold <= now_ts:
                total, keys = self.counts.pop(key)
                self.sink.send(TOPIC["control"], {"minute": minute, "topic": topic, "total": total,
                                                  "vehicles": len(keys), "producer_run": self.run_id},
                               mts + 60 + self.hold)
                n += 1
        return n


def send_sorted(sink, ctl: Control, topic: str, items: list[tuple[float, dict]], flush_every: int = 50_000) -> int:
    """Send (send time, message) pairs in send-time order, counting each into the control totals."""
    sent = 0
    for send_ts, msg in sorted(items, key=lambda x: (x[0], x[1]["event_id"])):
        sink.send(topic, msg, send_ts)
        ctl.count(topic, msg)
        sent += 1
        if sent % flush_every == 0:
            ctl.flush(send_ts)
    return sent


class FileSink:
    """JSON lines per topic per hour of send time, one Kafka record per line (see the module docstring)."""

    def __init__(self, out: str, tag: str):
        self.out, self.tag, self.handles, self.sent = Path(out), tag, {}, 0

    def send(self, topic: str, msg: dict, send_ts: float) -> None:
        hour = datetime.fromtimestamp(send_ts, timezone.utc).strftime("%Y%m%d%H")
        key = (topic, hour)
        if key not in self.handles:
            d = self.out / topic
            d.mkdir(parents=True, exist_ok=True)
            self.handles[key] = open(d / f"part-{hour}-{self.tag}.jsonl", "a")
        rec = {"key": record_key(msg), "timestamp": int(send_ts * 1000), "value": json.dumps(msg, separators=(",", ":"))}
        self.handles[key].write(json.dumps(rec, separators=(",", ":")) + "\n")
        self.sent += 1

    def close(self) -> None:
        for h in self.handles.values():
            h.close()


class ListSink:
    """In-memory sink (tests, dry runs)."""

    def __init__(self):
        self.msgs: list[tuple[str, dict, float]] = []

    def send(self, topic: str, msg: dict, send_ts: float) -> None:
        self.msgs.append((topic, msg, send_ts))
