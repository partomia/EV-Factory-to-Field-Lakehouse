"""
Vehicle telemetry for the live subset onto evx.vehicle.telemetry, with per-minute control counts on evx.control.

The live subset (generator fleet truth, live_subset = true; about 2,000 vehicles at default scale) rides 1 to 3
trips a day in IST commute windows; while riding each vehicle sends one reading every 30 s (speed, odometer,
SoC, pack voltage and current, cell temperature max / min, cell voltage min / max, BMS firmware, geohash-5).
Values come from the same fleet profile as the legacy daily summaries (daily distance, fast-charge share,
contaminated pack, cell voltage spread) and carry the same failure precursors in the 25 days before a field
event (thermal: hotter cells; imbalance: wider cell voltage spread). Deterministic per (VIN, date).

Planted stream faults (config/lakehouse.json faults):
  fahrenheit_firmware  from its date, N vehicles report firmware BMS-3.5.0-F and cell temperatures in degrees F
  stuck_soc            on its date, N vehicles report one SoC value all day
  late / out of order  ~1 % 2-8 min late, ~0.1 % 15-30 min late (beyond the watermark), ~2 % reordered

Every event-time minute gets a control message with the count sent per topic, once no reading of that minute
can still be sent: the stream reconciliation's control total. All data is synthetic.

  python -m stream.producer.telemetry --out data/stream --dates 2026-11-06,2026-11-07    # file sink (CI)
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from generators.core import IST, rng, stable_int  # noqa: E402

CFG = json.loads((ROOT / "config" / "streaming.json").read_text())
LH = json.loads((ROOT / "config" / "lakehouse.json").read_text())
P = CFG["producer"]
TOPIC = {k: v["name"] for k, v in CFG["topics"].items()}
FAULTS = LH["faults"]


def iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


def load_fleet(rows) -> list[dict]:
    """Live-subset vehicles from fleet truth rows (dicts), in VIN order."""
    return sorted((r for r in rows if r.get("live_subset")), key=lambda r: r["vin"])


def fleet_from_files(generated: Path) -> list[dict]:
    files = sorted((generated / "_truth" / "fleet").glob("part-*.json"))
    return load_fleet(json.loads(line) for f in files for line in f.read_text().splitlines() if line.strip())


def planted(fleet: list[dict], day: date) -> dict[str, set[str]]:
    """VINs carrying each planted stream fault on `day` (a stable choice by VIN hash)."""
    def pick(name: str, n: int) -> set[str]:
        return {v["vin"] for v in sorted(fleet, key=lambda v: stable_int(name, v["vin"]))[:n]}

    fw, soc = FAULTS["fahrenheit_firmware"], FAULTS["stuck_soc"]
    return {"fahrenheit": pick("fahrenheit", fw["vehicles"]) if day >= date.fromisoformat(fw["from"]) else set(),
            "stuck_soc": pick("stuck_soc", soc["vehicles"]) if day.isoformat() == soc["date"] else set()}


def vehicle_day(v: dict, day: date, faults: dict, interval: int, run_id: str) -> list[tuple[float, dict]]:
    """(send time, message) for one vehicle's rides on one date."""
    r = rng("telemetry", v["vin"], day.isoformat())
    delivery = date.fromisoformat(v["delivery_date"])
    if day < delivery or r.random() < 0.12:
        return []
    dist = max(2.0, r.gauss(v["daily_km"], 6))
    prec = 0.0
    if v.get("event_date"):
        ev = date.fromisoformat(v["event_date"])
        if ev - timedelta(days=25) <= day <= ev:
            prec = min(1.0, max(0.0, 1 - (ev - day).days / 25))
    kind = v.get("event_kind")
    t_amb = 29 + r.gauss(0, 2) + 8 * v["fast_charge_share"] + (1.2 if v.get("contaminated") else 0.0) \
        + (6 * prec if kind == "thermal" else 1.5 * prec)
    dv_base = v["v_spread_mv"] * 0.8 + (25 * prec if kind == "imbalance" else 6 * prec)
    fahrenheit, stuck = v["vin"] in faults["fahrenheit"], v["vin"] in faults["stuck_soc"]
    fw = "BMS-3.5.0" + CFG["units"]["fahrenheit_fw_suffix"] if fahrenheit else v["bms_fw"]
    odo = v["daily_km"] * (day - delivery).days
    soc = min(100.0, r.gauss(92, 5))
    stuck_value = round(soc, 1)
    n_trips = r.randint(*P["trips_per_day"])
    windows = P["trip_start_hours_ist"][:n_trips] if n_trips < 3 else P["trip_start_hours_ist"]
    out, seq = [], 0
    for k, (h0, h1) in enumerate(windows):
        trip_km = dist / n_trips
        speed_avg = max(10.0, r.gauss(P["avg_speed_kmh"], 3))
        dur_s = trip_km / speed_avg * 3600
        start = datetime(day.year, day.month, day.day, tzinfo=IST) + timedelta(hours=r.uniform(h0, h1))
        if k and r.random() < 0.4:
            soc = min(100.0, soc + r.uniform(20, 45))   # charged between trips
        t, heat = 0.0, 0.0
        while t <= dur_s:
            ts = start + timedelta(seconds=t + r.uniform(0, 0.9))
            speed = max(0.0, min(85.0, r.gauss(speed_avg, 8)))
            step_km = speed * interval / 3600
            odo += step_km
            soc = max(3.0, soc - step_km * 0.45)
            heat = min(5.0, heat + interval / 600)
            t_max = t_amb + heat + r.gauss(0, 0.3)
            t_min = t_max - r.uniform(3, 6)
            dv = max(1.0, dv_base + r.gauss(0, 1.5))
            v_mid = 3300 + soc * 8.5
            to_unit = (lambda c: round(c * 9 / 5 + 32, 1)) if fahrenheit else (lambda c: round(c, 1))
            seq += 1
            msg = {"event_id": f"{v['vin']}:{int(ts.timestamp() * 1000)}", "vin": v["vin"], "event_ts": iso(ts),
                   "seq": seq, "trip_no": k + 1, "speed_kmh": round(speed, 1), "odometer_km": round(odo, 2),
                   "soc_pct": stuck_value if stuck else round(soc, 1),
                   "pack_voltage_v": round(46.0 + soc * 0.025 + r.gauss(0, 0.05), 2),
                   "pack_current_a": round(speed * 1.8 + r.gauss(0, 4), 1),
                   "cell_t_max": to_unit(t_max), "cell_t_min": to_unit(t_min),
                   "cell_v_min_mv": round(v_mid - dv / 2, 1), "cell_v_max_mv": round(v_mid + dv / 2, 1),
                   "bms_fw": fw, "geohash5": v["geohash5"],
                   "quality": "GOOD" if r.random() > 0.002 else "UNCERTAIN", "producer_run": run_id}
            u, delay = r.random(), 0.0
            if u < P["late_fraction"]:
                delay = r.uniform(*P["late_seconds"])
            elif u < P["late_fraction"] + P["very_late_fraction"]:
                delay = r.uniform(*P["very_late_seconds"])
            elif u < P["late_fraction"] + P["very_late_fraction"] + P["out_of_order_fraction"]:
                delay = r.uniform(1.0, 4.0)
            out.append((ts.timestamp() + delay, msg))
            t += interval
    return out


class Control:
    """Counts per (event minute, topic); a minute's control message goes out once none of its readings can
    still be sent (the producer's maximum delay)."""

    def __init__(self, sink, run_id: str):
        self.sink, self.run_id, self.hold = sink, run_id, P["control_hold_s"]
        self.counts = defaultdict(lambda: [0, set()])

    def count(self, topic: str, msg: dict) -> None:
        c = self.counts[(msg["event_ts"][:16] + ":00.000Z", topic)]
        c[0] += 1
        c[1].add(msg["vin"])

    def flush(self, now_ts: float, final: bool = False) -> int:
        n = 0
        for key in sorted(self.counts):
            minute, topic = key
            mts = datetime.strptime(minute, "%Y-%m-%dT%H:%M:%S.000Z").replace(tzinfo=timezone.utc).timestamp()
            if final or mts + 60 + self.hold <= now_ts:
                total, vins = self.counts.pop(key)
                self.sink.send(TOPIC["control"], {"minute": minute, "topic": topic, "total": total,
                                                  "vehicles": len(vins), "producer_run": self.run_id},
                               mts + 60 + self.hold)
                n += 1
        return n


def backfill(sink, fleet: list[dict], dates: list[date], interval: int, run_id: str, log=print) -> int:
    """Each date's readings in send-time order, control messages as their minutes close."""
    ctl, sent = Control(sink, run_id), 0
    for day in dates:
        faults = planted(fleet, day)
        msgs = sorted((m for v in fleet for m in vehicle_day(v, day, faults, interval, run_id)), key=lambda x: x[0])
        for send_ts, msg in msgs:
            sink.send(TOPIC["telemetry"], msg, send_ts)
            ctl.count(TOPIC["telemetry"], msg)
            sent += 1
            if sent % 50_000 == 0:
                ctl.flush(send_ts)
        log(f"  {day} {len(msgs):,} readings from {len({m['vin'] for _, m in msgs}):,} vehicles "
            f"(fahrenheit {len(faults['fahrenheit'])}, stuck SoC {len(faults['stuck_soc'])})")
    ctl.flush(0, final=True)
    return sent


class FileSink:
    """JSON lines per topic per hour of send time (<out>/<topic>/part-<yyyymmddHH>.jsonl), one Kafka record
    per line ({"key", "timestamp" (send time, ms), "value" (the message as a JSON string)}), so the streaming
    jobs read files and Kafka through the same parsing."""

    def __init__(self, out: str):
        self.out, self.handles, self.sent = Path(out), {}, 0

    def send(self, topic: str, msg: dict, send_ts: float) -> None:
        hour = datetime.fromtimestamp(send_ts, timezone.utc).strftime("%Y%m%d%H")
        key = (topic, hour)
        if key not in self.handles:
            d = self.out / topic
            d.mkdir(parents=True, exist_ok=True)
            self.handles[key] = open(d / f"part-{hour}.jsonl", "a")
        rec = {"key": msg.get("vin") or msg.get("topic"), "timestamp": int(send_ts * 1000),
               "value": json.dumps(msg, separators=(",", ":"))}
        self.handles[key].write(json.dumps(rec, separators=(",", ":")) + "\n")
        self.sent += 1

    def close(self) -> None:
        for h in self.handles.values():
            h.close()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default=str(ROOT / "data" / "stream"))
    ap.add_argument("--generated", default=str(ROOT / "data" / "generated"))
    ap.add_argument("--dates", default=",".join(LH["calendar"]["live_dates"][:1]))
    ap.add_argument("--interval", type=int, default=P["interval_s"])
    args = ap.parse_args(argv)
    fleet = fleet_from_files(Path(args.generated))
    run_id = f"backfill-{uuid.uuid4().hex[:8]}"
    sink, t0 = FileSink(args.out), time.time()
    try:
        backfill(sink, fleet, [date.fromisoformat(x) for x in args.dates.split(",")], args.interval, run_id)
    finally:
        sink.close()
    print(f"sent {sink.sent:,} messages from {len(fleet):,} live vehicles in {time.time() - t0:.0f} s (run {run_id})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
