"""
Station cycle events onto evx.factory.line, with per-minute control totals (on mes_ts) on evx.control.

The events of a business date come from the digital-twin generator, generators.twin.station_cycle_events(cfg,
day, scale) (lane E), the same records its batch source line_event/<date>/ carries, so stream and batch
reconcile per date. The generator is imported when the producer runs, not at module import; `--events fake`
selects stream/producer/line_fake.py instead (a test double with the same fields, never chosen implicitly).

Message: the line-event fields (event_id, station_id, line_id, area, unit_id, cycle_start_ts, event_ts, mes_ts,
cycle_s, status, shift) plus producer_run. Sent at mes_ts (the MES receive time, the trusted clock) plus
latency, with the late / out-of-order tails of config/streaming.json producers.line. Live dates only. All data
is synthetic.

  python -m stream.producer.line --out data/stream --dates 2026-11-06 [--scale 0.05] [--events twin|fake]
"""
from __future__ import annotations

import argparse
import sys
import time
import uuid
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from generators.core import rng  # noqa: E402
from stream.producer import common as K  # noqa: E402

P = K.CFG["producers"]["line"]
FIELDS = ("event_id", "station_id", "line_id", "area", "unit_id", "cycle_start_ts", "event_ts", "mes_ts",
          "cycle_s", "status", "shift")


def event_source(name: str = "twin"):
    """station_cycle_events(cfg, day, scale) -> list[dict] of the chosen source."""
    if name == "twin":
        from generators.twin import station_cycle_events
    elif name == "fake":
        from stream.producer.line_fake import station_cycle_events
    else:
        raise ValueError(f"unknown line event source {name!r}")
    return station_cycle_events


def message(event: dict, run_id: str) -> dict:
    missing = [f for f in FIELDS if f not in event]
    if missing:
        raise KeyError(f"line event {event.get('event_id')} lacks {missing}")
    msg = {f: event[f] for f in FIELDS}
    msg["producer_run"] = run_id
    return msg


def timed(events: list[dict], run_id: str) -> list[tuple[float, dict]]:
    out = []
    for e in events:
        m = message(e, run_id)
        out.append((K.parse_ts(m["mes_ts"]).timestamp() + K.delay(rng("line-stream", m["event_id"]), P), m))
    return out


def backfill(sink, cfg: dict, dates: list[date], scale: float, run_id: str, events=None, log=print) -> int:
    """events: station_cycle_events(cfg, day, scale) (default: generators.twin's)."""
    source = events or event_source("twin")
    ctl, sent = K.Control(sink, run_id, P["control_hold_s"], "mes_ts"), 0
    for day in dates:
        K.refuse_history(day)
        evs = source(cfg, day, scale)
        sent += K.send_sorted(sink, ctl, K.TOPIC["line"], timed(evs, run_id))
        log(f"  {day} {len(evs):,} line events from {len({e['station_id'] for e in evs}):,} stations "
            f"({sum(1 for e in evs if e['status'] != 'OK'):,} not OK)")
    ctl.flush(0, final=True)
    return sent


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default=str(ROOT / "data" / "stream"))
    ap.add_argument("--dates", default=",".join(K.LH["calendar"]["live_dates"][:1]))
    ap.add_argument("--scale", type=float, default=None, help="default: the config's generator.scale, else 1.0")
    ap.add_argument("--events", default="twin", choices=["twin", "fake"])
    args = ap.parse_args(argv)
    sys.path.insert(0, str(ROOT / "cde" / "jobs"))
    import evx_common as C

    cfg = C.config()
    scale = args.scale if args.scale is not None else float(cfg.get("generator", {}).get("scale", 1.0))
    run_id = f"backfill-{uuid.uuid4().hex[:8]}"
    sink, t0 = K.FileSink(args.out, "line"), time.time()
    try:
        backfill(sink, cfg, [date.fromisoformat(x) for x in args.dates.split(",")], scale, run_id,
                 events=event_source(args.events))
    finally:
        sink.close()
    print(f"sent {sink.sent:,} messages at scale {scale} in {time.time() - t0:.0f} s (run {run_id})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
