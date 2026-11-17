"""
Diagnostic trouble codes of the live subset onto evx.vehicle.dtc, with per-minute control totals on evx.control.

Batch and stream are disjoint by date: generators/field.py writes dtc_history only up to the history end
(calendar.history_end, 2026-11-05) and drops every later DTC, so the live dates have no batch DTC rows for any
vehicle. This producer refuses history dates and emits, for each live date and live-subset vehicle (the fleet the
telemetry producer uses):

  battery   a vehicle whose field event (fleet event_date / event_kind) is on or before the date repeats its
            battery code every second day, 1 to 4 times from the event date, as the batch history does
            (BMS-T01 thermal: CRITICAL at onset, then HIGH; BMS-I02 imbalance: MEDIUM)
  other     Poisson(volumes.non_battery_dtc_per_vehicle_month / 30) non-battery codes per riding day, the batch
            history's rate (LOW; ABS-W30 CRITICAL with producers.dtc.critical_share)

Event ids (DTS-<VIN>-<yyyymmdd>-<nn>) cannot collide with the batch ids (DTC-<vkey>-<nnn>). gold.fact_dtc stays
batch-sourced; live DTCs live in bronze.vehicle_dtc_event and feed the NiFi alert router. Firmware is the day's
BMS firmware (the planted degrees-F firmware from its date). Deterministic per (VIN, date); late and
out-of-order tails from config/streaming.json producers.dtc. All data is synthetic.

  python -m stream.producer.dtc --out data/stream --dates 2026-11-06,2026-11-07
"""
from __future__ import annotations

import argparse
import math
import sys
import time
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from generators.core import IST, rng  # noqa: E402
from generators.field import BATTERY_DTC, OTHER_DTC  # noqa: E402
from stream.producer import common as K  # noqa: E402
from stream.producer import telemetry as T  # noqa: E402

P = K.CFG["producers"]["dtc"]
FAULTS = K.LH["faults"]
NON_BATTERY_PER_DAY = K.LH["volumes"]["non_battery_dtc_per_vehicle_month"] / 30.0


def _poisson(r, lam: float) -> int:
    L, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= r.random()
        if p <= L:
            return k
        k += 1


def vehicle_day(v: dict, day: date, fahrenheit: set[str], run_id: str) -> list[tuple[float, dict]]:
    """(send time, message) for one vehicle's DTCs on one live date."""
    delivery = date.fromisoformat(v["delivery_date"])
    if day < delivery:
        return []
    r = rng("dtc-stream", v["vin"], day.isoformat())
    codes = []
    if v.get("event_date") and v.get("event_kind") in BATTERY_DTC:
        ev = date.fromisoformat(v["event_date"])
        repeats = 1 + rng("dtc-stream-repeats", v["vin"]).randint(0, 3)
        j, rest = divmod((day - ev).days, 2)
        if 0 <= j < repeats and rest == 0:
            code, desc = BATTERY_DTC[v["event_kind"]]
            sev = ("CRITICAL" if j == 0 else "HIGH") if v["event_kind"] == "thermal" else "MEDIUM"
            codes.append((code, desc, sev))
    for _ in range(_poisson(r, NON_BATTERY_PER_DAY)):
        code, desc = r.choice(OTHER_DTC)
        codes.append((code, desc, "CRITICAL" if r.random() < P["critical_share"].get(code, 0.0) else "LOW"))
    fw = FAULTS["fahrenheit_firmware"]["fw"] if v["vin"] in fahrenheit else v["bms_fw"]
    out = []
    h0, h1 = P["ride_hours_ist"]
    for n, (code, desc, sev) in enumerate(codes):
        ts = datetime(day.year, day.month, day.day, tzinfo=IST) + timedelta(hours=r.uniform(h0, h1))
        ts = ts.replace(microsecond=ts.microsecond // 1000 * 1000)
        msg = {"event_id": f"DTS-{v['vin']}-{day:%Y%m%d}-{n:02d}", "vin": v["vin"], "event_ts": K.iso_utc(ts),
               "dtc_code": code, "dtc_desc": desc, "severity": sev,
               "odo_km": round(v["daily_km"] * (day - delivery).days, 1), "fw_bms": fw, "producer_run": run_id}
        out.append((ts.timestamp() + K.delay(r, P), msg))
    return out


def backfill(sink, fleet: list[dict], dates: list[date], run_id: str, log=print) -> int:
    """Each live date's DTCs in send-time order, control messages as their minutes close."""
    ctl, sent = K.Control(sink, run_id, P["control_hold_s"], "event_ts"), 0
    for day in dates:
        K.refuse_history(day)
        fahrenheit = T.planted(fleet, day)["fahrenheit"]
        msgs = [m for v in fleet for m in vehicle_day(v, day, fahrenheit, run_id)]
        sent += K.send_sorted(sink, ctl, K.TOPIC["dtc"], msgs)
        log(f"  {day} {len(msgs):,} DTCs from {len({m['vin'] for _, m in msgs}):,} vehicles "
            f"({sum(1 for _, m in msgs if m['dtc_code'] in ('BMS-T01', 'BMS-I02')):,} battery, "
            f"{sum(1 for _, m in msgs if m['severity'] == 'CRITICAL'):,} critical)")
    ctl.flush(0, final=True)
    return sent


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default=str(ROOT / "data" / "stream"))
    ap.add_argument("--generated", default=str(ROOT / "data" / "generated"))
    ap.add_argument("--dates", default=",".join(K.LH["calendar"]["live_dates"][:1]))
    args = ap.parse_args(argv)
    fleet = T.fleet_from_files(Path(args.generated))
    run_id = f"backfill-{uuid.uuid4().hex[:8]}"
    sink, t0 = K.FileSink(args.out, "dtc"), time.time()
    try:
        backfill(sink, fleet, [date.fromisoformat(x) for x in args.dates.split(",")], run_id)
    finally:
        sink.close()
    print(f"sent {sink.sent:,} messages from {len(fleet):,} live vehicles in {time.time() - t0:.0f} s (run {run_id})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
