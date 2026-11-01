import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from stream.producer import telemetry as T  # noqa: E402

FLEET = [{"vin": f"XEVS1K2609010{i:04d}", "delivery_date": "2026-09-05", "daily_km": 30.0, "fast_charge_share": 0.2,
          "contaminated": i == 0, "v_spread_mv": 15.0, "event_date": "2026-11-20" if i == 1 else None,
          "event_kind": "imbalance" if i == 1 else None, "live_subset": True, "bms_fw": "BMS-3.4.2",
          "geohash5": "te7mu"} for i in range(60)]


class ListSink:
    def __init__(self):
        self.msgs = []

    def send(self, topic, msg, send_ts):
        self.msgs.append((topic, msg, send_ts))


def test_deterministic_per_vehicle_and_date():
    f = T.planted(FLEET, date(2026, 11, 6))
    a = T.vehicle_day(FLEET[3], date(2026, 11, 6), f, 30, "r")
    b = T.vehicle_day(FLEET[3], date(2026, 11, 6), f, 30, "r")
    assert a == b


def test_planted_faults_follow_the_config():
    cfg = json.loads((ROOT / "config" / "lakehouse.json").read_text())["faults"]
    assert T.planted(FLEET, date(2026, 11, 7)) == {"fahrenheit": set(), "stuck_soc": set()}
    f = T.planted(FLEET, date(2026, 11, 9))
    assert len(f["fahrenheit"]) == cfg["fahrenheit_firmware"]["vehicles"]
    assert len(f["stuck_soc"]) == cfg["stuck_soc"]["vehicles"]
    for v in FLEET:
        msgs = [m for _, m in T.vehicle_day(v, date(2026, 11, 9), f, 30, "r")]
        if v["vin"] in f["stuck_soc"] and msgs:
            assert len({m["soc_pct"] for m in msgs}) == 1
        if v["vin"] in f["fahrenheit"]:
            assert all(m["bms_fw"].endswith("-F") and m["cell_t_max"] > 75 for m in msgs)


def test_control_totals_equal_messages_sent():
    sink = ListSink()
    n = T.backfill(sink, FLEET, [date(2026, 11, 6), date(2026, 11, 7)], 30, "r", log=lambda s: None)
    readings = [m for t, m, _ in sink.msgs if t == T.TOPIC["telemetry"]]
    control = [m for t, m, _ in sink.msgs if t == T.TOPIC["control"]]
    assert len(readings) == n > 0
    assert sum(c["total"] for c in control) == n
    assert len({c["minute"] for c in control}) == len(control)
    sends = [s for t, _, s in sink.msgs if t == T.TOPIC["telemetry"]]
    assert sends == sorted(sends)
