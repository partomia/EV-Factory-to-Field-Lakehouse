"""
Test double for generators.twin.station_cycle_events (lane E): station cycle events with the line-event fields
and timestamp formats of the interface (one record per completed cycle, ISO-8601 +05:30, mes_ts = cycle end +
0.5..3 s), for tests and laptop runs on a branch without the twin generator. Chosen only by `--events fake`.
Deterministic per (station, date, scale). All data is synthetic.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta

from generators.core import IST, rng

STATIONS = [("CL1", "CELL", "CL-WND-01", 75.0), ("CL2", "CELL", "CL-WND-02", 75.0),
            ("PL1", "PACK", "PA-WB-01", 240.0), ("PL2", "PACK", "PA-WB-02", 240.0),
            ("VA1", "VEHICLE", "VA-EOL-01", 420.0), ("VA2", "VEHICLE", "VA-EOL-02", 420.0)]


def _iso(ts: datetime) -> str:
    return ts.astimezone(IST).isoformat(timespec="seconds")


def _shift(ts: datetime) -> str:
    h = ts.astimezone(IST).hour
    return "A" if 6 <= h < 14 else "B" if 14 <= h < 22 else "C"


def station_cycle_events(cfg: dict, day: date, scale: float) -> list[dict]:
    out = []
    for line_id, area, station, nominal in STATIONS:
        r = rng("line-fake", station, day.isoformat(), scale)
        t = datetime(day.year, day.month, day.day, tzinfo=IST) + timedelta(seconds=r.uniform(5, 60))
        end_of_day = datetime(day.year, day.month, day.day, tzinfo=IST) + timedelta(days=1, seconds=-10)
        nominal_s = nominal * (1 + 4 * (1 - min(max(scale, 0.0), 1.0)))
        seq = 0
        while True:
            u = r.random()
            status = "OK" if u < 0.95 else "FAULT" if u < 0.97 else "BLOCKED" if u < 0.99 else "STARVED"
            cycle_s = round(max(5.0, r.gauss(nominal_s, nominal_s * 0.06) * (1.6 if status != "OK" else 1.0)), 1)
            start, end = t, t + timedelta(seconds=cycle_s)
            mes = end + timedelta(seconds=r.uniform(0.5, 3.0))
            if mes >= end_of_day:
                break
            seq += 1
            out.append({"event_id": f"LE-{station}-{day:%Y%m%d}-{seq:06d}", "station_id": station, "line_id": line_id,
                        "area": area, "unit_id": f"U-{station}-{day:%y%m%d}-{seq:05d}",
                        "cycle_start_ts": _iso(start), "event_ts": _iso(end), "mes_ts": _iso(mes),
                        "cycle_s": cycle_s,
                        "status": status, "shift": _shift(end)})
            t = end + timedelta(seconds=r.uniform(2, 20))
    return out
