"""
Planted fault 11: one station's PLC clock runs ahead on one business date (config/lakehouse.json
faults.clock_skew: station, date, seconds). Line events pass through apply() before they are serialised; the
MES receive time (mes_ts) is the trusted clock and is never shifted, so silver can measure the offset
(cde/jobs/clock_skew.py correct_clock_skew, which never reads this fault). truth_rows() feeds
ref.generator_truth_fault; generators/twin.py yields it once, with the line events of the skew date.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from generators.core import IST

SHIFTED = ("cycle_start_ts", "event_ts")


def _skew(cfg: dict) -> dict | None:
    s = (cfg.get("faults") or {}).get("clock_skew")
    return s if s and s.get("station") and s.get("date") and s.get("seconds") else None


def _iso(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds" if not dt.microsecond else "auto")


def business_date(mes_ts: str) -> str:
    """The business date of an event: the IST date of its MES receive time."""
    return datetime.fromisoformat(mes_ts).astimezone(IST).date().isoformat()


def apply(event: dict, cfg: dict) -> dict:
    """A new event dict; the skewed station's PLC timestamps on the skew date are shifted by +seconds."""
    out = dict(event)
    s = _skew(cfg)
    if not s or out.get("station_id") != s["station"] or business_date(out["mes_ts"]) != s["date"]:
        return out
    for col in SHIFTED:
        if out.get(col):
            out[col] = _iso(datetime.fromisoformat(out[col]) + timedelta(seconds=int(s["seconds"])))
    return out


def truth_rows(cfg: dict) -> list[dict]:
    """One generator_truth_fault row (expected_recon fault 11), shaped like the other truth_fault records."""
    s = _skew(cfg)
    if not s:
        return []
    return [{"fault": "clock_skew", "date": s["date"], "key": s["station"], "value": str(int(s["seconds"]))}]
