import ast
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "cde" / "jobs"))

import reprocess_late as R  # noqa: E402

OPS = json.loads((ROOT / "config" / "operations.json").read_text())
LAKE = json.loads((ROOT / "config" / "lakehouse.json").read_text())
D = date.fromisoformat


def test_late_from_day_four_for_72_hours():
    assert OPS["late_data"]["sla_hours"] == 72
    assert R.late_after_days(72) == 4
    assert R.late_after_days(48) == 3
    assert R.late_after_days(71.5) == 3


def test_natural_delays_are_never_late():
    """Generator: trigger 07:00-21:00 IST, upload delay within volumes.edr_delay_hours (2-72 h). The latest such
    dump lands on day 3, which the rule keeps on time; only a dump held past the SLA is late."""
    lo, hi = LAKE["volumes"]["edr_delay_hours"]
    assert hi <= OPS["late_data"]["sla_hours"]
    late_after = R.late_after_days(OPS["late_data"]["sla_hours"])
    ev = datetime(2026, 11, 6)
    worst = max(((ev + timedelta(hours=h + d)).date() - ev.date()).days
                for h in (0, 7, 21, 23.99) for d in (lo, hi))
    assert worst < late_after


def test_plan_touches_only_event_dates_of_processed_arrivals():
    arrivals = [(D("2026-11-10"), D("2026-11-06"), 1), (D("2026-11-10"), D("2026-11-08"), 2),
                (D("2026-11-10"), D("2026-11-10"), 1)]
    pl = R.plan(arrivals, [D("2026-11-10")], 4)
    assert pl["touched"] == [D("2026-11-06"), D("2026-11-08"), D("2026-11-10")]
    p = pl["per_arrival"][D("2026-11-10")]
    assert p["late"] == {D("2026-11-06"): 1}
    assert R.status_of(p) == "REPROCESSED"


def test_status_on_time_and_no_data():
    pl = R.plan([(D("2026-11-09"), D("2026-11-06"), 1)], [D("2026-11-08"), D("2026-11-09")], 4)
    assert R.status_of(pl["per_arrival"][D("2026-11-09")]) == "ON_TIME"
    assert R.status_of(pl["per_arrival"][D("2026-11-08")]) == "NO_DATA"
    assert pl["touched"] == [D("2026-11-06")]


def test_plan_is_deterministic_on_rerun():
    arrivals = [(D("2026-11-10"), D("2026-11-06"), 1), (D("2026-11-10"), D("2026-11-09"), 3)]
    assert R.plan(arrivals, [D("2026-11-10")], 4) == R.plan(list(reversed(arrivals)), [D("2026-11-10")], 4)


def test_guard_refuses_other_prefixes():
    assert R.guard("rsingh_evx_gold.edr_event_daily")
    with pytest.raises(ValueError):
        R.guard("other_gold.edr_event_daily")


def test_log_schema_is_impala_writable():
    cols = dict(c.strip().split(" ", 1) for c in R.LOG_SCHEMA.split(","))
    assert cols["started_at"] == cols["ended_at"] == "timestamp_ntz"
    assert {"run_id", "arrival_date", "event_dates_touched", "rows_before", "rows_after", "status"} <= set(cols)
    assert "_file" not in cols


def test_job_reads_no_planted_fault():
    src = (ROOT / "cde" / "jobs" / "reprocess_late.py").read_text()
    assert "faults" not in src and "edr_late" not in src
    imports = {n.names[0].name.split(".")[0] for n in ast.walk(ast.parse(src)) if isinstance(n, ast.Import)}
    assert imports <= {"sys", "time", "evx_common"}
