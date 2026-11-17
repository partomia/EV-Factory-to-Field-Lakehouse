import json
import sys
from datetime import date, datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "cde" / "jobs"))

import evx_common as C  # noqa: E402
import freshness as F  # noqa: E402

OPS = json.loads((ROOT / "config" / "operations.json").read_text())


def items(spec, kind):
    return [{"table": i} if isinstance(i, str) else i for i in spec.get(kind, [])]


def test_operations_keys_kept():
    assert set(OPS) >= {"_comment", "maintenance", "freshness", "late_data", "clock_skew"}
    assert OPS["clock_skew"]["tolerance_s"] == 120


def test_every_product_names_valid_tables_and_slas():
    names = C.Names()
    for product, spec in OPS["freshness"]["products"].items():
        assert spec["owner"] and "sla_days" in spec
        assert spec.get("batch") or spec.get("stream"), product
        for i in items(spec, "batch") + items(spec, "stream"):
            F.table_of(names, i["table"])
            for key in ("date_col", "time_col", "stage"):
                if key in i:
                    F.ident(i[key])
        if spec.get("stream"):
            assert spec["sla_minutes"] > 0


def test_batch_products_cover_bronze_silver_gold():
    layers = {i["table"].split(".")[0] for s in OPS["freshness"]["products"].values() for i in items(s, "batch")}
    assert {"bronze", "silver", "gold"} <= layers


def test_batch_status():
    bd = date(2026, 11, 9)
    assert F.batch_status(bd, date(2026, 11, 9), 0, 1) == ("GREEN", 0.0)
    assert F.batch_status(bd, date(2026, 11, 8), 0, 1) == ("AMBER", 1.0)
    assert F.batch_status(bd, date(2026, 11, 7), 0, 1) == ("RED", 2.0)
    assert F.batch_status(bd, None, 0, 1) == ("RED", None)
    assert F.batch_status(bd, date(2026, 11, 6), 3, 1) == ("GREEN", 3.0)


def test_stream_status():
    ref = datetime(2026, 11, 10)
    assert F.stream_status(ref, datetime(2026, 11, 9, 23, 50), 15, 2.0) == ("GREEN", 10.0)
    assert F.stream_status(ref, datetime(2026, 11, 9, 23, 35), 15, 2.0) == ("AMBER", 25.0)
    assert F.stream_status(ref, datetime(2026, 11, 9, 23, 0), 15, 2.0) == ("RED", 60.0)
    assert F.stream_status(ref, None, 15, 2.0) == ("RED", None)


def test_worst_check_decides():
    checks = [{"status": "GREEN", "lag": 0.0}, {"status": "AMBER", "lag": 1.0}, {"status": "AMBER", "lag": 2.0}]
    assert F.worst(checks)["lag"] == 2.0
    assert F.worst(checks + [{"status": "RED", "lag": None}])["status"] == "RED"


def test_identifiers_are_validated():
    with pytest.raises(ValueError):
        F.ident("event_ts; DROP TABLE x")
    with pytest.raises(ValueError):
        F.table_of(C.Names(), "gold.fact dtc")
    with pytest.raises(ValueError):
        F.table_of(C.Names(), "nolayer.t")


def test_freshness_schema_is_impala_writable():
    cols = dict(c.strip().split(" ", 1) for c in F.SCHEMA.split(","))
    assert cols["last_commit_at"] == cols["measured_at"] == "timestamp_ntz"
    assert {"product", "last_business_date", "last_commit_at", "lag", "sla", "status"} <= set(cols)
