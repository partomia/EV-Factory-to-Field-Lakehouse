import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "usecases" / "uc01_pack_risk"))

import build_features as F  # noqa: E402


def test_factory_sql_does_not_read_field_outcomes():
    sql = F.FACTORY_SQL.lower()
    found = [t for t in F.OUTCOME_TABLES if t in sql]
    assert not found, found


def test_label_columns_are_not_features():
    banned = {"label", "label_kind", "label_date", "matured", "safety_floor", "vin", "pack_serial"}
    assert not banned & set(F.FEATURE_COLUMNS)


def test_the_label_query_does_read_the_outcomes():
    sql = F.LABEL_SQL.lower()
    assert "fact_dtc" in sql and "fact_warranty_claim" in sql and "fact_edr_event" in sql
