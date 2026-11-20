import json
import shutil
import sys
from datetime import date
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "cde" / "jobs"))

from recon import classify, exceptions_sql  # noqa: E402

EXPECTED = json.loads((ROOT / "config" / "expected_recon.json").read_text())["faults"]


def test_listed_exception_takes_its_outcome():
    out = classify([("2026-09-15", "bronze", "cell_mes_step", "trailer count", 1, "f.csv")], EXPECTED, {"M1"},
                   {"2026-09-15"})
    assert out == [("2026-09-15", "bronze", "cell_mes_step", "trailer count", 1, "MISMATCH", 2, "f.csv")]


def test_unlisted_exception_and_missing_fault():
    out = classify([("2026-09-14", "bronze", "cell_mes_step", "trailer count", 1, "f.csv")], EXPECTED, {"M1"},
                   {"2026-09-14", "2026-09-15"})
    assert ("2026-09-14", "bronze", "cell_mes_step", "trailer count", 1, "UNEXPECTED", None, "f.csv") in out
    assert [r[5:7] for r in out if r[0] == "2026-09-15"] == [("MISSING", 2)]


def test_unbuilt_milestone_is_not_missing():
    out = classify([], EXPECTED, {"M1"}, {"2026-11-09"})
    assert out == []


def test_expected_rows_are_well_formed():
    ids = [e["id"] for e in EXPECTED]
    assert ids == sorted(set(ids))
    assert all(e["outcome"] in ("EXPLAINED", "MISMATCH") and e["milestone"] in ("M1", "M2", "M2-regen", "M2-produce")
               for e in EXPECTED)


def test_telemetry_faults_are_not_missing_without_the_producer():
    out = classify([], EXPECTED, {"M1", "M2"}, {"2026-11-08", "2026-11-09", "2026-11-10"})
    missing = {r[6] for r in out if r[5] == "MISSING"}
    assert not missing & {6, 7} and 11 in missing


def test_late_edr_is_not_missing_until_regenerated():
    out = classify([], EXPECTED, {"M1", "M2"}, {"2026-11-10"})
    assert 8 not in {r[6] for r in out}


def test_late_edr_on_the_new_date_is_explained_and_elsewhere_unexpected():
    row = ("gold", "fact_edr_event", "late reprocess", 1, "re-opened 2026-11-06")
    assert classify([("2026-11-10", *row)], EXPECTED, {"M1"}, {"2026-11-10"})[0][5:7] == ("EXPLAINED", 8)
    assert classify([("2026-11-09", *row)], EXPECTED, {"M1"}, {"2026-11-09"})[0][5:7] == ("UNEXPECTED", None)


def test_clock_skew_is_explained_where_planted_and_unexpected_elsewhere():
    row = ("silver", "line_event", "skew", 1, "VA-EOL-02 5400 s")
    assert classify([("2026-11-09", *row)], EXPECTED, {"M1"}, {"2026-11-09"})[0][5:7] == ("EXPLAINED", 11)
    assert classify([("2026-11-08", *row)], EXPECTED, {"M1"}, {"2026-11-08"})[0][5:7] == ("UNEXPECTED", None)


class _GlobalTemp:
    def db(self, _layer: str) -> str:
        return "global_temp"


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark")
    if not shutil.which("java"):
        pytest.skip("no Java for local Spark")
    from scripts.run_local import local_spark

    return local_spark(tmp_path_factory.mktemp("warehouse"), driver_memory="2g")


def test_identity_match_counts_dms_vins_not_candidate_pairs(spark):
    rows = [(date(2026, 11, 7), "XEVS1K26110100106", "XEVS1K26110100006", "VIN_EDIT"),
            (date(2026, 11, 7), "XEVS1K26110100106", "XEVS1K26110100016", "VIN_TRANSPOSITION"),
            (date(2026, 11, 7), "XEVS1K26102500082", "XEVS1K26102500002", "VIN_TRANSPOSITION"),
            (date(2026, 10, 28), "XEVS1K26102000031", "XEVS1K26102000013", "VIN_TRANSPOSITION"),
            (date(2026, 11, 7), "XEVS1K26110100200", "XEVS1K26110100201", "NO_MATCH")]
    spark.createDataFrame(rows, "first_seen_date date, source_key string, candidate_id string, rule string") \
        .createOrReplaceGlobalTempView("identity_review")
    sql = next(q for layer, q, table in exceptions_sql(_GlobalTemp(), "DATE '2026-10-28', DATE '2026-11-07'")
               if table.endswith(".identity_review"))
    got = {d.isoformat(): (n, detail) for d, _e, _c, n, detail in spark.sql(sql).collect()}
    assert got["2026-11-07"][0] == 2 and got["2026-10-28"][0] == 1
    assert sorted(got["2026-11-07"][1].split("; ")) == [
        "XEVS1K26102500082~XEVS1K26102500002", "XEVS1K26110100106~XEVS1K26110100006",
        "XEVS1K26110100106~XEVS1K26110100016"]
