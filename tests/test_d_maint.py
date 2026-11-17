"""Iceberg maintenance (cde/jobs/maintenance.py): prefix guard, stream-table expiry skip, branches, gold window."""
import os
import shutil
import time
from datetime import datetime, timedelta

import pytest

import evx_common as C
import maintenance as M


# ---------------------------------------------------------------- guard and rules (no Spark)


@pytest.mark.parametrize("name", ["rsingh_evx_gold.fact_dtc", "rsingh_evx_bronze.telemetry_reading",
                                  "rsingh_evx_ref.publish_log"])
def test_prefix_guard_accepts_project_tables(name):
    assert M.assert_project_table(name) == name


@pytest.mark.parametrize("name", ["other_gold.fact_dtc", "rsingh_evxgold.t", "RSINGH_EVX_gold.t", "rsingh_evx_gold",
                                  "local.rsingh_evx_gold.t", "rsingh_evx_gold.t; DROP TABLE x", "rsingh_evx_gold.t'",
                                  "rsingh_evx_gold.`t`", "", None, "default.rsingh_evx_t"])
def test_prefix_guard_refuses_everything_else(name):
    with pytest.raises(AssertionError):
        M.assert_project_table(name)


class NoSpark:
    """A session that fails the test if the job asks it anything."""

    class conf:
        @staticmethod
        def get(*_):
            raise AssertionError("spark touched")

    def sql(self, *_):
        raise AssertionError("spark touched")


@pytest.mark.parametrize("argv", [
    ["--db-prefix", "other", "--only", "silver.cell_step"],
    ["--only", "silver.cell_step,silver.x';DROP"],
    ["--only", "nolayer.cell_step"],
    ["--only", "silver"],
])
def test_bad_names_refused_before_any_spark_call(argv):
    with pytest.raises(AssertionError):
        M.run(NoSpark(), argv)


def test_operations_config():
    ops = C.load_json("config/operations.json")
    m = ops["maintenance"]
    assert {"bronze.telemetry_reading", "bronze.factory_line_event"} <= set(m["no_expire"])
    assert (m["snapshots_days"], m["orphan_days"], m["branch_prefix"]) == (7, 3, "wap_")
    assert set(m["layers"]) <= set(C.LAYERS)
    assert ops["clock_skew"]["tolerance_s"] == 120
    assert {"freshness", "late_data"} <= set(ops)


@pytest.mark.parametrize("hhmm,inside", [("22:59", False), ("23:00", True), ("23:30", True), ("02:00", True),
                                         ("03:59", True), ("04:00", False), ("04:30", False), ("12:00", False)])
def test_gold_window_wraps_midnight(hhmm, inside):
    w = M.settings()["gold_publish_window_utc"]
    assert M.in_window(datetime.fromisoformat(f"2026-11-17T{hhmm}:00"), w) is inside


def test_stale_branch_and_in_flight_rules():
    cut = datetime(2026, 11, 10)
    old, new = datetime(2026, 11, 1), datetime(2026, 11, 12)
    refs = [("main", "BRANCH", old), ("wap_a", "BRANCH", old), ("wap_b", "BRANCH", new), ("wap_t", "TAG", old),
            ("audit_x", "BRANCH", old), ("wap_c", "BRANCH", None)]
    assert M.stale_branches(refs, "wap_", cut) == ["wap_a"]
    log = [("wap_a", "AUDIT_FAILED", old, old), ("wap_b", None, old, None), ("wap_c", "PUBLISHED", new, new),
           ("wap_d", "ERROR", old, old)]
    assert M.in_flight(log, cut) == {"wap_b", "wap_c"}


def test_literals_are_quoted_like_the_app():
    assert M.lit("a'b") == "'ab'" and M.lit(5) == "'5'"


# ---------------------------------------------------------------- on local Spark + Iceberg


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark")
    if not shutil.which("java"):
        pytest.skip("no Java for local Spark")
    from scripts.run_local import local_spark

    s = local_spark(tmp_path_factory.mktemp("warehouse"), driver_memory="2g")
    if "Iceberg" not in (s.conf.get("spark.sql.extensions", "") or ""):
        pytest.skip("an earlier session without Iceberg is active")
    return s


PROBES = ("bronze.telemetry_reading", "silver.maint_probe", "gold.maint_probe")
COMMITS = 8


@pytest.fixture(scope="module")
def tables(spark):
    n = C.Names()
    C.ensure_databases(spark, n, ("bronze", "silver", "gold", "ref"))
    for short in PROBES:
        full = n.t(*short.split("."))
        spark.sql(f"DROP TABLE IF EXISTS {full}")
        spark.sql(f"CREATE TABLE {full} (id bigint, v string) USING iceberg")
        for i in range(COMMITS):
            spark.sql(f"INSERT INTO {full} VALUES ({i}, 'v{i}')")
    probe = n.t("silver", "maint_probe")
    spark.sql(f"ALTER TABLE {probe} CREATE BRANCH wap_stale")
    spark.sql(f"ALTER TABLE {probe} CREATE BRANCH wap_busy")
    log = n.t("ref", "publish_log")
    spark.sql(f"DROP TABLE IF EXISTS {log}")
    spark.sql(f"CREATE TABLE {log} (table_name string, branch string, status string, started_at timestamp_ntz, "
              f"ended_at timestamp_ntz) USING iceberg")
    spark.sql(f"INSERT INTO {log} VALUES ('{probe}', 'wap_busy', 'RUNNING', TIMESTAMP_NTZ '2026-11-01 00:00:00', NULL)")
    return n


def snapshots(spark, full):
    return spark.sql(f"SELECT count(*) FROM {full}.snapshots").collect()[0][0]


def data_dir(spark, full):
    p = spark.sql(f"SELECT file_path FROM {full}.files LIMIT 1").collect()[0][0]
    return os.path.dirname(p.replace("file:", "", 1))


def test_dry_run_changes_nothing(spark, tables):
    before = {s: snapshots(spark, tables.t(*s.split("."))) for s in PROBES}
    later = (C.now_utc() + timedelta(days=10)).replace(tzinfo=None, hour=12).isoformat()
    res = M.run(spark, ["--dry-run", "--only", ",".join(PROBES), "--now", later])
    assert {s: snapshots(spark, tables.t(*s.split("."))) for s in PROBES} == before
    assert res["bronze.telemetry_reading"]["expire"].startswith("skipped")
    assert res["silver.maint_probe"]["stale_branches"] == ["wap_stale"]


def test_gold_skipped_inside_publish_window(spark, tables):
    inside = (C.now_utc() + timedelta(days=10)).replace(tzinfo=None, hour=1, minute=0).isoformat()
    res = M.run(spark, ["--only", "gold.maint_probe", "--now", inside])
    assert res["gold.maint_probe"] == {"skip": "inside the gold publish window"}
    assert snapshots(spark, tables.t("gold", "maint_probe")) == COMMITS


def test_maintenance_run(spark, tables):
    tr, probe = tables.t("bronze", "telemetry_reading"), tables.t("silver", "maint_probe")
    files_before = spark.sql(f"SELECT count(*) FROM {tr}.files").collect()[0][0]
    stale_orphan, fresh_orphan = (os.path.join(data_dir(spark, probe), f"{x}-orphan.parquet") for x in ("old", "new"))
    for f in (stale_orphan, fresh_orphan):
        with open(f, "wb") as fh:
            fh.write(b"not a data file")
    ten_days_ago = time.time() - 10 * 86400
    os.utime(stale_orphan, (ten_days_ago, ten_days_ago))

    later = (C.now_utc() + timedelta(days=10)).replace(tzinfo=None, hour=12).isoformat()
    res = M.run(spark, ["--only", ",".join(PROBES), "--now", later])

    # stream table: compacted, never expired (every snapshot a reader checkpoint may hold is still there)
    t = res["bronze.telemetry_reading"]
    assert t["expire"] == "skipped: read as a stream"
    assert t["compact"] == f"{files_before}->1"
    assert snapshots(spark, tr) >= COMMITS + 1
    assert spark.table(tr).count() == COMMITS

    # batch table: expired to retain_last, stale branch dropped, in-flight branch kept, old orphan only removed
    p = res["silver.maint_probe"]
    assert p["dropped_branches"] == ["wap_stale"] and p["kept_branches"] == ["wap_busy"]
    refs = {r[0] for r in spark.sql(f"SELECT name FROM {probe}.refs").collect()}
    assert refs == {"main", "wap_busy"}
    assert p["expire"] == f"{COMMITS + 1}->{snapshots(spark, probe)}"
    assert snapshots(spark, probe) <= M.settings()["retain_last"] + 1   # + the snapshot wap_busy holds
    assert spark.table(probe).count() == COMMITS
    assert p["orphans"] == 1 and not os.path.exists(stale_orphan) and os.path.exists(fresh_orphan)
    assert snapshots(spark, tables.t("gold", "maint_probe")) <= M.settings()["retain_last"]
