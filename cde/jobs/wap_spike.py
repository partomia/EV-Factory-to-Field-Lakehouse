"""
WAP feasibility spike (PLAN.md section 9): what write-audit-publish CDE Spark 3.5.4 / Iceberg 1.5.2 supports.

Runs on a scratch table only (rsingh_evx_ref.wap_spike_scratch). Each check is tried independently and
recorded as PASS / FAIL with the error text, so one unsupported feature does not hide the others. Results are
printed as one JSON line (prefix WAP_SPIKE_RESULT) for the driver log and docs/WAP_SPIKE.md.

  --mode run    create the scratch table and run checks 1-4 and 6; leave branch `impala_probe` unpublished
                so Impala can be probed from outside (check 5)
  --mode drop   drop the scratch table (PURGE)

Standard library and PySpark only.
"""

from __future__ import annotations

import argparse
import json
import traceback

DB = "rsingh_evx_ref"
TABLE = f"{DB}.wap_spike_scratch"
RESULTS: list[dict] = []


def check(name: str):
    def wrap(fn):
        def run(spark):
            try:
                detail = fn(spark)
                RESULTS.append({"check": name, "status": "PASS", "detail": detail})
            except Exception as e:  # recorded, the spike continues
                RESULTS.append({"check": name, "status": "FAIL",
                                "error": f"{type(e).__name__}: {str(e)[:600]}",
                                "trace": traceback.format_exc()[-800:]})
        return run
    return wrap


def rows(spark, day: str, n: int, tag: str):
    return spark.sql(
        f"SELECT CAST('{day}' AS DATE) AS business_date, id AS k, '{tag}' AS tag FROM range({n})")


def count(spark, ref: str = "") -> int:
    q = f"SELECT COUNT(*) FROM {TABLE}" + (f" VERSION AS OF '{ref}'" if ref else "")
    return int(spark.sql(q).collect()[0][0])


def snapshots(spark) -> list:
    return [r.asDict() for r in spark.sql(
        f"SELECT snapshot_id, operation, summary['wap.id'] AS wap_id FROM {TABLE}.snapshots "
        "ORDER BY committed_at").collect()]


def refs(spark) -> dict:
    return {r["name"]: int(r["snapshot_id"]) for r in spark.sql(f"SELECT name, snapshot_id FROM {TABLE}.refs").collect()}


@check("0_create_scratch")
def c0(spark):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB}")
    spark.sql(f"DROP TABLE IF EXISTS {TABLE} PURGE")
    rows(spark, "2026-01-01", 100, "main").writeTo(TABLE).using("iceberg") \
        .tableProperty("format-version", "2").tableProperty("write.wap.enabled", "true") \
        .partitionedBy("business_date").create()
    return {"main_count": count(spark), "version": spark.version}


@check("1a_create_branch")
def c1a(spark):
    spark.sql(f"ALTER TABLE {TABLE} CREATE BRANCH audit_b")
    return {"refs": refs(spark)}


@check("1b_write_spark_wap_branch")
def c1b(spark):
    spark.conf.set("spark.wap.branch", "audit_b")
    try:
        rows(spark, "2026-01-02", 50, "wap_branch_conf").writeTo(TABLE).append()
    finally:
        spark.conf.unset("spark.wap.branch")
    return {"branch_count": count(spark, "audit_b"), "main_count": count(spark)}


@check("1c_write_branch_identifier")
def c1c(spark):
    rows(spark, "2026-01-03", 25, "branch_ident").writeTo(f"{TABLE}.branch_audit_b").append()
    return {"branch_count": count(spark, "audit_b"), "main_count": count(spark)}


@check("2_main_readers_isolated")
def c2(spark):
    main = count(spark)
    assert main == 100, f"main sees {main} rows, expected 100"
    tags = [r[0] for r in spark.sql(f"SELECT DISTINCT tag FROM {TABLE}").collect()]
    return {"main_count": main, "main_tags": tags}


@check("3_audit_query_on_branch")
def c3(spark):
    r = spark.sql(
        f"SELECT business_date, COUNT(*) n, COUNT(DISTINCT k) dk, SUM(CASE WHEN k IS NULL THEN 1 ELSE 0 END) nulls "
        f"FROM {TABLE} VERSION AS OF 'audit_b' GROUP BY business_date ORDER BY business_date").collect()
    via_ident = int(spark.sql(f"SELECT COUNT(*) FROM {TABLE}.branch_audit_b").collect()[0][0])
    return {"by_date": [x.asDict() for x in r], "count_via_identifier": via_ident}


@check("4a_fast_forward")
def c4a(spark):
    before = refs(spark)
    out = spark.sql(f"CALL spark_catalog.system.fast_forward('{TABLE}', 'main', 'audit_b')").collect()
    after = refs(spark)
    assert after["main"] == after["audit_b"], "main not at the branch head"
    return {"result": [x.asDict() for x in out], "refs_before": before, "refs_after": after,
            "main_count": count(spark)}


@check("4b_wap_id_cherrypick")
def c4b(spark):
    spark.conf.set("spark.wap.id", "evx-spike-1")
    try:
        rows(spark, "2026-01-04", 10, "wap_id_staged").writeTo(TABLE).append()
    finally:
        spark.conf.unset("spark.wap.id")
    main_before = count(spark)
    staged = [s for s in snapshots(spark) if s["wap_id"] == "evx-spike-1"]
    assert staged, "no staged snapshot with wap.id"
    sid = int(staged[-1]["snapshot_id"])
    out = spark.sql(f"CALL spark_catalog.system.cherrypick_snapshot('{TABLE}', {sid})").collect()
    return {"staged_snapshot": sid, "main_before": main_before, "main_after": count(spark),
            "result": [x.asDict() for x in out]}


@check("6a_overwrite_partitions_on_branch")
def c6a(spark):
    spark.sql(f"ALTER TABLE {TABLE} CREATE BRANCH ow_b")
    rows(spark, "2026-01-02", 7, "overwrite").writeTo(f"{TABLE}.branch_ow_b").overwritePartitions()
    by_date = {str(r[0]): int(r[1]) for r in spark.sql(
        f"SELECT business_date, COUNT(*) FROM {TABLE} VERSION AS OF 'ow_b' GROUP BY business_date").collect()}
    main_by_date = {str(r[0]): int(r[1]) for r in spark.sql(
        f"SELECT business_date, COUNT(*) FROM {TABLE} GROUP BY business_date").collect()}
    assert by_date.get("2026-01-02") == 7, by_date
    spark.sql(f"CALL spark_catalog.system.fast_forward('{TABLE}', 'main', 'ow_b')")
    return {"branch_by_date": by_date, "main_by_date_before_ff": main_by_date, "main_count_after_ff": count(spark)}


@check("6b_overwrite_partitions_with_wap_branch_conf")
def c6b(spark):
    spark.sql(f"ALTER TABLE {TABLE} CREATE BRANCH ow_conf_b")
    spark.conf.set("spark.wap.branch", "ow_conf_b")
    try:
        rows(spark, "2026-01-03", 3, "overwrite_conf").writeTo(TABLE).overwritePartitions()
    finally:
        spark.conf.unset("spark.wap.branch")
    by_date = {str(r[0]): int(r[1]) for r in spark.sql(
        f"SELECT business_date, COUNT(*) FROM {TABLE} VERSION AS OF 'ow_conf_b' GROUP BY business_date").collect()}
    assert by_date.get("2026-01-03") == 3, by_date
    return {"branch_by_date": by_date, "main_count": count(spark)}


@check("5_prepare_impala_probe")
def c5(spark):
    spark.sql(f"ALTER TABLE {TABLE} CREATE BRANCH impala_probe")
    rows(spark, "2026-01-05", 11, "impala_probe").writeTo(f"{TABLE}.branch_impala_probe").append()
    r = refs(spark)
    return {"refs": r, "main_count": count(spark), "probe_count": count(spark, "impala_probe"),
            "probe_snapshot": r["impala_probe"], "main_snapshot": r["main"]}


@check("7_drop_branches_except_probe")
def c7(spark):
    for b in ("audit_b", "ow_b", "ow_conf_b"):
        spark.sql(f"ALTER TABLE {TABLE} DROP BRANCH IF EXISTS {b}")
    return {"refs": refs(spark)}


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=("run", "drop"), default="run")
    args, _ = p.parse_known_args()
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(f"evx-wap-spike-{args.mode}").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    if args.mode == "drop":
        spark.sql(f"DROP TABLE IF EXISTS {TABLE} PURGE")
        print("WAP_SPIKE_RESULT " + json.dumps({"dropped": TABLE, "exists": spark.catalog.tableExists(TABLE)}))
        return 0
    RESULTS.append({"check": "env", "status": "INFO", "detail": {
        "spark": spark.version,
        "extensions": spark.conf.get("spark.sql.extensions", ""),
        "catalog_impl": spark.conf.get("spark.sql.catalog.spark_catalog", ""),
    }})
    for fn in (c0, c1a, c1b, c1c, c2, c3, c4a, c4b, c6a, c6b, c5, c7):
        fn(spark)
    print("WAP_SPIKE_RESULT " + json.dumps(RESULTS, default=str))
    return 0 if RESULTS[1]["status"] == "PASS" else 1


if __name__ == "__main__":
    rc = main()
    if rc:
        raise SystemExit(rc)
