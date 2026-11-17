"""
Failed-batch drill on silver (CDE job rsingh-evx-failed-batch-drill, by hand; never in the nightly DAG): a silver
batch for one business date fails in the middle of a table write, main is left as it was, and the resumed batch
ends with the same rows as a clean run.

  clean    build_silver for the date, unchanged (skip with --no-clean-run to take the current main as the clean
           state); then, per silver table: rows of the date, a content fingerprint of those rows, the snapshot id
           of main and the number of live data files
  fail     build_silver again with one fault injected: when it writes --fail-table, the overwrite runs over four
           partitions and the task of partition 1 raises (evx_common.fail_on_partition), so Spark aborts the
           write job before Iceberg commits. build_silver itself is not changed: the drill wraps
           evx_common.write_dates for that one table. Tables silver writes earlier in the batch commit again with
           the same rows; the failing table and every later one keep their snapshot
  resume   build_silver for the date, unchanged (the same command the nightly retry runs)
  verdict  PASS when, after the failure, the failing table and every later table still have their clean snapshot
           and data files, every table has its clean rows and fingerprint, and after the resume every table has
           its clean rows and fingerprint again

Each phase is logged in ref.failed_batch_drill (one row per table and phase; a re-run for the date replaces its rows).
Writes only rsingh_evx_* tables.

  spark-submit failed_batch_drill.py --business-date 2026-11-06 [--fail-table sap_inspection_lot] [--no-clean-run]
"""
from __future__ import annotations

import re
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import evx_common as C  # noqa: E402

TABLE_PREFIX = "rsingh_evx_"
NAME = re.compile(r"^[a-z0-9_]+$")
SCHEMA = ("drill_id string, business_date date, phase string, table_name string, write_order int, rows bigint, "
          "fingerprint bigint, snapshot_id bigint, data_files bigint, matches_clean boolean, "
          "logged_at timestamp_ntz, note string")


class InjectedFailure(RuntimeError):
    pass


def guard(table: str) -> str:
    if not table.startswith(TABLE_PREFIX):
        raise ValueError(f"refusing to touch {table}: not a {TABLE_PREFIX}* table")
    return table


def state(spark, names: C.Names, tables: list[str], day) -> dict:
    """{table: {rows, fingerprint, snapshot, files}} of main for one business date."""
    from pyspark.sql import functions as F

    out = {}
    for t in tables:
        full = names.t("silver", t)
        if not C.table_exists(spark, full):
            out[t] = {"rows": 0, "fingerprint": 0, "snapshot": None, "files": 0}
            continue
        df = spark.table(full).where(F.col("_business_date") == F.lit(day))
        row = df.agg(F.count(F.lit(1)).alias("n"),
                     F.sum(F.xxhash64(F.to_json(F.struct(*[F.col(c) for c in sorted(df.columns)])))).alias("h")) \
            .collect()[0]
        files = spark.sql(f"SELECT count(*) FROM {full}.files").collect()[0][0]
        out[t] = {"rows": int(row["n"]), "fingerprint": int(row["h"] or 0), "snapshot": C.snapshot_id(spark, full),
                  "files": int(files)}
    return out


def silver_tables(build_silver) -> list[str]:
    """Silver tables in the order build_silver writes them."""
    sap = list(build_silver.SAP_TABLE.values())
    docs = list(dict.fromkeys(build_silver.DOC_TABLE.values()))
    return list(build_silver.TYPED.values()) + ["pack_build", "pack_cell", "vehicle_build"] + sap + docs


def failing_write(target: str, original):
    """evx_common.write_dates, except that the write of `target` fails in one task, before the commit."""

    def write_dates(spark, df, table, dates):
        if table != target:
            return original(spark, df, table, dates)
        if not C.table_exists(spark, table):
            raise InjectedFailure(f"{table} does not exist yet: run silver for a date first")
        rdd = df.repartition(4).rdd.mapPartitionsWithIndex(lambda i, it: (C.fail_on_partition(i), it)[1])
        try:
            spark.createDataFrame(rdd, df.schema).writeTo(table).overwritePartitions()
        except Exception as e:
            raise InjectedFailure(f"injected task failure while writing {table}: {C.error_summary(e)}") from e
        raise InjectedFailure(f"the write of {table} committed although a task failed")

    return write_dates


def compare(clean: dict, now: dict, keep_snapshot: set[str]) -> list[str]:
    """Differences from the clean state: rows / fingerprint for every table; snapshot and files where kept."""
    bad = []
    for t, c in clean.items():
        n = now[t]
        if (n["rows"], n["fingerprint"]) != (c["rows"], c["fingerprint"]):
            bad.append(f"{t}: rows {c['rows']} -> {n['rows']}, fingerprint changed")
        if t in keep_snapshot and (n["snapshot"], n["files"]) != (c["snapshot"], c["files"]):
            bad.append(f"{t}: snapshot {c['snapshot']} -> {n['snapshot']}, files {c['files']} -> {n['files']}")
    return bad


def run(spark, argv=None) -> dict:
    import build_silver

    p = C.base_parser(__doc__.split("\n\n")[0])
    p.add_argument("--fail-table", default="sap_inspection_lot", help="silver table whose write fails")
    p.add_argument("--no-clean-run", action="store_true", help="take the current main as the clean state")
    args = C.parse(p, argv)
    if args.date_from != args.date_to:
        raise SystemExit("failed_batch_drill: one --business-date only")
    names = C.Names(args.db_prefix)
    day = args.date_to
    tables = silver_tables(build_silver)
    if not NAME.match(args.fail_table) or args.fail_table not in tables:
        raise SystemExit(f"--fail-table must be one of {tables}")
    for t in tables:
        guard(names.t("silver", t))
    drill_table = guard(names.t("ref", "failed_batch_drill"))
    order = {t: i for i, t in enumerate(tables)}
    later = {t for t in tables if order[t] >= order[args.fail_table]}
    silver_args = ["--business-date", day.isoformat(), "--db-prefix", args.db_prefix, "--landing", args.landing]
    if args.pipeline_run:
        silver_args += ["--pipeline-run", args.pipeline_run]
    drill_id, t0, log = f"drill-{uuid.uuid4().hex[:12]}", time.time(), []

    def record(phase: str, st: dict, clean: dict | None, note: str = "") -> None:
        now = C.now_utc().replace(tzinfo=None)
        for t in tables:
            s = st[t]
            same = None if clean is None else (s["rows"], s["fingerprint"]) == (clean[t]["rows"], clean[t]["fingerprint"])
            log.append((drill_id, day, phase, t, order[t], s["rows"], s["fingerprint"], s["snapshot"], s["files"],
                        same, now, note[:500]))
            print(f"DRILL_TABLE {phase:<7} {t:<24} rows={s['rows']} snapshot={s['snapshot']} files={s['files']}"
                  f"{'' if same is None else ' same=' + str(same).lower()}", flush=True)

    if not args.no_clean_run:
        build_silver.run(spark, silver_args)
    clean = state(spark, names, tables, day)
    record("CLEAN", clean, None, "clean run" if not args.no_clean_run else "current main")

    original = C.write_dates
    C.write_dates = failing_write(names.t("silver", args.fail_table), original)
    error = ""
    try:
        build_silver.run(spark, silver_args)
    except InjectedFailure as e:
        error = str(e)
    finally:
        C.write_dates = original
    if not error:
        raise SystemExit("failed_batch_drill: the injected failure did not happen")
    failed = state(spark, names, tables, day)
    after_fail = compare(clean, failed, later)
    rewritten = sorted(t for t in tables if t not in later and failed[t]["snapshot"] != clean[t]["snapshot"])
    record("FAILED", failed, clean, error)

    build_silver.run(spark, silver_args)
    resumed = state(spark, names, tables, day)
    after_resume = compare(clean, resumed, set())
    record("RESUMED", resumed, clean)

    C.replace_rows(spark, drill_table, SCHEMA, f"business_date = DATE '{day.isoformat()}'", log)
    verdict = "PASS" if not after_fail and not after_resume else "FAIL"
    print(f"DRILL_FAILURE {error[:300]}", flush=True)
    print(f"DRILL_RESULT business_date={day} fail_table={args.fail_table} verdict={verdict} "
          f"tables={len(tables)} kept_snapshot={len(later)} recommitted_same_rows={len(rewritten)} "
          f"rows_clean={sum(s['rows'] for s in clean.values())} rows_failed={sum(s['rows'] for s in failed.values())} "
          f"rows_resumed={sum(s['rows'] for s in resumed.values())} seconds={time.time() - t0:.0f}", flush=True)
    for line in after_fail + after_resume:
        print(f"DRILL_DIFF {line}", flush=True)
    if verdict != "PASS":
        raise SystemExit(f"failed_batch_drill: {len(after_fail) + len(after_resume)} differences")
    return {"verdict": verdict, "clean": clean, "failed": failed, "resumed": resumed, "error": error,
            "recommitted": rewritten}


def main() -> int:
    spark = C.get_spark("evx-failed-batch-drill")
    run(spark, sys.argv[1:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
