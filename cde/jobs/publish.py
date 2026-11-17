"""
publish(): the one way gold tables are written.

  audit    in Spark: the key columns are present and not NULL, the key is unique, and every extra check (a SQL
           predicate that must hold on every row) passes; a failed audit raises PublishError and main is left
           as it was
  write    mode "dates" replaces the business dates processed (facts, partitioned by _business_date); mode
           "full" replaces the table (dimensions and SCD2 history recomputed as of the date)
  record   ref.load_audit COMMITTED with snapshot ids and row counts; ref.transform_log with the source tables;
           in wap mode, ref.publish_log one row per publish (status PUBLISHED | AUDIT_FAILED | MAIN_MOVED | ERROR);
           direct mode writes no publish_log row, exactly as M1

Publish mode (first set wins): Spark conf spark.evx.publish.mode, env EVX_PUBLISH_MODE, config/lakehouse.json
publish.mode, default "direct".

  direct   M1: audit the DataFrame, then write main directly (one commit per write, as before)
  wap      M2, docs/WAP_SPIKE.md decision (a): create the table on main if absent; CREATE BRANCH wap_<run> at main;
           write the branch (<table>.branch_wap_<run>); audit the branch rows of the processed dates and compare
           the count with the DataFrame; pass -> fast_forward main to the branch and drop it; fail -> the branch
           is kept (its snapshot id in ref.publish_log; Impala reads it with FOR SYSTEM_VERSION AS OF <id>) and
           main is untouched. fast_forward needs main to be an ancestor of the branch: one writer per gold table,
           and a main that moved while the branch was open fails the publish (MAIN_MOVED).

Only <prefix>_gold tables (prefix rsingh_evx*) are published; streaming bronze tables never go through here.

Self-test on a scratch table (CDE job rsingh-evx-wap-check):
  python cde/jobs/publish.py --self-test [--db-prefix rsingh_evx] [--keep]
"""
from __future__ import annotations

import os
import re
import sys
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import evx_common as C  # noqa: E402

MODES = ("direct", "wap")
CONF_KEY = "spark.evx.publish.mode"
ENV_KEY = "EVX_PUBLISH_MODE"
NAME = re.compile(r"^[a-z0-9_]+$")
TABLE = re.compile(r"^[a-z0-9_]+\.[a-z0-9_]+$")
PUBLISH_LOG_SCHEMA = (
    "run_id string, pipeline_run string, table_name string, mode string, write_mode string, business_dates string, "
    "branch string, main_snapshot_before bigint, branch_snapshot bigint, main_snapshot_after bigint, "
    "row_count bigint, status string, detail string, started_at timestamp_ntz, ended_at timestamp_ntz")
SCRATCH = "zz_wap_scratch"


class PublishError(RuntimeError):
    pass


@dataclass
class Spec:
    table: str                       # gold table name
    keys: list[str]                  # unique per row (with _business_date for mode "dates")
    mode: str = "dates"              # "dates" | "full"
    checks: dict[str, str] = field(default_factory=dict)   # name -> SQL predicate true on every row
    transform: str = "aggregate"
    note: str = ""


def audit_frame(df, spec: Spec) -> dict:
    from pyspark.sql import functions as F

    keys = spec.keys + (["_business_date"] if spec.mode == "dates" and "_business_date" not in spec.keys else [])
    missing = [k for k in keys if k not in df.columns]
    if missing:
        raise PublishError(f"{spec.table}: key columns missing {missing}")
    any_null = F.col(keys[0]).isNull()
    for k in keys[1:]:
        any_null = any_null | F.col(k).isNull()
    aggs = [F.count(F.lit(1)).alias("rows"), F.sum(F.when(any_null, 1).otherwise(0)).alias("null_keys")]
    for name, pred in spec.checks.items():
        aggs.append(F.sum(F.when(~F.expr(pred) | F.expr(pred).isNull(), 1).otherwise(0)).alias(f"chk_{name}"))
    r = df.agg(*aggs).collect()[0].asDict()
    dup = df.groupBy(*keys).count().where("count > 1").limit(3).collect()
    failed = {k[4:]: v for k, v in r.items() if k.startswith("chk_") and v}
    if r["null_keys"] or dup or failed:
        raise PublishError(f"{spec.table}: audit failed (null keys {r['null_keys']}, duplicate keys "
                           f"{[tuple(x)[:-1] for x in dup]}, checks {failed})")
    return {"rows": int(r["rows"])}


# ---------------------------------------------------------------- mode, names, refs


def publish_mode(spark, cfg: dict | None = None) -> str:
    """spark.evx.publish.mode, then EVX_PUBLISH_MODE, then config/lakehouse.json publish.mode, then "direct"."""
    cfg = C.CFG if cfg is None else cfg
    conf = spark.conf.get(CONF_KEY, None) if spark is not None else None
    for source, value in ((CONF_KEY, conf), (ENV_KEY, os.environ.get(ENV_KEY)),
                          ("config publish.mode", (cfg.get("publish") or {}).get("mode"))):
        if value is not None and str(value).strip():
            mode = str(value).strip().lower()
            if mode not in MODES:
                raise ValueError(f"{source}={value!r}: publish mode must be one of {MODES}")
            return mode
    return "direct"


def gold_target(names: C.Names, table: str) -> str:
    assert names.prefix.startswith("rsingh_evx") and NAME.match(names.prefix), names.prefix
    assert NAME.match(table), table
    target = names.t("gold", table)
    assert target.split(".")[0] == f"{names.prefix}_gold", target
    return target


def branch_name(audit: C.Audit) -> str:
    """wap_<run>: the run id lower-cased, anything outside [a-z0-9_] replaced by _."""
    run = re.sub(r"_+", "_", re.sub(r"[^a-z0-9_]", "_", (audit.run_id or audit.pipeline_run or "").lower())).strip("_")
    b = f"wap_{run}"
    if not run or not NAME.match(b):
        raise PublishError(f"cannot derive a branch name from run {audit.run_id!r}")
    return b


def impala_branch_sql(table: str, snapshot: int) -> str:
    """Impala cannot address an Iceberg branch by name; it reads the branch head by snapshot id."""
    if not isinstance(table, str) or not TABLE.match(table):
        raise ValueError(f"table must be <db>.<table> in [a-z0-9_]: {table!r}")
    if isinstance(snapshot, bool) or not isinstance(snapshot, int):
        raise ValueError(f"snapshot id must be an integer: {snapshot!r}")
    return f"SELECT * FROM {table} FOR SYSTEM_VERSION AS OF {snapshot}"


def refs(spark, table: str) -> dict[str, int]:
    return {r[0]: int(r[1]) for r in spark.sql(f"SELECT name, snapshot_id FROM {table}.refs").collect()}


def main_snapshot(spark, table: str) -> int | None:
    return refs(spark, table).get("main")


def log_publish(spark, names: C.Names, audit: C.Audit, row: dict) -> None:
    t = names.t("ref", "publish_log")
    C.ensure_table(spark, t, PUBLISH_LOG_SCHEMA)
    cols = [c.split()[0] for c in PUBLISH_LOG_SCHEMA.split(", ")]
    row = {"run_id": audit.run_id, "pipeline_run": audit.pipeline_run, "ended_at": C.now_utc(), **row}
    row["detail"] = (row.get("detail") or "")[:2000]
    for k in ("started_at", "ended_at"):
        row[k] = row[k].replace(tzinfo=None)
    spark.createDataFrame([tuple(row.get(c) for c in cols)], PUBLISH_LOG_SCHEMA).writeTo(t).append()


# ---------------------------------------------------------------- publish


def publish(spark, df, spec: Spec, names: C.Names, audit: C.Audit, dates: list[str], sources) -> dict:
    return _publish(spark, df, spec, names, audit, dates, sources, publish_mode(spark))


def _publish(spark, df, spec, names, audit, dates, sources, mode: str) -> dict:
    target = gold_target(names, spec.table)
    if mode == "wap":
        return _publish_wap(spark, df, spec, names, audit, dates, sources, target)
    assert mode == "direct", mode
    started = C.now_utc()
    before = C.snapshot_id(spark, target)
    df = df.persist()
    try:
        stats = audit_frame(df, spec)
        if spec.mode == "dates":
            C.write_dates(spark, df, target, dates)
        elif spec.mode == "full":
            C.replace_table(df, target)
        else:
            raise ValueError(spec.mode)
    finally:
        df.unpersist()
    after = C.snapshot_id(spark, target)
    msg = f"{stats['rows']} rows ({spec.mode}){spec.note}"
    audit.load("gold", spec.table, "COMMITTED", rows_in=None, rows_out=stats["rows"], snapshot_before=before,
               snapshot_after=after, started_at=started, message=msg)
    audit.transform(f"publish {spec.table}", spec.transform, sources, target, None, stats["rows"], msg)
    print(f"  {spec.table:<24} {msg}", flush=True)
    return {"rows": stats["rows"], "snapshot": after}


def prepare_wap_table(spark, df, target: str, spec: Spec) -> None:
    """The table on main (empty, partitioned as the direct create path), wap enabled, new columns added."""
    if not C.table_exists(spark, target):
        parts = ["_business_date"] if spec.mode == "dates" else []
        C._create(spark.createDataFrame([], df.schema), target, parts, {"write.wap.enabled": "true"}).create()
        return
    props = {r[0]: r[1] for r in spark.sql(f"SHOW TBLPROPERTIES {target}").collect()}
    if props.get("write.wap.enabled") != "true":
        spark.sql(f"ALTER TABLE {target} SET TBLPROPERTIES ('write.wap.enabled' = 'true')")
    C.add_new_columns(spark, df, target)


def empty_dates_on_branch(spark, df, ident: str, dates: list[str]) -> None:
    try:
        spark.sql(f"DELETE FROM {ident} WHERE _business_date IN ({C.date_list(dates)})")
    except Exception as e:  # an Iceberg without row-level DML on branches: the same delete as an overwrite
        print(f"  DELETE on {ident} not supported ({C.error_summary(e, 200)}); overwrite by filter", flush=True)
        _empty_by_overwrite(spark, df, ident, dates)


def _empty_by_overwrite(spark, df, ident: str, dates: list[str]) -> None:
    from pyspark.sql import functions as F

    days = [date.fromisoformat(d) for d in dates]   # typed literals: a string IN filter is not pushed to Iceberg
    spark.createDataFrame([], df.schema).writeTo(ident).overwrite(F.col("_business_date").isin(days))


def write_branch(spark, df, spec: Spec, ident: str, dates: list[str]) -> list[str]:
    """C.write_dates / C.replace_table semantics on the branch; returns the business dates the write covers."""
    from pyspark.sql import functions as F

    if spec.mode == "full":
        df.writeTo(ident).overwrite(F.lit(True))
        return []
    if spec.mode != "dates":
        raise ValueError(spec.mode)
    present = sorted({r[0].isoformat() for r in df.select("_business_date").distinct().collect()})
    missing = [d for d in dates if d not in present]
    if missing:
        empty_dates_on_branch(spark, df, ident, missing)
    if present:
        df.writeTo(ident).overwritePartitions()
    return sorted(set(dates) | set(present))


def audit_branch(spark, spec: Spec, target: str, branch: str, scope: list[str], expected_rows: int) -> dict:
    from pyspark.sql import functions as F

    rows = spark.sql(f"SELECT * FROM {target} VERSION AS OF '{branch}'")
    if spec.mode == "dates":
        rows = rows.where(F.col("_business_date").isin(scope))
    stats = audit_frame(rows, spec)
    if stats["rows"] != expected_rows:
        raise PublishError(f"{spec.table}: branch {branch} has {stats['rows']} rows for the dates processed, "
                           f"the DataFrame {expected_rows}")
    return stats


def _publish_wap(spark, df, spec, names, audit, dates, sources, target) -> dict:
    started = C.now_utc()
    branch = branch_name(audit)
    ident = f"{target}.branch_{branch}"
    catalog = spark.catalog.currentCatalog()
    assert NAME.match(catalog), catalog
    log = {"table_name": target, "mode": "wap", "write_mode": spec.mode, "business_dates": ",".join(dates),
           "branch": branch, "started_at": started}
    df = df.persist()
    before = branch_snap = None
    logged = False
    try:
        prepare_wap_table(spark, df, target, spec)
        if branch in refs(spark, target):
            raise PublishError(f"{target}: branch {branch} already exists (one publish per table and run)")
        spark.sql(f"ALTER TABLE {target} CREATE BRANCH {branch}")
        before = refs(spark, target)[branch]
        log["main_snapshot_before"] = before
        scope = write_branch(spark, df, spec, ident, dates)
        branch_snap = refs(spark, target)[branch]
        log["branch_snapshot"] = branch_snap
        expected = df.count()
        try:
            stats = audit_branch(spark, spec, target, branch, scope, expected)
        except PublishError as e:
            logged = True
            log_publish(spark, names, audit, {**log, "status": "AUDIT_FAILED", "detail": str(e),
                                              "main_snapshot_after": main_snapshot(spark, target)})
            print(f"  {spec.table:<24} AUDIT_FAILED, branch {branch} kept at snapshot {branch_snap}: "
                  f"{impala_branch_sql(target, branch_snap)}", flush=True)
            raise
        now = main_snapshot(spark, target)
        if now != before:
            msg = (f"{target}: main moved from {before} to {now} while branch {branch} was open "
                   f"(one writer per gold table); branch kept at snapshot {branch_snap}")
            logged = True
            log_publish(spark, names, audit, {**log, "status": "MAIN_MOVED", "detail": msg, "main_snapshot_after": now})
            print(f"  {spec.table:<24} MAIN_MOVED, branch {branch} kept", flush=True)
            raise PublishError(msg)
        spark.sql(f"CALL {catalog}.system.fast_forward('{target}', 'main', '{branch}')").collect()
        after = main_snapshot(spark, target)
        if after != branch_snap:
            raise PublishError(f"{target}: main at {after} after fast_forward, branch head {branch_snap}")
        spark.sql(f"ALTER TABLE {target} DROP BRANCH {branch}")
    except Exception as e:
        if not logged:
            log_publish(spark, names, audit, {**log, "status": "ERROR", "detail": C.error_summary(e, 2000),
                                              "branch_snapshot": branch_snap})
        raise
    finally:
        df.unpersist()
    msg = f"{stats['rows']} rows ({spec.mode}, wap {branch}){spec.note}"
    audit.load("gold", spec.table, "COMMITTED", rows_in=None, rows_out=stats["rows"], snapshot_before=before,
               snapshot_after=after, started_at=started, message=msg)
    audit.transform(f"publish {spec.table}", spec.transform, sources, target, None, stats["rows"], msg)
    log_publish(spark, names, audit, {**log, "status": "PUBLISHED", "detail": msg, "main_snapshot_after": after,
                                      "row_count": stats["rows"]})
    print(f"  {spec.table:<24} {msg}", flush=True)
    return {"rows": stats["rows"], "snapshot": after}


# ---------------------------------------------------------------- self-test


def self_test(spark, prefix: str = C.DEFAULT_PREFIX, keep: bool = False) -> int:
    """WAP on <prefix>_gold.zz_wap_scratch: a passing batch, a date emptied on the branch, a failing audit."""
    from pyspark.sql import functions as F

    names = C.Names(prefix)
    target = gold_target(names, SCRATCH)
    log_t = names.t("ref", "publish_log")
    C.ensure_databases(spark, names, ("gold", "ref"))
    spark.sql(f"DROP TABLE IF EXISTS {target} PURGE")
    spec = Spec(SCRATCH, ["k"], "dates", {"v_positive": "v > 0"}, "generate")
    d1, d2 = "2026-01-01", "2026-01-02"
    results: list[bool] = []
    run_ids: list[str] = []

    def mark(check: str, ok: bool, detail) -> None:
        results.append(bool(ok))
        print(f"WAP_SELFTEST {check} {'PASS' if ok else 'FAIL'} {detail}", flush=True)

    def batch(n: int, days: list[str], v: str = "1"):
        day = F.when(F.col("id") % len(days) == 0, F.lit(days[0])).otherwise(F.lit(days[-1])).cast("date")
        return spark.range(n).select(F.col("id").alias("k"), F.expr(v).cast("int").alias("v"),
                                     day.alias("_business_date"))

    def new_audit() -> C.Audit:
        a = C.Audit(spark, names, "wap_selftest", date.fromisoformat(d2), None)
        run_ids.append(a.run_id)
        return a

    def count(where: str = "") -> int:
        return int(spark.sql(f"SELECT COUNT(*) FROM {target}" + (f" WHERE {where}" if where else "")).collect()[0][0])

    def last_log(run_id: str) -> dict:
        r = spark.sql(f"SELECT * FROM {log_t} WHERE run_id = '{run_id}' AND table_name = '{target}' "
                      "ORDER BY ended_at DESC LIMIT 1").collect()
        return r[0].asDict() if r else {}

    try:
        a = new_audit()
        out = _publish(spark, batch(40, [d1, d2]), spec, names, a, [d1, d2], [], "wap")
        r, lg = refs(spark, target), last_log(a.run_id)
        mark("pass_publish", count() == 40 and set(r) == {"main"} and lg.get("status") == "PUBLISHED"
             and r["main"] == out["snapshot"] == lg.get("branch_snapshot"),
             f"main_rows={count()} refs={sorted(r)} status={lg.get('status')} main={r.get('main')}")

        a = new_audit()
        _publish(spark, batch(10, [d1]), spec, names, a, [d1, d2], [], "wap")
        mark("date_emptied", count(f"_business_date = DATE '{d1}'") == 10 and count(f"_business_date = DATE '{d2}'") == 0,
             f"{d1}={count(f'_business_date = DATE {d1!r}')} {d2}={count(f'_business_date = DATE {d2!r}')}")

        main_before, rows_before = main_snapshot(spark, target), count()
        a = new_audit()
        try:
            _publish(spark, batch(30, [d1, d2], "-1"), spec, names, a, [d1, d2], [], "wap")
            mark("audit_fail_raises", False, "publish of a failing batch returned")
        except PublishError as e:
            mark("audit_fail_raises", True, str(e)[:160])
        r, lg, b = refs(spark, target), last_log(a.run_id), branch_name(a)
        mark("main_untouched", main_snapshot(spark, target) == main_before and count() == rows_before,
             f"main={main_snapshot(spark, target)} before={main_before} rows={count()}")
        kept = b in r and lg.get("status") == "AUDIT_FAILED" and lg.get("branch_snapshot") == r.get(b)
        mark("branch_kept", kept, f"branch={b} snapshot={r.get(b)} status={lg.get('status')} "
             f"impala: {impala_branch_sql(target, r[b]) if b in r else '-'}")
    except Exception as e:  # reported as a failed check, then cleaned up
        mark("unexpected_error", False, C.error_summary(e, 300))
    finally:
        if not keep:
            for b in refs(spark, target) if C.table_exists(spark, target) else {}:
                if b != "main":
                    spark.sql(f"ALTER TABLE {target} DROP BRANCH {b}")
            spark.sql(f"DROP TABLE IF EXISTS {target} PURGE")
            if C.table_exists(spark, log_t):
                spark.sql(f"DELETE FROM {log_t} WHERE table_name = '{target}'")
            runs = ",".join(repr(x) for x in run_ids if NAME.match(x.replace("-", "_")))
            for t in ("load_audit", "transform_log"):
                if runs and C.table_exists(spark, names.t("ref", t)):
                    spark.sql(f"DELETE FROM {names.t('ref', t)} WHERE run_id IN ({runs})")
            print(f"WAP_SELFTEST cleanup PASS dropped {target} and its log rows", flush=True)
    ok = bool(results) and all(results)
    print(f"WAP_SELFTEST result {'PASS' if ok else 'FAIL'} {sum(results)}/{len(results)} checks", flush=True)
    return 0 if ok else 1


def main(argv=None) -> int:
    p = C.base_parser("publish() self-test on a scratch gold table", dated=False)
    p.add_argument("--self-test", action="store_true")
    p.add_argument("--keep", action="store_true", help="keep the scratch table, branches and log rows")
    args = C.parse(p, argv)
    if not args.self_test:
        p.error("publish.py is a library; run it with --self-test")
    spark = C.get_spark("evx-wap-check")
    return self_test(spark, args.db_prefix, args.keep)


if __name__ == "__main__":
    rc = main()
    if rc:
        raise SystemExit(rc)
