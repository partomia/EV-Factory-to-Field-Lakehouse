"""
Iceberg table maintenance (CDE job rsingh-evx-maintenance, its own schedule 04:30 UTC, after the DAG window).
Every Iceberg table of the layers in config/operations.json maintenance, each name asserted to be
rsingh_evx_<layer>.<table> before any procedure is called. Per table, in this order:

  compact     rewrite_data_files on the stream tables (maintenance.compact.tables): many small files per
              micro-batch. Iceberg stream readers skip the replace snapshot it commits.
  manifests   rewrite_manifests
  branches    wap_* branches whose head is older than stale_branch_days are dropped, unless a ref.publish_log
              row for that table and branch is in flight (no terminal status, no end time, or started inside the
              retention); --keep-branches only reports them
  expire      expire_snapshots older than snapshots_days (7), keeping the last retain_last. Never on a table read
              as a stream by another job (maintenance.no_expire: bronze.telemetry_reading,
              bronze.factory_line_event), so no reader checkpoint can point at an expired snapshot. Snapshots held
              by a branch or tag are kept by Iceberg.
  orphans     remove_orphan_files older than orphan_days (3), measured from the real clock even under --now, so
              the files of an in-flight write are never touched

Gold tables are skipped while the wall clock is inside gold_publish_window_utc (the nightly gold publish); the
clock is read again before each gold table, so a long run cannot drift into the window. Tables that do not
exist yet, and views, are skipped. --dry-run prints the plan and changes nothing (orphans run as a dry run and
report what would go). Procedures are called on the session catalog (spark_catalog on CDE, local on a laptop).

  spark-submit maintenance.py [--dry-run] [--only bronze.telemetry_reading,...] [--keep-branches]
"""
from __future__ import annotations

import re
import sys
import time
from datetime import datetime, timedelta
from datetime import time as dtime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import evx_common as C  # noqa: E402

NAME = re.compile(r"^[a-z0-9_]+$")
PROJECT_TABLE = re.compile(r"^rsingh_evx_[a-z0-9_]+\.[a-z0-9_]+$")
TERMINAL = ("PUBLISHED", "AUDIT_FAILED", "MAIN_MOVED", "ERROR", "DIRECT")


def settings() -> dict:
    return C.load_json("config/operations.json")["maintenance"]


def lit(value) -> str:
    """A SQL string literal, quoted as the app pages do."""
    return "'" + str(value).replace("'", "") + "'"


def assert_project_table(full: str) -> str:
    """Every destructive call goes through here: <rsingh_evx_...>.<table>, lower-case identifiers only."""
    if not isinstance(full, str) or not PROJECT_TABLE.match(full):
        raise AssertionError(f"maintenance refuses a table outside the rsingh_evx_ prefix: {full!r}")
    return full


def qualified(names: C.Names, short: str) -> str:
    layer, _, table = short.partition(".")
    if layer not in C.LAYERS or not NAME.match(table or ""):
        raise AssertionError(f"not a <layer>.<table> name: {short!r}")
    return assert_project_table(names.t(layer, table))


def catalog_name(spark) -> str:
    cat = spark.conf.get("spark.sql.defaultCatalog", "spark_catalog") or "spark_catalog"
    if not NAME.match(cat):
        raise AssertionError(cat)
    return cat


def in_window(now: datetime, window: dict | None) -> bool:
    """now (UTC) inside [start, end); a window may wrap past midnight (23:00 -> 04:00)."""
    if not window:
        return False
    start, end, t = dtime.fromisoformat(window["start"]), dtime.fromisoformat(window["end"]), now.time()
    return start <= t < end if start <= end else (t >= start or t < end)


def stale_branches(refs: list[tuple[str, str, datetime | None]], prefix: str, older_than: datetime) -> list[str]:
    """refs: (name, type, head committed_at) -> branch names with the prefix whose head is older than the cut-off."""
    return sorted(n for n, kind, at in refs if kind == "BRANCH" and n != "main" and n.startswith(prefix)
                  and at is not None and at < older_than)


def in_flight(log_rows: list[tuple[str, str | None, datetime | None, datetime | None]], older_than: datetime) -> set[str]:
    """ref.publish_log rows (branch, status, started_at, ended_at) of one table -> branches a publish may still use."""
    out = set()
    for branch, status, started, ended in log_rows:
        if branch and (status not in TERMINAL or ended is None or (started is not None and started >= older_than)):
            out.add(branch)
    return out


def project_tables(spark, names: C.Names, layers: list[str]) -> list[str]:
    out = []
    for layer in layers:
        db = names.db(layer)
        if not spark.catalog.databaseExists(db):
            continue
        for r in spark.sql(f"SHOW TABLES IN {db}").collect():
            if not getattr(r, "isTemporary", False) and NAME.match(r.tableName):
                out.append(f"{layer}.{r.tableName}")
    return sorted(out)


def is_iceberg(spark, table: str) -> bool:
    try:
        spark.sql(f"SELECT 1 FROM {table}.snapshots LIMIT 1").collect()
        return True
    except Exception:
        return False


def ts_literal(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def publish_log_rows(spark, names: C.Names, full: str) -> list[tuple]:
    log = names.t("ref", "publish_log")
    if not C.table_exists(spark, log):
        return []
    short = full.split(".", 1)[1]
    return [tuple(r) for r in spark.sql(
        f"SELECT branch, status, CAST(started_at AS TIMESTAMP_NTZ), CAST(ended_at AS TIMESTAMP_NTZ) FROM {log} "
        f"WHERE table_name IN ({lit(full)}, {lit(short)})").collect()]


def wall_clock() -> datetime:
    return C.now_utc().replace(tzinfo=None)


def run(spark, argv=None) -> dict:
    p = C.base_parser(__doc__.split("\n\n")[0], dated=False)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--only", default="", help="comma list of <layer>.<table>")
    p.add_argument("--keep-branches", action="store_true", help="report stale wap_* branches, do not drop them")
    p.add_argument("--now", default=None, help="UTC time to measure snapshot and branch ages from (tests, drills)")
    args = C.parse(p, argv)
    names = C.Names(args.db_prefix)
    mcfg = settings()
    only = [t.strip() for t in args.only.split(",") if t.strip()]
    for short in only:
        qualified(names, short)                     # refuse a bad name before Spark is asked anything
    cat = catalog_name(spark)
    real = wall_clock()
    now = datetime.fromisoformat(args.now) if args.now else real
    snap_cutoff = now - timedelta(days=int(mcfg["snapshots_days"]))
    orphan_cutoff = min(now, real) - timedelta(days=int(mcfg["orphan_days"]))
    branch_cutoff = now - timedelta(days=int(mcfg["stale_branch_days"]))
    retain_last = int(mcfg["retain_last"])
    compact, no_expire = set(mcfg["compact"]["tables"]), set(mcfg.get("no_expire", []))
    target_bytes = int(mcfg["compact"]["target_file_size_mb"]) * 1024 * 1024
    min_files = int(mcfg["compact"]["min_input_files"])
    window = mcfg.get("gold_publish_window_utc")
    tables = only or project_tables(spark, names, mcfg["layers"])
    fulls = {short: qualified(names, short) for short in tables}   # every name checked before any call
    mode = "DRY_RUN" if args.dry_run else "RUN"
    print(f"MAINT_START mode={mode} catalog={cat} tables={len(tables)} snapshots_before={ts_literal(snap_cutoff)} "
          f"orphans_before={ts_literal(orphan_cutoff)} gold_window={'inside' if in_window(now, window) else 'outside'}",
          flush=True)
    t0, results = time.time(), {}

    def call(proc: str, full: str, body: str = ""):
        assert_project_table(full)
        return spark.sql(f"CALL {cat}.system.{proc}(table => {lit(full)}{', ' + body if body else ''})").collect()

    for short in tables:
        full, res = fulls[short], results.setdefault(short, {})
        if short.startswith("gold.") and in_window(now if args.now else wall_clock(), window):
            res["skip"] = "inside the gold publish window"
            print(f"MAINT_SKIP {short} reason=gold-publish-window", flush=True)
            continue
        if not C.table_exists(spark, full):
            res["skip"] = "not created yet"
            print(f"MAINT_SKIP {short} reason=not-created-yet", flush=True)
            continue
        if not is_iceberg(spark, full):
            res["skip"] = "not an Iceberg table"
            print(f"MAINT_SKIP {short} reason=not-iceberg", flush=True)
            continue
        try:
            if short in compact:
                if args.dry_run:
                    n = spark.sql(f"SELECT count(*) FROM {full}.files").collect()[0][0]
                    print(f"MAINT_PLAN {short} compact data_files={n}", flush=True)
                else:
                    r = call("rewrite_data_files", full, f"options => map('target-file-size-bytes', {lit(target_bytes)}, "
                                                         f"'min-input-files', {lit(min_files)})")
                    res["compact"] = f"{r[0][0]}->{r[0][1]}" if r else "0->0"
                    print(f"MAINT_DONE {short} compact files={res['compact']}", flush=True)
            if args.dry_run:
                print(f"MAINT_PLAN {short} rewrite_manifests", flush=True)
            else:
                r = call("rewrite_manifests", full)
                res["manifests"] = f"{r[0][0]}->{r[0][1]}" if r else "0->0"
                print(f"MAINT_DONE {short} manifests={res['manifests']}", flush=True)

            committed = {int(r[0]): r[1] for r in spark.sql(
                f"SELECT snapshot_id, CAST(committed_at AS TIMESTAMP_NTZ) FROM {full}.snapshots").collect()}
            refs = [(r[0], r[1], committed.get(int(r[2]))) for r in spark.sql(
                f"SELECT name, type, snapshot_id FROM {full}.refs").collect()]
            stale = stale_branches(refs, mcfg["branch_prefix"], branch_cutoff)
            busy = in_flight(publish_log_rows(spark, names, full), branch_cutoff) if stale else set()
            for b in stale:
                if b in busy:
                    res.setdefault("kept_branches", []).append(b)
                    print(f"MAINT_BRANCH_KEPT {short} branch={b} reason=publish-in-flight", flush=True)
                elif args.dry_run or args.keep_branches or not NAME.match(b):
                    res.setdefault("stale_branches", []).append(b)
                    print(f"MAINT_BRANCH_STALE {short} branch={b} kept", flush=True)
                else:
                    assert_project_table(full)
                    spark.sql(f"ALTER TABLE {full} DROP BRANCH `{b}`")
                    res.setdefault("dropped_branches", []).append(b)
                    print(f"MAINT_DONE {short} dropped_branch={b}", flush=True)

            if short in no_expire:
                res["expire"] = "skipped: read as a stream"
                print(f"MAINT_SKIP {short} expire reason=stream-reader snapshots={len(committed)}", flush=True)
            else:
                older = sum(1 for at in committed.values() if at < snap_cutoff)
                if args.dry_run:
                    print(f"MAINT_PLAN {short} expire snapshots_older={older} of {len(committed)} "
                          f"retain_last={retain_last}", flush=True)
                else:
                    call("expire_snapshots", full, f"older_than => TIMESTAMP {lit(ts_literal(snap_cutoff))}, "
                                                   f"retain_last => {retain_last}")
                    after = spark.sql(f"SELECT count(*) FROM {full}.snapshots").collect()[0][0]
                    res["expire"] = f"{len(committed)}->{after}"
                    print(f"MAINT_DONE {short} expire snapshots={res['expire']}", flush=True)

            r = call("remove_orphan_files", full, f"older_than => TIMESTAMP {lit(ts_literal(orphan_cutoff))}, "
                                                  f"dry_run => {'true' if args.dry_run else 'false'}")
            res["orphans"] = len(r)
            print(f"MAINT_{'PLAN' if args.dry_run else 'DONE'} {short} orphan_files={len(r)}", flush=True)
        except Exception as e:
            res["error"] = C.error_summary(e, 300)
            print(f"MAINT_ERROR {short} {res['error']}", flush=True)
    errors = [t for t, r in results.items() if "error" in r]
    print(f"MAINT_RESULT mode={mode} tables={len(tables)} skipped={sum(1 for r in results.values() if 'skip' in r)} "
          f"expiry_skipped={sum(1 for r in results.values() if str(r.get('expire', '')).startswith('skipped'))} "
          f"errors={len(errors)} seconds={time.time() - t0:.0f}", flush=True)
    if errors:
        raise SystemExit(f"maintenance: {len(errors)} tables failed: {errors}")
    return results


def main() -> int:
    spark = C.get_spark("evx-maintenance")
    run(spark, sys.argv[1:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
