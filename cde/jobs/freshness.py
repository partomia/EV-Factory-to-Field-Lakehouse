"""
Freshness per data product (CDE job rsingh-evx-freshness, after recon): one rsingh_evx_ref.freshness row per
product of PLAN.md section 5 and business date, with the SLAs of config/operations.json freshness.

Business dates are decoupled from the wall clock (PLAN.md section 15), so lag is measured on the business calendar
(every calendar date is a business date), never against today's date:

  batch     last business date loaded = the latest ref.load_audit COMMITTED date of the table (stage = its layer,
            or the item's "stage": silver tables written by extract), or max(date_col) when the product names one;
            lag = business date - that date, in business days. Each product lists its bronze, silver and gold tables
  commit    last commit time = the latest Iceberg snapshot of the determining table (UTC)
  stream    latest event time in the table; lag = end of the business date (next midnight UTC) - that time, in
            minutes, clamped at 0. --live measures from the wall clock instead (a producer running in real time)
  status    GREEN within the SLA, AMBER within SLA + amber_extra_days (batch) or SLA x amber_factor_minutes
            (stream), RED beyond, or when a table of the product has never been loaded. The worst check is the
            product's row; every check is in details. Products whose tables do not exist yet are skipped.

A re-run for the same business date replaces that date's rows.

  spark-submit freshness.py --business-date 2026-11-09 [--live]
"""
from __future__ import annotations

import json
import re
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import evx_common as C  # noqa: E402

NAME = re.compile(r"^[a-z0-9_]+$")
TABLE_PREFIX = "rsingh_evx_"
SCHEMA = ("run_id string, pipeline_run string, business_date date, product string, owner string, "
          "status string, determining_table string, last_business_date date, last_commit_at timestamp_ntz, "
          "latest_event_at timestamp_ntz, lag double, lag_unit string, sla double, checks int, details string, "
          "measured_at timestamp_ntz")
RANK = {"GREEN": 0, "AMBER": 1, "RED": 2}


def settings() -> dict:
    return C.load_json("config/operations.json")["freshness"]


def ident(value: str) -> str:
    """A column name from config, only ever used as an identifier."""
    if not NAME.match(value or ""):
        raise ValueError(f"not a column name: {value!r}")
    return value


def table_of(names: C.Names, short: str) -> tuple[str, str, str]:
    layer, _, table = short.partition(".")
    if layer not in C.LAYERS or not NAME.match(table or ""):
        raise ValueError(f"not a <layer>.<table> name: {short!r}")
    return layer, table, names.t(layer, table)


def batch_status(business_date: date, last: date | None, sla_days: int, amber_extra: int) -> tuple[str, float | None]:
    if last is None:
        return "RED", None
    lag = max((business_date - last).days, 0)
    return ("GREEN" if lag <= sla_days else "AMBER" if lag <= sla_days + amber_extra else "RED"), float(lag)


def stream_status(reference: datetime, latest: datetime | None, sla_minutes: float,
                  factor: float) -> tuple[str, float | None]:
    if latest is None:
        return "RED", None
    lag = max((reference - latest).total_seconds() / 60.0, 0.0)
    return ("GREEN" if lag <= sla_minutes else "AMBER" if lag <= sla_minutes * factor else "RED"), round(lag, 1)


def worst(checks: list[dict]) -> dict:
    return max(checks, key=lambda c: (RANK[c["status"]], c["lag"] if c["lag"] is not None else float("inf")))


def run(spark, argv=None) -> list[dict]:
    p = C.base_parser(__doc__.split("\n\n")[0])
    p.add_argument("--live", action="store_true", help="stream lag from the wall clock")
    args = C.parse(p, argv)
    names = C.Names(args.db_prefix)
    C.ensure_databases(spark, names, ("ref",))
    fcfg = settings()
    audit_t = names.t("ref", "load_audit")
    C.ensure_table(spark, audit_t, C.LOAD_AUDIT_SCHEMA)
    if args.date_to is None:
        row = spark.sql(f"SELECT max(business_date) FROM {audit_t} WHERE status = 'COMMITTED'").collect()
        args.date_to = row[0][0] if row and row[0][0] else C.now_utc().date()
    bd = args.date_to
    bd_sql = f"DATE '{bd.isoformat()}'"
    reference = C.now_utc().replace(tzinfo=None) if args.live else datetime.combine(bd + timedelta(days=1),
                                                                                    datetime.min.time())
    audit = C.Audit(spark, names, "freshness", bd, args.pipeline_run)
    t0, measured = time.time(), C.now_utc().replace(tzinfo=None)
    loaded = {(r[0], r[1]): r[2] for r in spark.sql(
        f"SELECT stage, entity, max(business_date) FROM {audit_t} WHERE status = 'COMMITTED' "
        f"AND business_date <= {bd_sql} GROUP BY 1, 2").collect()}

    def last_commit(full: str):
        r = spark.sql(f"SELECT CAST(max(committed_at) AS TIMESTAMP_NTZ) FROM {full}.snapshots").collect()
        return r[0][0] if r else None

    out = []
    for product, spec in fcfg["products"].items():
        checks = []
        for item in spec.get("batch", []):
            item = {"table": item} if isinstance(item, str) else item
            layer, table, full = table_of(names, item["table"])
            if not C.table_exists(spark, full):
                continue
            if item.get("date_col"):
                col = ident(item["date_col"])
                last = spark.sql(f"SELECT max(CAST({col} AS DATE)) FROM {full} "
                                 f"WHERE CAST({col} AS DATE) <= {bd_sql}").collect()[0][0]
            else:
                last = loaded.get((ident(item.get("stage", layer)), table))
            status, lag = batch_status(bd, last, int(spec["sla_days"]), int(fcfg["amber_extra_days"]))
            checks.append({"table": item["table"], "status": status, "lag": lag, "unit": "business_days",
                           "sla": float(spec["sla_days"]), "last": last, "commit": last_commit(full), "event": None})
        for item in spec.get("stream", []):
            _, _, full = table_of(names, item["table"])
            if not C.table_exists(spark, full):
                continue
            col = ident(item["time_col"])
            latest = spark.sql(f"SELECT CAST(max({col}) AS TIMESTAMP_NTZ) FROM {full} "
                               f"WHERE CAST({col} AS TIMESTAMP_NTZ) < TIMESTAMP_NTZ "
                               f"'{reference:%Y-%m-%d %H:%M:%S}'").collect()[0][0]
            status, lag = stream_status(reference, latest, float(spec["sla_minutes"]),
                                        float(fcfg["amber_factor_minutes"]))
            checks.append({"table": item["table"], "status": status, "lag": lag, "unit": "minutes",
                           "sla": float(spec["sla_minutes"]), "last": None, "commit": last_commit(full),
                           "event": latest})
        if not checks:
            print(f"FRESHNESS_SKIP {product} reason=no-table-yet", flush=True)
            continue
        w = worst(checks)
        details = json.dumps([{k: (str(v) if isinstance(v, (date, datetime)) else v) for k, v in c.items()
                               if k in ("table", "status", "lag", "unit", "last")} for c in checks])
        out.append({"product": product, "owner": spec["owner"], "status": w["status"], "table": w["table"],
                    "last": w["last"], "commit": w["commit"], "event": w["event"], "lag": w["lag"], "unit": w["unit"],
                    "sla": w["sla"], "checks": len(checks), "details": details})
        print(f"FRESHNESS {product} {w['status']} table={w['table']} lag={w['lag']} {w['unit']} sla={w['sla']} "
              f"last={w['last'] or w['event']}", flush=True)
    rows = [(audit.run_id, audit.pipeline_run, bd, r["product"], r["owner"], r["status"], r["table"], r["last"],
             r["commit"], r["event"], r["lag"], r["unit"], r["sla"], r["checks"], r["details"][:4000], measured)
            for r in out]
    target = names.t("ref", "freshness")
    if not target.startswith(TABLE_PREFIX):
        raise ValueError(f"refusing to write {target}: not a {TABLE_PREFIX}* table")
    C.replace_rows(spark, target, SCHEMA, f"business_date = {bd_sql}", rows)
    tally = {}
    for r in out:
        tally[r["status"]] = tally.get(r["status"], 0) + 1
    audit.load("freshness", "*", "COMPLETED", rows_out=len(rows), message=f"{tally} (reference {reference})")
    audit.flush()
    print(f"FRESHNESS_RESULT business_date={bd} products={len(out)} "
          f"{' '.join(f'{k}={v}' for k, v in sorted(tally.items()))} seconds={time.time() - t0:.0f}", flush=True)
    return out


def main() -> int:
    spark = C.get_spark("evx-freshness")
    run(spark, sys.argv[1:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
