"""
Late EDR reprocess (CDE job rsingh-evx-reprocess, after gold): event-recorder dumps land days after their event,
so the gold aggregate keyed by the EVENT date, gold.edr_event_daily, is recomputed for exactly the event dates
that the dumps of the arrival dates processed touch, and for no other date.

  touched    gold.fact_edr_event is partitioned by the ARRIVAL business date (_business_date: the landing folder
             the dump arrived in), so nightly gold already holds a dump in its arrival partition. The event date of
             a dump is the IST date of trigger_ts (the business calendar of the landing folders); the event dates
             touched are those of the dumps that arrived on the arrival dates processed.
  recompute  gold.edr_event_daily for the touched event dates only, one publish() commit (mode dates), from every
             dump of those event dates in gold. Re-running any arrival date gives the same rows (idempotent), and
             replaying an older date never drops a dump that arrived later.
  late       only the landing date is known, not the time of day. A dump is late when it certainly missed the SLA
             (config/operations.json late_data, 72 h): it landed on or after event date + floor(sla_hours / 24) + 1
             (day 4 for 72 h), i.e. more than sla_hours after any trigger time on its event date. A dump on day 3
             may still be within 72 h of its trigger and is on time. A late dump re-opens a closed event date.
  log        ref.reprocess_log, one row per arrival date processed (a re-run replaces them): event dates touched and
             re-opened, dumps and late dumps, events of the touched dates in gold.edr_event_daily before and after,
             snapshots before / after, status REPROCESSED (late dumps), ON_TIME (dumps, none late), NO_DATA or FAILED.

Usage:
  spark-submit reprocess_late.py --business-date 2026-11-10       # nightly, after gold
  spark-submit reprocess_late.py --from 2026-02-09 --to 2026-11-05 # initial load
"""
from __future__ import annotations

import sys
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import evx_common as C  # noqa: E402

STAGE = "reprocess"
AGG = "edr_event_daily"
SOURCE = "gold.fact_edr_event"
IST_MINUTES = 330
TABLE_PREFIX = "rsingh_evx_"
LOG_SCHEMA = ("run_id string, pipeline_run string, arrival_date date, event_dates_touched string, "
              "event_dates_reopened string, source_table string, target_table string, dumps bigint, late_dumps bigint, "
              "late_after_days int, rows_before bigint, rows_after bigint, snapshot_before bigint, "
              "snapshot_after bigint, status string, started_at timestamp_ntz, ended_at timestamp_ntz, message string")


def settings() -> dict:
    return C.load_json("config/operations.json")["late_data"]


def late_after_days(sla_hours: float) -> int:
    """First arrival day (event date + n) on which a dump has certainly waited more than sla_hours."""
    return int(sla_hours // 24) + 1


def plan(arrivals: list[tuple[date, date, int]], arrival_dates: list[date], late_after: int) -> dict:
    """arrivals: (arrival date, event date, dumps) of the arrival dates processed ->
    touched (event dates to recompute), per_arrival {arrival: {"touched": {event: dumps}, "late": {event: dumps}}}."""
    per = {a: {"touched": {}, "late": {}} for a in arrival_dates}
    for arrival, event, n in arrivals:
        p = per.setdefault(arrival, {"touched": {}, "late": {}})
        p["touched"][event] = p["touched"].get(event, 0) + n
        if (arrival - event).days >= late_after:
            p["late"][event] = p["late"].get(event, 0) + n
    touched = sorted({e for p in per.values() for e in p["touched"]})
    return {"touched": touched, "per_arrival": per}


def status_of(p: dict) -> str:
    return "REPROCESSED" if p["late"] else "ON_TIME" if p["touched"] else "NO_DATA"


def guard(table: str) -> str:
    """Destructive writes only on this demo's tables."""
    if not table.startswith(TABLE_PREFIX):
        raise ValueError(f"refusing to write {table}: not a {TABLE_PREFIX}* table")
    return table


def event_date():
    from pyspark.sql import functions as F

    return F.to_date(F.col("trigger_ts") + F.expr(f"INTERVAL {IST_MINUTES} MINUTES"))


def daily(edr, event_dates: list[date], late_after: int):
    """gold.edr_event_daily rows for the given event dates: one row per (event date, trigger kind)."""
    from pyspark.sql import functions as F

    lag = F.datediff(F.col("_arrival"), F.col("_event"))
    return (edr.where(F.col("_event").isin(event_dates))
            .groupBy(F.col("_event").alias("_business_date"), "trigger_kind")
            .agg(F.count(F.lit(1)).alias("events"), F.countDistinct("vin").alias("vehicles"),
                 F.sum(F.when(F.col("battery_related"), 1).otherwise(0)).cast("bigint").alias("battery_related_events"),
                 F.sum(F.when(F.col("safety_floor"), 1).otherwise(0)).cast("bigint").alias("safety_floor_events"),
                 F.sum(F.when(lag >= late_after, 1).otherwise(0)).cast("bigint").alias("late_events"),
                 F.max(lag).alias("max_arrival_lag_days"), F.max("_arrival").alias("last_arrival_date"),
                 F.max("cell_t_max_post_c").alias("cell_t_max_post_c"))
            .withColumn("event_date", F.col("_business_date"))
            .withColumn("_batch_id", F.concat(F.lit("B"), F.date_format("_business_date", "yyyyMMdd"))))


def events_by_date(spark, table: str, event_dates: list[date]) -> dict:
    """{event date: events} in gold.edr_event_daily (empty when the table does not exist yet)."""
    from pyspark.sql import functions as F

    if not event_dates or not C.table_exists(spark, table):
        return {}
    return {r[0]: int(r[1]) for r in spark.table(table).where(F.col("_business_date").isin(event_dates))
            .groupBy("_business_date").agg(F.sum("events")).collect()}


def run(spark, argv=None) -> dict:
    from pyspark.sql import functions as F

    from publish import Spec, publish

    p = C.base_parser(__doc__.split("\n\n")[0])
    p.add_argument("--sla-hours", type=float, default=None, help="default: config/operations.json late_data")
    args = C.parse(p, argv)
    names = C.Names(args.db_prefix)
    C.ensure_databases(spark, names, ("gold", "ref"))
    sla = args.sla_hours or float(settings()["sla_hours"])
    late_after = late_after_days(sla)
    days = C.date_range(args.date_from, args.date_to)
    source, target = names.t("gold", "fact_edr_event"), guard(names.t("gold", AGG))
    log_table = guard(names.t("ref", "reprocess_log"))
    audit = C.Audit(spark, names, "reprocess_late", args.date_to, args.pipeline_run)
    t0, started = time.time(), C.now_utc()
    pl, rows, late_total = {"touched": [], "per_arrival": {}}, [], 0
    before = after = None
    audit.load(STAGE, AGG, "STARTED", message=f"arrival dates {days[0]}..{days[-1]}, late from day {late_after}")
    try:
        if not C.table_exists(spark, source):
            raise RuntimeError(f"{source} does not exist: run gold first")
        edr = spark.table(source).withColumn("_arrival", F.col("_business_date").cast("date")) \
            .withColumn("_event", event_date())
        arrivals = [(r[0], r[1], int(r[2])) for r in edr.where(F.col("_arrival").isin(days))
                    .groupBy("_arrival", "_event").count().collect()]
        pl = plan(arrivals, days, late_after)
        before = after = C.snapshot_id(spark, target)
        n_before = events_by_date(spark, target, pl["touched"])
        n_after = n_before
        if pl["touched"]:
            reopened = sorted({e for x in pl["per_arrival"].values() for e in x["late"]})
            # a dump arrives on or after its event date, so older arrival partitions cannot hold a touched date
            src = edr.where(F.col("_arrival") >= F.lit(pl["touched"][0]))
            res = publish(spark, daily(src, pl["touched"], late_after),
                          Spec(AGG, ["trigger_kind"], "dates", {"late_within_events": "late_events <= events"},
                               "aggregate", f" (event dates touched {len(pl['touched'])}, re-opened "
                                            f"{','.join(d.isoformat() for d in reopened) or '-'})"),
                          names, audit, [d.isoformat() for d in pl["touched"]], [source])
            after = res["snapshot"]
            n_after = events_by_date(spark, target, pl["touched"])
        ended = C.now_utc().replace(tzinfo=None)
        for d in days:
            x = pl["per_arrival"][d]
            n, late = sum(x["touched"].values()), sum(x["late"].values())
            late_total += late
            mine = sorted(x["touched"])
            rows.append((audit.run_id, audit.pipeline_run, d, ",".join(e.isoformat() for e in mine),
                         ",".join(e.isoformat() for e in sorted(x["late"])), SOURCE, f"gold.{AGG}", n, late,
                         late_after, sum(n_before.get(e, 0) for e in mine), sum(n_after.get(e, 0) for e in mine),
                         before, after if n else before, status_of(x), started.replace(tzinfo=None), ended,
                         f"late from arrival day {late_after} (SLA {sla:g} h)"))
        C.replace_rows(spark, log_table, LOG_SCHEMA, f"arrival_date IN ({C.date_list([d.isoformat() for d in days])})",
                       rows)
        audit.load(STAGE, AGG, "COMPLETED", rows_out=late_total, snapshot_before=before, snapshot_after=after,
                   message=f"touched {len(pl['touched'])} event dates, {late_total} late dumps")
    except Exception as e:
        msg = C.error_summary(e)
        ended = C.now_utc().replace(tzinfo=None)
        failed = [(audit.run_id, audit.pipeline_run, d, "", "", SOURCE, f"gold.{AGG}", None, None, late_after,
                   None, None, before, C.snapshot_id(spark, target), "FAILED", started.replace(tzinfo=None), ended,
                   msg) for d in days]
        try:
            C.replace_rows(spark, log_table, LOG_SCHEMA,
                           f"arrival_date IN ({C.date_list([d.isoformat() for d in days])})", failed)
        finally:
            audit.load(STAGE, AGG, "FAILED", message=msg)
        raise
    finally:
        audit.flush()
    tally = {}
    for r in rows:
        tally[r[14]] = tally.get(r[14], 0) + 1
    print(f"REPROCESS_RESULT arrival_dates={len(days)} touched={len(pl['touched'])} late_dumps={late_total} "
          f"{' '.join(f'{k}={v}' for k, v in sorted(tally.items()))} seconds={time.time() - t0:.0f}", flush=True)
    for r in rows:
        if r[7]:
            print(f"REPROCESS_ROW arrival={r[2]} status={r[14]} touched={r[3]} reopened={r[4] or '-'} "
                  f"late_dumps={r[8]} of {r[7]} events_before={r[10]} events_after={r[11]}", flush=True)
    return {"touched": [d.isoformat() for d in pl["touched"]], "late_dumps": late_total, "status": tally,
            "rows": [dict(zip(("arrival_date", "touched", "reopened", "dumps", "late_dumps", "rows_before",
                               "rows_after", "status"), (r[2], r[3], r[4], r[7], r[8], r[10], r[11], r[14])))
                     for r in rows]}


def main() -> int:
    spark = C.get_spark("evx-reprocess-late")
    run(spark, sys.argv[1:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
