"""
publish(): the one way gold tables are written.

  audit    before anything is committed, in Spark: the key columns are present and not NULL, the key is unique,
           and every extra check (a SQL predicate that must hold on every row) passes; a failed audit raises
           PublishError and the table is left as it was
  write    one Iceberg commit: mode "dates" replaces the business dates processed (facts, partitioned by
           _business_date); mode "full" replaces the table (dimensions and SCD2 history recomputed as of the date)
  record   ref.load_audit COMMITTED with snapshot ids and row counts; ref.transform_log with the source tables

M1 writes main directly. M2 keeps this interface and writes to an Iceberg branch, audits the branch and
fast-forwards main (docs/WAP_SPIKE.md, decision (a)).
"""
from __future__ import annotations

from dataclasses import dataclass, field

import evx_common as C


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


def publish(spark, df, spec: Spec, names: C.Names, audit: C.Audit, dates: list[str], sources) -> dict:
    target = names.t("gold", spec.table)
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
