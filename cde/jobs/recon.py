"""
Reconciliation per layer for the business dates processed, compared with config/expected_recon.json.

  counts      rows per business date, bronze -> silver -> gold for each entity that maps one to one
              (and the warranty amount sum): MATCH or MISMATCH
  exceptions  what the layers set aside or flagged: file trailers and re-sent delta pages (ref.file_control),
              quarantined objects and records (bronze.quarantine), contract diffs (ref.schema_change),
              OCR fallbacks and failed extracts, silver typing issues, orphan genealogy and VIN reviews (master),
              pack-link changes (gold dim_vehicle). Each must match an expected row (layer, entity, check, date):
              it takes that row's outcome (EXPLAINED / MISMATCH) and fault id, else UNEXPECTED
  missing     an expected row of a built milestone whose date is processed and that was not observed: MISSING
  info        identities with no match (field data older than the factory history), counted only

Writes rsingh_evx_ref.recon_result (the dates processed are replaced). Fails on MISMATCH counts, UNEXPECTED
or MISSING rows.

Usage:
  spark-submit recon.py --from 2026-02-09 --to 2026-11-05
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import evx_common as C  # noqa: E402

COUNTS = [  # (layer, entity, upstream table, downstream table)
    ("silver", "cell_step", "bronze.cell_mes_step", "silver.cell_step"),
    ("silver", "qc_inspection", "bronze.cell_qc_inspection", "silver.qc_inspection"),
    ("silver", "formation_curve_point", "bronze.formation_curve_point", "silver.formation_curve_point"),
    ("silver", "telematics_daily", "bronze.telematics_daily", "silver.telematics_daily"),
    ("silver", "dtc_event", "bronze.dtc_history", "silver.dtc_event"),
    ("silver", "warranty_claim", "bronze.warranty_claims", "silver.warranty_claim"),
    ("silver", "rider", "bronze.dms_riders", "silver.rider"),
    ("silver", "pack_build", "bronze.pack_build_doc", "silver.pack_build"),
    ("silver", "vehicle_build", "bronze.vehicle_eol_doc", "silver.vehicle_build"),
    ("gold", "fact_cell_process", "silver.cell_step", "gold.fact_cell_process"),
    ("gold", "fact_qc_inspection", "silver.qc_inspection", "gold.fact_qc_inspection"),
    ("gold", "fact_pack_eol", "silver.pack_build", "gold.fact_pack_eol"),
    ("gold", "fact_vehicle_eol", "silver.vehicle_build", "gold.fact_vehicle_eol"),
    ("gold", "fact_vehicle_daily", "silver.telematics_daily", "gold.fact_vehicle_daily"),
    ("gold", "fact_dtc", "silver.dtc_event", "gold.fact_dtc"),
    ("gold", "fact_service_job", "silver.service_job", "gold.fact_service_job"),
    ("gold", "fact_warranty_claim", "silver.warranty_claim", "gold.fact_warranty_claim"),
]
SUMS = [("gold", "fact_warranty_claim amount_inr", "silver.warranty_claim", "gold.fact_warranty_claim", "amount_inr")]
FILE_OK = ("OK", "LOADED", "MATCH", "NO_CONTROL")
REVIEW_MATCH = ("VIN_TRANSPOSITION", "VIN_EDIT")
SCHEMA = ("run_id string, business_date date, layer string, entity string, check_name string, observed bigint, "
          "expected bigint, status string, fault_id int, detail string, logged_at timestamp")


def exceptions_sql(n: C.Names, dl: str) -> list[tuple[str, str]]:
    """(layer, SQL returning business_date, entity, check, n, detail) for each exception source."""
    b, s, m, g, r = (n.db(x) for x in ("bronze", "silver", "master", "gold", "ref"))
    fc = ",".join(f"'{x}'" for x in FILE_OK)
    rv = ",".join(f"'{x}'" for x in REVIEW_MATCH)
    return [
        ("bronze", f"SELECT business_date, entity, CASE status WHEN 'MISMATCH' THEN 'trailer count' "
                   f"WHEN 'DUPLICATE_DELTA' THEN 'delta-token duplicate' ELSE concat('file ', lower(status)) END, "
                   f"count(*), concat_ws('; ', collect_list(source_file)) FROM {r}.file_control "
                   f"WHERE business_date IN ({dl}) AND status NOT IN ({fc}) GROUP BY 1, 2, 3"),
        ("bronze", f"SELECT _business_date, _entity, CASE WHEN _entity = 'doc_object' THEN 'decode' ELSE 'contract' END, "
                   f"count(*), concat_ws('; ', collect_set(array_join(_reject_reasons, ','))) FROM {b}.quarantine "
                   f"WHERE _business_date IN ({dl}) AND NOT exists(_reject_reasons, x -> x LIKE 'file:%') GROUP BY 1, 2, 3"),
        ("bronze", f"SELECT first_seen_date, entity, 'contract diff', count(*), concat_ws('; ', collect_list(field)) "
                   f"FROM {r}.schema_change WHERE first_seen_date IN ({dl}) GROUP BY 1, 2, 3"),
        ("extract", f"SELECT _business_date, 'doc_extract', CASE WHEN status = 'FAILED' THEN 'extract failed' "
                    f"ELSE 'text layer' END, count(*), concat_ws('; ', collect_list(file_name)) FROM {s}.doc_extract "
                    f"WHERE _business_date IN ({dl}) AND (method = 'OCR' OR status = 'FAILED') GROUP BY 1, 2, 3"),
        ("extract", f"SELECT _business_date, 'qc_image_metric', 'extract failed', count(*), '' FROM {s}.qc_image_metric "
                    f"WHERE _business_date IN ({dl}) AND status = 'FAILED' GROUP BY 1"),
        ("master", f"SELECT first_seen_date, 'genealogy', 'orphan cell', count(*), concat_ws('; ', collect_list(pack_serial)) "
                   f"FROM {m}.genealogy WHERE link_status = 'ORPHAN' AND first_seen_date IN ({dl}) GROUP BY 1"),
        ("master", f"SELECT first_seen_date, 'identity_review', 'identity match', count(*), "
                   f"concat_ws('; ', collect_list(concat(source_key, '~', candidate_id))) FROM {r}.identity_review "
                   f"WHERE rule IN ({rv}) AND first_seen_date IN ({dl}) GROUP BY 1"),
        ("gold", f"SELECT to_date(valid_from), 'dim_vehicle', 'link change', count(*), concat_ws('; ', collect_list(vin)) "
                 f"FROM {g}.dim_vehicle WHERE version > 1 AND pack_serial <> eol_pack_serial "
                 f"AND to_date(valid_from) IN ({dl}) GROUP BY 1"),
    ]


def silver_typing_sql(spark, n: C.Names, dl: str) -> list[str]:
    out = []
    for t in [r[1] for r in spark.sql(f"SHOW TABLES IN {n.db('silver')}").collect()]:
        if "_dq_issues" in spark.table(n.t("silver", t)).columns:
            out.append(f"SELECT _business_date, '{t}', 'contract typing', count(*), '' FROM {n.t('silver', t)} "
                       f"WHERE _business_date IN ({dl}) AND size(_dq_issues) > 0 GROUP BY 1")
    return out


def classify(observed: list[tuple], expected: list[dict], built: set[str], dates: set[str]) -> list[tuple]:
    """observed: (date, layer, entity, check, n, detail) -> (date, layer, entity, check, n, status, fault_id, detail),
    plus MISSING rows for expected faults of a built milestone on a processed date that were not observed."""
    out, seen = [], set()
    for d, layer, entity, check, n, detail in observed:
        hit = next((e for e in expected if (e["layer"], e["entity"], e["check"]) == (layer, entity, check)
                    and d in e["dates"]), None)
        if hit:
            seen.add((hit["id"], d))
        out.append((d, layer, entity, check, n, hit["outcome"] if hit else "UNEXPECTED", hit["id"] if hit else None,
                    detail))
    for e in expected:
        for d in e["dates"]:
            if e["milestone"] in built and d in dates and (e["id"], d) not in seen:
                out.append((d, e["layer"], e["entity"], e["check"], 0, "MISSING", e["id"], e["fault"]))
    return out


def run(spark, argv=None) -> dict:
    p = C.base_parser(__doc__.split("\n\n")[0])
    args = C.parse(p, argv)
    n = C.Names(args.db_prefix)
    days = C.date_range(args.date_from, args.date_to)
    dates = [d.isoformat() for d in days]
    dl = C.date_list(dates)
    spec = C.load_json("config/expected_recon.json")
    audit = C.Audit(spark, n, "recon", args.date_to, args.pipeline_run)
    run_id, t0, rows = audit.run_id, time.time(), []
    try:
        for layer, entity, up, down in COUNTS:
            a, b = (dict(spark.sql(f"SELECT CAST(_business_date AS STRING), count(*) FROM {n.db(t.split('.')[0])}."
                                   f"{t.split('.')[1]} WHERE _business_date IN ({dl}) GROUP BY 1").collect())
                    for t in (up, down))
            for d in dates:
                if a.get(d, 0) or b.get(d, 0):
                    rows.append((d, layer, entity, "row count", b.get(d, 0), a.get(d, 0),
                                 "MATCH" if a.get(d, 0) == b.get(d, 0) else "MISMATCH", None, f"{up} -> {down}"))
        for layer, label, up, down, col in SUMS:
            a, b = (dict(spark.sql(f"SELECT CAST(_business_date AS STRING), round(sum({col}), 2) FROM "
                                   f"{n.db(t.split('.')[0])}.{t.split('.')[1]} WHERE _business_date IN ({dl}) "
                                   "GROUP BY 1").collect()) for t in (up, down))
            for d in sorted(set(a) | set(b)):
                rows.append((d, layer, label, "sum", int(round(b.get(d) or 0)), int(round(a.get(d) or 0)),
                             "MATCH" if a.get(d) == b.get(d) else "MISMATCH", None, f"{up} -> {down}"))
        observed = []
        for layer, sql in exceptions_sql(n, dl) + [("silver", q) for q in silver_typing_sql(spark, n, dl)]:
            for d, entity, check, cnt, detail in spark.sql(sql).collect():
                observed.append((d.isoformat(), layer, entity, check, cnt, (detail or "")[:500]))
        for d, layer, entity, check, cnt, status, fid, detail in classify(
                observed, spec["faults"], set(spec["built_milestones"]), set(dates)):
            rows.append((d, layer, entity, check, cnt, None, status, fid, detail))
        info = spark.sql(f"SELECT CAST(first_seen_date AS STRING), rule, count(*) FROM {n.t('ref', 'identity_review')} "
                         f"WHERE rule NOT IN ({','.join(repr(x) for x in REVIEW_MATCH)}) AND first_seen_date IN ({dl}) "
                         "GROUP BY 1, 2").collect()
        rows += [(d, "master", "identity_review", rule.lower(), cnt, None, "INFO", None, "") for d, rule, cnt in info]

        now = C.now_utc()
        out = [(run_id, C.parse_date(d), layer, entity, check, obs, exp, status, fid, detail, now)
               for d, layer, entity, check, obs, exp, status, fid, detail in rows]
        C.replace_rows(spark, n.t("ref", "recon_result"), SCHEMA, f"business_date IN ({dl})", out)
        bad = [r for r in rows if r[6] in ("UNEXPECTED", "MISSING") or (r[6] == "MISMATCH" and r[7] is None)]
        tally = {}
        for r in rows:
            tally[r[6]] = tally.get(r[6], 0) + 1
        audit.load("recon", "*", "FAILED" if bad else "COMPLETED", rows_out=len(rows),
                   message=f"{tally}; {len(bad)} failing")
    except Exception as e:
        audit.load("recon", "*", "FAILED", message=C.error_summary(e))
        raise
    finally:
        audit.flush()
    print(f"RECON_RESULT dates={len(dates)} rows={len(rows)} {' '.join(f'{k}={v}' for k, v in sorted(tally.items()))} "
          f"failing={len(bad)} seconds={time.time() - t0:.0f}", flush=True)
    for r in sorted(rows):
        if r[6] not in ("MATCH", "INFO"):
            print(f"RECON_ROW {r[0]} {r[1]} {r[2]} '{r[3]}' {r[4]} {r[6]} fault={r[7]} {r[8][:120]}", flush=True)
    if bad:
        raise SystemExit(f"recon: {len(bad)} failing rows")
    return tally


def main() -> int:
    spark = C.get_spark("evx-recon")
    run(spark, sys.argv[1:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
