"""
Assertions on the local warehouse after scripts/run_local.py (CI): each planted fault is caught at the layer
config/expected_recon.json names, and nothing else is quarantined.

  python scripts/check_local.py bronze
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from run_local import DATA, local_spark  # noqa: E402


def check_bronze(spark) -> list[str]:
    errors = []

    def rows(sql):
        return [tuple(r) for r in spark.sql(sql).collect()]

    mism = rows("SELECT CAST(business_date AS STRING), source_file FROM rsingh_evx_ref.file_control "
                "WHERE status = 'MISMATCH' ORDER BY 1")
    if mism != [("2026-09-15", "CL1_formation_B.csv"), ("2026-11-07", "CL1_formation_B.csv")]:
        errors.append(f"trailer mismatches {mism}")
    dup = rows("SELECT CAST(business_date AS STRING), source_file FROM rsingh_evx_ref.file_control "
               "WHERE status = 'DUPLICATE_DELTA'")
    if dup != [("2026-11-07", "A_MaterialDocument_resent_001.json")]:
        errors.append(f"re-sent OData pages {dup}")
    sc = rows("SELECT entity_set, field, CAST(first_seen_date AS STRING) FROM rsingh_evx_ref.schema_change")
    if sc != [("A_InspectionLot", "ZZ_MOISTURE_PPM", "2026-11-08")]:
        errors.append(f"schema changes {sc}")
    q = rows("SELECT CAST(_business_date AS STRING), file_name, reject_reason FROM rsingh_evx_bronze.doc_object "
             "WHERE ingest_status <> 'VALID'")
    if [(d, r) for d, _, r in q] != [("2026-11-08", "PNG_TRUNCATED")]:
        errors.append(f"objects not VALID {q}")
    other = rows("SELECT _entity, CAST(_business_date AS STRING), count(*) FROM rsingh_evx_bronze.quarantine "
                 "WHERE NOT array_contains(_reject_reasons, 'file:TRAILER_MISMATCH') "
                 "AND NOT array_contains(_reject_reasons, 'object:PNG_TRUNCATED') GROUP BY 1, 2")
    if other:
        errors.append(f"unexpected quarantine {other}")
    failed = rows("SELECT f.run_id, f.message FROM rsingh_evx_ref.load_audit f WHERE f.status = 'FAILED' "
                  "AND NOT EXISTS (SELECT 1 FROM rsingh_evx_ref.load_audit s WHERE s.stage = f.stage "
                  "AND s.status = 'COMPLETED' AND s.ended_at > f.ended_at)")
    if failed:
        errors.append(f"failed stages {failed}")
    return errors


def check_extract(spark) -> list[str]:
    errors = []
    failed = spark.sql("SELECT 'doc' t, count(*) n FROM rsingh_evx_silver.doc_extract WHERE status = 'FAILED' UNION ALL "
                       "SELECT 'img', count(*) FROM rsingh_evx_silver.qc_image_metric WHERE status = 'FAILED' UNION ALL "
                       "SELECT 'xrf', count(*) FROM rsingh_evx_silver.spectrum_feature WHERE status = 'FAILED' UNION ALL "
                       "SELECT 'edr', count(*) FROM rsingh_evx_silver.edr_event WHERE status = 'FAILED'").collect()
    if any(r["n"] for r in failed):
        errors.append(f"extraction failures {[tuple(r) for r in failed]}")
    ocr = [tuple(r) for r in spark.sql("SELECT CAST(_business_date AS STRING), count(*) FROM "
                                       "rsingh_evx_silver.doc_extract WHERE method = 'OCR' GROUP BY 1").collect()]
    if [d for d, _ in ocr] != ["2026-11-08"]:
        errors.append(f"OCR job cards {ocr}")
    swap = spark.sql("SELECT count(*) FROM rsingh_evx_silver.doc_extract WHERE method = 'OCR' "
                     "AND get_json_object(fields, '$.service_type') = 'PACK REPLACEMENT'").collect()[0][0]
    if swap != 1:
        errors.append(f"OCR pack-swap job card read {swap} times")
    n_obj = spark.sql("SELECT count(*) FROM rsingh_evx_bronze.doc_object WHERE ingest_status <> 'QUARANTINED'").collect()[0][0]
    n_ext = spark.sql("SELECT (SELECT count(*) FROM rsingh_evx_silver.doc_extract) + "
                      "(SELECT count(*) FROM rsingh_evx_silver.qc_image_metric) + "
                      "(SELECT count(*) FROM rsingh_evx_silver.spectrum_feature) + "
                      "(SELECT count(*) FROM rsingh_evx_silver.edr_event)").collect()[0][0]
    if n_obj != n_ext:
        errors.append(f"{n_obj} catalogued objects, {n_ext} extracted rows")
    return errors


def main(argv: list[str]) -> int:
    spark = local_spark(DATA / "warehouse")
    errors = []
    for layer in argv or ["bronze"]:
        errors += {"bronze": check_bronze, "extract": check_extract}[layer](spark)
    for e in errors:
        print(f"FAIL {e}")
    print("OK" if not errors else f"{len(errors)} check(s) failed")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
