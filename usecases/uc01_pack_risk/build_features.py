"""
uc01 factory features at vehicle EOL, and the training example that carries the label.

  features.pack_factory_v1       one row per vehicle whose pack EOL is loaded. Factory and QC only,
                                 as of eol_ts. Shared: no label, no field event.
  gold.uc01_pack_eol_example     the same row plus the label and the safety floor.

Features: grading spread across the pack's cells (capacity, IR, K-value), a robust formation
z-score against the cell's own batch median, X-ray and film failures, wire-bond minimum pull,
the supplier batch's quality status in the SCD2 version valid at eol_ts, station and shift.
The Isolation Forest on formation summaries is fit in the train step, on TRAIN batches only.

Label: a battery DTC, a battery warranty claim or a battery EDR within label_days of entering
service (delivery, else the EOL date). Matured: label_days have passed by the last telematics
date loaded. Safety floor: some other vehicle of the same powder lot already had a thermal
event before this EOL (known at score time; the vehicle's own later events are the label).

  spark-submit usecases/uc01_pack_risk/build_features.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "cde" / "jobs"))
import evx_common as C  # noqa: E402

FEATURE_SET = "pack_factory_v1"
CATEGORICAL = ["pack_line", "shift", "wb_station", "batch_status"]
NUMERIC = [
    "wb_pull_min_g", "n_linked", "n_graded", "n_grade_missing", "genealogy_open",
    "cap_mean", "cap_std", "cap_p10", "cap_p90", "ir_mean", "ir_p90", "k_mean", "k_p90",
    "n_reject", "n_grade_b", "ce_mean", "ce_std", "formation_z_max",
    "n_xray_fail", "n_film_fail", "overhang_mean", "overhang_max", "film_score_max", "batch_suspect",
]
FEATURE_COLUMNS = CATEGORICAL + NUMERIC
# Field outcomes. The factory query must not name these tables (tests/test_uc01_features.py).
OUTCOME_TABLES = ("fact_dtc", "fact_warranty_claim", "fact_edr_event", "fact_vehicle_daily")

# Factory and QC only. {g} is the gold database.
FACTORY_SQL = """
WITH veh AS (
    SELECT vin, eol_pack_serial AS pack_serial, eol_ts, CAST(eol_ts AS DATE) AS eol_date,
           coalesce(delivery_date, CAST(eol_ts AS DATE)) AS in_service_date
    FROM {g}.dim_vehicle
    WHERE is_current AND eol_pack_serial IS NOT NULL
),
ce_med AS (
    SELECT batch_id, percentile_approx(ce, 0.5) AS ce_med
    FROM {g}.fact_cell_grading WHERE ce IS NOT NULL GROUP BY batch_id
),
ce_mad AS (
    SELECT m.batch_id, m.ce_med, percentile_approx(abs(c.ce - m.ce_med), 0.5) AS ce_mad
    FROM {g}.fact_cell_grading c JOIN ce_med m ON c.batch_id = m.batch_id
    WHERE c.ce IS NOT NULL GROUP BY m.batch_id, m.ce_med
),
xray AS (
    SELECT cell_id, max(CASE WHEN verdict = 'FAIL' THEN 1 ELSE 0 END) AS xray_fail, max(score) AS overhang
    FROM {g}.fact_qc_inspection WHERE inspection = 'XRAY' GROUP BY cell_id
),
film AS (
    SELECT batch_id, max(score) AS film_score, sum(CASE WHEN verdict = 'FAIL' THEN 1 ELSE 0 END) AS film_fail
    FROM {g}.fact_qc_inspection WHERE inspection = 'FILM' GROUP BY batch_id
),
cells AS (
    SELECT g.pack_serial, c.capacity_ah, c.ir_mohm, c.k_value_mv_day, c.ce, c.grade, c.powder_lot,
           CASE WHEN c.ce IS NULL OR m.ce_mad IS NULL OR m.ce_mad = 0 THEN 0.0
                ELSE abs(c.ce - m.ce_med) / m.ce_mad END AS formation_z,
           coalesce(q.xray_fail, 0) AS xray_fail, q.overhang, f.film_score, coalesce(f.film_fail, 0) AS film_fail
    FROM {g}.bridge_genealogy g
    LEFT JOIN {g}.fact_cell_grading c ON c.cell_id = g.cell_id
    LEFT JOIN ce_mad m ON m.batch_id = c.batch_id
    LEFT JOIN xray q ON q.cell_id = g.cell_id
    LEFT JOIN film f ON f.batch_id = c.batch_id
),
pack_cells AS (
    SELECT pack_serial, count(*) AS n_linked, count(capacity_ah) AS n_graded,
           avg(capacity_ah) AS cap_mean, stddev_samp(capacity_ah) AS cap_std,
           percentile_approx(capacity_ah, 0.1) AS cap_p10, percentile_approx(capacity_ah, 0.9) AS cap_p90,
           avg(ir_mohm) AS ir_mean, percentile_approx(ir_mohm, 0.9) AS ir_p90,
           avg(k_value_mv_day) AS k_mean, percentile_approx(k_value_mv_day, 0.9) AS k_p90,
           sum(CASE WHEN grade = 'REJECT' THEN 1 ELSE 0 END) AS n_reject,
           sum(CASE WHEN grade = 'B' THEN 1 ELSE 0 END) AS n_grade_b,
           avg(ce) AS ce_mean, stddev_samp(ce) AS ce_std, max(formation_z) AS formation_z_max,
           sum(xray_fail) AS n_xray_fail, max(film_fail) AS n_film_fail, avg(overhang) AS overhang_mean,
           max(overhang) AS overhang_max, max(film_score) AS film_score_max
    FROM cells GROUP BY pack_serial
),
powder AS (
    SELECT pack_serial, powder_lot AS supplier_batch FROM (
        SELECT pack_serial, powder_lot,
               row_number() OVER (PARTITION BY pack_serial ORDER BY n DESC, powder_lot) AS rn
        FROM (SELECT pack_serial, powder_lot, count(*) AS n FROM cells
              WHERE powder_lot IS NOT NULL GROUP BY pack_serial, powder_lot) c
    ) r WHERE rn = 1
),
batch_at_eol AS (
    SELECT vin, quality_status FROM (
        SELECT v.vin, mb.quality_status,
               row_number() OVER (PARTITION BY v.vin ORDER BY mb.valid_from DESC) AS rn
        FROM veh v
        JOIN powder p ON p.pack_serial = v.pack_serial
        JOIN {g}.dim_material_batch mb ON mb.charg = p.supplier_batch
         AND CAST(mb.valid_from AS TIMESTAMP) <= CAST(v.eol_ts AS TIMESTAMP)
         AND (mb.valid_to IS NULL OR CAST(v.eol_ts AS TIMESTAMP) < CAST(mb.valid_to AS TIMESTAMP))
    ) q WHERE rn = 1
)
SELECT v.vin, v.pack_serial, v.eol_date AS as_of, v.eol_ts, v.in_service_date, p.supplier_batch,
       k.pack_line, k.shift, k.wb_station, k.wb_pull_min_g,
       coalesce(c.n_linked, 0) AS n_linked, coalesce(c.n_graded, 0) AS n_graded,
       coalesce(c.n_linked, 0) - coalesce(c.n_graded, 0) AS n_grade_missing,
       CASE WHEN k.genealogy_status <> 'COMPLETE' OR coalesce(c.n_graded, 0) < coalesce(c.n_linked, 0)
            THEN 1 ELSE 0 END AS genealogy_open,
       c.cap_mean, coalesce(c.cap_std, 0) AS cap_std, c.cap_p10, c.cap_p90, c.ir_mean, c.ir_p90,
       c.k_mean, c.k_p90, coalesce(c.n_reject, 0) AS n_reject, coalesce(c.n_grade_b, 0) AS n_grade_b,
       c.ce_mean, coalesce(c.ce_std, 0) AS ce_std, coalesce(c.formation_z_max, 0) AS formation_z_max,
       coalesce(c.n_xray_fail, 0) AS n_xray_fail, coalesce(c.n_film_fail, 0) AS n_film_fail,
       c.overhang_mean, c.overhang_max, c.film_score_max,
       coalesce(b.quality_status, 'UNKNOWN') AS batch_status,
       CASE WHEN b.quality_status = 'SUSPECT' THEN 1 ELSE 0 END AS batch_suspect
FROM veh v
JOIN {g}.fact_pack_eol k ON k.pack_serial = v.pack_serial
LEFT JOIN pack_cells c ON c.pack_serial = v.pack_serial
LEFT JOIN powder p ON p.pack_serial = v.pack_serial
LEFT JOIN batch_at_eol b ON b.vin = v.vin
"""

# Reads the factory rows (view uc01_factory) plus field outcomes. {g} gold, {days} the label window.
LABEL_SQL = """
WITH asof AS (SELECT max(_business_date) AS data_as_of FROM {g}.fact_vehicle_daily),
events AS (
    SELECT vin, CAST(event_ts AS DATE) AS d, 'DTC' AS kind FROM {g}.fact_dtc WHERE battery_related
    UNION ALL
    SELECT resolved_vin, failure_date, 'WARRANTY' FROM {g}.fact_warranty_claim
    WHERE battery_related AND resolved_vin IS NOT NULL
    UNION ALL
    SELECT vin, CAST(trigger_ts AS DATE) AS d, 'EDR' FROM {g}.fact_edr_event WHERE battery_related
),
first_event AS (
    SELECT vin, d AS label_date, kind AS label_kind FROM (
        SELECT e.vin, e.d, e.kind, row_number() OVER (PARTITION BY e.vin ORDER BY e.d, e.kind) AS rn
        FROM events e
        JOIN uc01_factory f ON f.vin = e.vin
        WHERE datediff(e.d, f.in_service_date) BETWEEN 0 AND {days}
    ) q WHERE rn = 1
),
thermal AS (
    SELECT vin, CAST(event_ts AS DATE) AS d FROM {g}.fact_dtc WHERE safety_floor
    UNION ALL
    SELECT vin, CAST(trigger_ts AS DATE) AS d FROM {g}.fact_edr_event WHERE safety_floor
),
floor AS (
    SELECT f.vin, 1 AS safety_floor
    FROM uc01_factory f
    JOIN uc01_factory o ON o.supplier_batch = f.supplier_batch AND o.vin <> f.vin
    JOIN thermal t ON t.vin = o.vin AND t.d < f.as_of
    WHERE f.supplier_batch IS NOT NULL
    GROUP BY f.vin
)
SELECT f.vin, a.data_as_of,
       CASE WHEN datediff(a.data_as_of, f.in_service_date) >= {days} THEN 1 ELSE 0 END AS matured,
       CASE WHEN e.vin IS NOT NULL THEN 1 ELSE 0 END AS label, e.label_kind, e.label_date,
       coalesce(s.safety_floor, 0) AS safety_floor
FROM uc01_factory f
CROSS JOIN asof a
LEFT JOIN first_event e ON e.vin = f.vin
LEFT JOIN floor s ON s.vin = f.vin
"""


def feature_hash(df, salt: str):
    from pyspark.sql import functions as F

    parts = [F.lit(salt)]
    for c in FEATURE_COLUMNS:
        col = F.col(c)
        if c not in CATEGORICAL:
            col = F.round(col.cast("double"), 6)
        parts.append(F.coalesce(col.cast("string"), F.lit("")))
    return df.withColumn("feature_hash", F.sha2(F.concat_ws("|", *parts), 256)) \
             .withColumn("feature_set", F.lit(FEATURE_SET))


def run(spark, argv=None) -> dict:
    p = C.base_parser(__doc__.split("\n\n")[0], dated=False)
    args = C.parse(p, argv)
    names = C.Names(args.db_prefix)
    C.ensure_databases(spark, names, ("features", "gold", "ref"))
    g = names.db("gold")
    days = int(C.config(args.overlay)["calendar"]["label_days"])
    salt = C.config(args.overlay).get("features", {}).get("hash_salt", "platform")
    audit = C.Audit(spark, names, "uc01_features", C.parse_date(C.config(args.overlay)["calendar"]["history_end"]),
                    args.pipeline_run)
    try:
        factory = feature_hash(spark.sql(FACTORY_SQL.format(g=g)), salt)
        factory.createOrReplaceTempView("uc01_factory")
        labels = spark.sql(LABEL_SQL.format(g=g, days=days))
        example = factory.join(labels, "vin", "left")
        feat_table = names.t("features", FEATURE_SET)
        ex_table = names.t("gold", "uc01_pack_eol_example")
        stored = factory.drop("eol_ts", "in_service_date")
        C.replace_table(stored, feat_table)
        C.replace_table(example.drop("eol_ts"), ex_table)
        n, vins, bad_label, late, matured, positives, open_gen, floor = spark.sql(f"""
            SELECT count(*), count(DISTINCT vin),
                   sum(CASE WHEN label NOT IN (0, 1) THEN 1 ELSE 0 END),
                   sum(CASE WHEN label = 1 AND (label_date IS NULL
                        OR datediff(label_date, in_service_date) NOT BETWEEN 0 AND {days}) THEN 1 ELSE 0 END),
                   sum(matured), sum(label), sum(genealogy_open), sum(safety_floor)
            FROM {ex_table}""").collect()[0]
        leaked = [c for c in ("label", "matured", "safety_floor") if c in spark.table(feat_table).columns]
        if leaked or bad_label or late or not n or vins != n:
            raise RuntimeError(f"feature checks failed: rows={n} vins={vins} bad_label={bad_label} "
                               f"late={late} leaked={leaked}")
        audit.load("features", FEATURE_SET, "COMPLETED", rows_out=n, message=f"hash salt {salt}")
        print(f"FEATURE_RESULT rows={n} salt={salt} matured={matured} positives={positives} "
              f"genealogy_open={open_gen} safety_floor={floor}", flush=True)
    except Exception as e:
        audit.load("features", FEATURE_SET, "FAILED", message=C.error_summary(e))
        raise
    finally:
        audit.flush()
    return {"rows": n}


def main() -> int:
    spark = C.get_spark("evx-uc01-features")
    run(spark, sys.argv[1:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
