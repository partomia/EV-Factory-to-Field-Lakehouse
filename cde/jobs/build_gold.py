"""
Stage 5 - Gold: the star model of PLAN.md section 8, every table through publish() (cde/jobs/publish.py).

Dimensions (recomputed as of the business date, mode "full"):
  dim_date, dim_supplier, dim_station, dim_cell, dim_rider (PII; classified and masked by SDX)
  dim_material_batch  SCD2 on quality status: RECEIVED (goods receipt) -> RELEASED (inspection lot usage
                      decision) -> SUSPECT (a supplier 8D report names the batch, with its root-cause category)
  dim_pack            SCD2 on fitment: IN_STOCK at build -> FITTED to a VIN -> REMOVED when a job card swaps it out
  dim_vehicle         SCD2 on the fitted pack (EOL, then PACK REPLACEMENT job cards) and the BMS firmware the
                      vehicle reports (telematics)
  SCD2 versions are event-sourced: each event carries the full tracked state, versions with an unchanged state
  or zero length are dropped, valid_to is the next version's valid_from (UTC), is_current marks the open one.

Facts (the business dates processed, mode "dates"):
  fact_cell_process (one row per cell step), fact_cell_grading (one row per graded cell: formation, aging,
  grading and electrode measurements), fact_qc_inspection (verdict rows with the image metrics; a quarantined
  image keeps its verdict row, image_status MISSING), fact_pack_eol, fact_vehicle_eol, fact_vehicle_daily,
  fact_dtc and fact_edr_event (with config/dtc_catalog.json: battery-related, safety floor; EDR arrival lag),
  fact_service_job and fact_warranty_claim (VIN resolved through master.xref)
Bridge: bridge_genealogy (pack -> module -> cell -> cell batch -> material lots, link_status), mode "full".

Usage:
  spark-submit build_gold.py --business-date 2026-11-08
  spark-submit build_gold.py --from 2026-02-09 --to 2026-11-05
"""
from __future__ import annotations

import sys
import time
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import evx_common as C  # noqa: E402
from publish import Spec, publish  # noqa: E402
from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

STAGE = "gold"
MEASURES = {  # step -> params key -> output column
    "mixing": {"viscosity_mpas": "slurry_viscosity_mpas", "solids_pct": "slurry_solids_pct"},
    "coating": {"coat_weight_gsm": "coat_weight_gsm"},
    "calendering": {"thickness_um": "electrode_thickness_um", "density_gcc": "electrode_density_gcc"},
    "electrolyte_fill": {"fill_g": "electrolyte_fill_g"},
    "formation": {"first_charge_ah": "first_charge_ah", "first_discharge_ah": "first_discharge_ah", "ce": "ce",
                  "dcir_mohm": "dcir_mohm", "dqdv_peak_v": "dqdv_peak_v"},
    "aging": {"k_value_mv_day": "k_value_mv_day", "ocv_drop_mv": "ocv_drop_mv"},
    "grading": {"capacity_ah": "capacity_ah", "ir_mohm": "ir_mohm"},
}
SCD2_CHECK = {"valid_range": "valid_to IS NULL OR valid_to > valid_from"}


def sk(*cols):
    return F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in cols]), 256).substr(1, 16)


def scd2(events, key: str, tracked: list[str], order=("valid_from", "_seq")):
    """events: key, valid_from, _seq (tie order), tracked state columns -> versions."""
    w = Window.partitionBy(key).orderBy(*order)
    state = F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("\u0000")) for c in tracked]), 256)
    v = events.withColumn("_state", state) \
        .withColumn("_next_from", F.lead("valid_from").over(w)) \
        .where(F.col("_next_from").isNull() | (F.col("_next_from") > F.col("valid_from"))) \
        .withColumn("_prev_state", F.lag("_state").over(w)) \
        .where(F.col("_prev_state").isNull() | (F.col("_prev_state") != F.col("_state")))
    return v.withColumn("version", F.row_number().over(w)) \
        .withColumn("valid_to", F.lead("valid_from").over(w)) \
        .withColumn("is_current", F.col("valid_to").isNull()) \
        .drop("_state", "_next_from", "_prev_state", "_seq")


def dim_date(start: date, end: date, history_end: str):
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    days = [(start + timedelta(days=i),) for i in range((end - start).days + 1)]
    df = spark.createDataFrame(days, "date date")
    return df.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"), "date",
        F.year("date").alias("year"), F.month("date").alias("month"), F.date_format("date", "yyyy-MM").alias("year_month"),
        F.weekofyear("date").alias("iso_week"),
        F.concat(F.year(F.date_sub(F.next_day("date", "Thu"), 3)), F.lit("-W"),
                 F.lpad(F.weekofyear("date").cast("string"), 2, "0")).alias("build_week"),
        F.date_format("date", "E").alias("day_of_week"),
        (F.col("date") <= F.lit(history_end).cast("date")).alias("is_history"))


def run(spark, argv=None) -> dict:
    p = C.base_parser(__doc__.split("\n\n")[0])
    args = C.parse(p, argv)
    names = C.Names(args.db_prefix)
    C.ensure_databases(spark, names, ("gold", "ref"))
    days = C.date_range(args.date_from, args.date_to)
    dates = [d.isoformat() for d in days]
    as_of = args.date_to
    cfg = C.config(args.overlay)
    catalog = C.load_json("config/dtc_catalog.json")
    audit = C.Audit(spark, names, "build_gold", as_of, args.pipeline_run)
    audit.load(STAGE, "*", "STARTED", message=f"dates {dates[0]}..{dates[-1]}, dimensions as of {as_of}")
    t0, summary = time.time(), {}

    def silver(t, upto=True):
        df = spark.table(names.t("silver", t))
        return df.where(F.col("_business_date") <= F.lit(as_of)) if upto else df.where(F.col("_business_date").isin(days))

    def master(t):
        return spark.table(names.t("master", t))

    def pub(df, spec, sources):
        summary[spec.table] = publish(spark, df, spec, names, audit, dates, sources)

    try:
        # ------------------------------------------------------------ dimensions
        cal = cfg["calendar"]
        pub(dim_date(date(2026, 1, 1), date(2026, 12, 31), cal["history_end"]),
            Spec("dim_date", ["date_key"], "full", transform="generate"), [])
        pub(master("supplier").withColumn("supplier_sk", sk("supplier_id")),
            Spec("dim_supplier", ["supplier_id"], "full", transform="consume"), [names.t("master", "supplier")])

        stations = silver("cell_step").select("station", F.lit("CELL").alias("area"), "line", "step").distinct() \
            .unionByName(silver("qc_inspection").select("station", F.lit("QC").alias("area"), "line",
                                                        F.lower("inspection").alias("step")).distinct()) \
            .unionByName(silver("pack_build").select(F.col("wb_station").alias("station"), F.lit("PACK").alias("area"),
                                                     F.col("pack_line").alias("line"), F.lit("wire_bond").alias("step")).distinct()) \
            .unionByName(silver("vehicle_build").select(F.col("vehicle_line").alias("station"), F.lit("VEHICLE").alias("area"),
                                                        F.col("vehicle_line").alias("line"), F.lit("vehicle_eol").alias("step")).distinct())
        stations = stations.groupBy("station").agg(F.first("area").alias("area"), F.first("line").alias("line"),
                                                   F.first("step").alias("step")) \
            .withColumn("station_sk", sk("station"))
        pub(stations, Spec("dim_station", ["station"], "full", transform="consume"),
            [names.t("silver", t) for t in ("cell_step", "qc_inspection", "pack_build", "vehicle_build")])

        pub(master("cell").withColumn("cell_sk", sk("cell_id")).drop("_as_of_date"),
            Spec("dim_cell", ["cell_id"], "full", transform="consume"), [names.t("master", "cell")])

        rider = silver("rider")
        w = Window.partitionBy("rider_id").orderBy(F.col("_business_date").desc())
        rider = rider.withColumn("_rn", F.row_number().over(w)).where("_rn = 1") \
            .drop("_rn", "_warnings", "_dq_issues", "_source_file", "_source_row", "_record_hash", "_batch_id") \
            .withColumnRenamed("_business_date", "first_seen_date").withColumn("rider_sk", sk("rider_id"))
        pub(rider, Spec("dim_rider", ["rider_id"], "full", transform="consume"), [names.t("silver", "rider")])

        mb = master("material_batch").drop("_as_of_date")
        ev = mb.where(F.col("gr_date").isNotNull()).select(
            "charg", F.col("gr_date").cast("timestamp_ntz").alias("valid_from"), F.lit(1).alias("_seq"),
            F.lit("RECEIVED").alias("quality_status"), F.lit(None).cast("string").alias("root_cause_category"))
        il = silver("sap_inspection_lot").where(F.col("vcode") == "A").groupBy("charg") \
            .agg(F.min("enstehdat").alias("d"))
        ev = ev.unionByName(il.select("charg", F.col("d").cast("timestamp_ntz").alias("valid_from"), F.lit(2).alias("_seq"),
                                      F.lit("RELEASED").alias("quality_status"),
                                      F.lit(None).cast("string").alias("root_cause_category")))
        rep = silver("quality_report").where((F.col("report_type") == "8D") & F.col("sap_batch").isNotNull()) \
            .groupBy(F.col("sap_batch").alias("charg")) \
            .agg(F.min("report_date").alias("d"), F.first("root_cause_category").alias("root_cause_category"))
        ev = ev.unionByName(rep.select("charg", F.col("d").cast("timestamp_ntz").alias("valid_from"), F.lit(3).alias("_seq"),
                                       F.lit("SUSPECT").alias("quality_status"), "root_cause_category"))
        dmb = scd2(ev, "charg", ["quality_status", "root_cause_category"]).join(mb, "charg", "left") \
            .withColumn("material_batch_sk", sk("charg", "version"))
        pub(dmb, Spec("dim_material_batch", ["charg", "version"], "full", SCD2_CHECK, "historise"),
            [names.t("master", "material_batch"), names.t("silver", "sap_inspection_lot"),
             names.t("silver", "quality_report")])

        link = master("vehicle_pack_link")
        pk = master("pack").drop("_as_of_date")
        ev = pk.select("pack_serial", F.col("build_ts").alias("valid_from"), F.lit(1).alias("_seq"),
                       F.lit(None).cast("string").alias("vin"), F.lit("IN_STOCK").alias("fitment_status")) \
            .unionByName(link.select("pack_serial", "valid_from", F.lit(3).alias("_seq"), "vin",
                                     F.lit("FITTED").alias("fitment_status"))) \
            .unionByName(link.where(F.col("valid_to").isNotNull()).select(
                "pack_serial", F.col("valid_to").alias("valid_from"), F.lit(2).alias("_seq"),
                F.lit(None).cast("string").alias("vin"), F.lit("REMOVED").alias("fitment_status")))
        dpk = scd2(ev, "pack_serial", ["vin", "fitment_status"]).join(pk, "pack_serial", "left") \
            .withColumn("pack_sk", sk("pack_serial", "version"))
        pub(dpk, Spec("dim_pack", ["pack_serial", "version"], "full", SCD2_CHECK, "historise"),
            [names.t("master", "pack"), names.t("master", "vehicle_pack_link")])

        veh = master("vehicle").drop("_as_of_date")
        tel = silver("telematics_daily").select("vin", F.col("summary_date").cast("timestamp_ntz").alias("valid_from"),
                                                F.col("fw_bms").alias("bms_fw")).where(F.col("bms_fw").isNotNull())
        wv = Window.partitionBy("vin").orderBy("valid_from")
        fw = tel.withColumn("_prev", F.lag("bms_fw").over(wv)).where(F.col("_prev").isNull() | (F.col("_prev") != F.col("bms_fw"))) \
            .drop("_prev")
        ev = link.select("vin", "valid_from", F.lit(1).alias("_seq"), "pack_serial",
                         F.lit(None).cast("string").alias("bms_fw")) \
            .unionByName(veh.select("vin", F.col("eol_ts").alias("valid_from"), F.lit(0).alias("_seq"),
                                    F.col("eol_pack_serial").alias("pack_serial"), "bms_fw")) \
            .unionByName(fw.select("vin", "valid_from", F.lit(2).alias("_seq"),
                                   F.lit(None).cast("string").alias("pack_serial"), "bms_fw")) \
            .join(veh.select("vin"), "vin", "left_semi")
        wf = Window.partitionBy("vin").orderBy("valid_from", "_seq").rowsBetween(Window.unboundedPreceding, 0)
        ev = ev.withColumn("pack_serial", F.last("pack_serial", True).over(wf)).withColumn("bms_fw", F.last("bms_fw", True).over(wf))
        static = veh.drop("bms_fw", "current_pack_serial", "pack_versions")
        dvh = scd2(ev, "vin", ["pack_serial", "bms_fw"]).join(static, "vin", "left") \
            .withColumn("vehicle_sk", sk("vin", "version"))
        pub(dvh, Spec("dim_vehicle", ["vin", "version"], "full", SCD2_CHECK, "historise"),
            [names.t("master", "vehicle"), names.t("master", "vehicle_pack_link"), names.t("silver", "telematics_daily")])

        pub(master("genealogy").drop("_as_of_date"),
            Spec("bridge_genealogy", ["pack_serial", "cell_id"], "full", transform="consume"), [names.t("master", "genealogy")])

        # ------------------------------------------------------------ facts
        lin = ["_business_date", "_batch_id"]
        cs = silver("cell_step", upto=False)
        pub(cs.select("cell_id", "batch_id", "line", "step", "station", "start_ts", "end_ts", "duration_s", "result",
                      "params", *lin),
            Spec("fact_cell_process", ["cell_id", "step"], transform="consume"), [names.t("silver", "cell_step")])

        graded = cs.where(F.col("step") == "grading").select(
            "cell_id", "batch_id", "line", F.col("station").alias("grading_station"),
            F.col("end_ts").alias("graded_ts"), F.col("result").alias("grading_result"),
            F.col("params")["grade"].alias("grade"), *lin)
        steps = silver("cell_step").join(graded.select("cell_id"), "cell_id", "left_semi")
        aggs = []
        for step, keys in MEASURES.items():
            for k, col in keys.items():
                aggs.append(F.max(F.when(F.col("step") == step, F.col("params")[k])).cast("double").alias(col))
        aggs.append(F.max(F.when(F.col("step") == "formation", F.col("params")["channel"])).alias("formation_channel"))
        aggs.append(F.max(F.when(F.col("step") == "formation", F.col("station"))).alias("formation_station"))
        aggs.append(F.max(F.when(F.col("step") == "mixing", F.col("params")["powder_lot"])).alias("powder_lot"))
        fcg = graded.join(steps.groupBy("cell_id").agg(*aggs), "cell_id", "left")
        pub(fcg, Spec("fact_cell_grading", ["cell_id"], transform="aggregate",
                      checks={"grade_known": "grade IN ('A', 'B', 'REJECT')"}), [names.t("silver", "cell_step")])

        qc = silver("qc_inspection", upto=False)
        img = silver("qc_image_metric").where(F.col("status") == "EXTRACTED").select(
            F.col("subject_id").alias("cell_id"), F.col("image_kind").alias("inspection"),
            F.col("mean_gray").alias("image_mean_gray"), "overhang_px", "overhang_box", "streak_cols", "defect_patch",
            F.col("_business_date").alias("_img_date"))
        wi = Window.partitionBy("cell_id", "inspection").orderBy(F.col("_img_date").desc())
        img = img.withColumn("_rn", F.row_number().over(wi)).where("_rn = 1").drop("_rn", "_img_date")
        fqc = qc.select("cell_id", "batch_id", "line", "inspection", "station", "inspected_ts", "verdict", "defect_code",
                        "score", "bbox", "image_ref", *lin) \
            .join(img, ["cell_id", "inspection"], "left") \
            .withColumn("image_status", F.when(F.col("image_ref").isNull() | (F.col("image_ref") == ""), "NONE")
                        .when(F.col("image_mean_gray").isNull(), "MISSING").otherwise("MEASURED"))
        pub(fqc, Spec("fact_qc_inspection", ["cell_id", "inspection"], transform="enrich"),
            [names.t("silver", "qc_inspection"), names.t("silver", "qc_image_metric")])

        pb = silver("pack_build", upto=False)
        gen_cols = pk.select("pack_serial", "genealogy_status", "cells_ok", "cells_orphan", "cells_pending",
                             "n_cell_batches", "majority_cell_batch", "majority_cells")
        fitted = veh.select(F.col("eol_pack_serial").alias("pack_serial"), F.col("vin").alias("eol_vin"))
        fpe = pb.drop("_warnings", "_dq_issues", "_source_file", "_source_row", "_record_hash") \
            .join(gen_cols, "pack_serial", "left").join(fitted, "pack_serial", "left")
        pub(fpe, Spec("fact_pack_eol", ["pack_serial"], transform="enrich",
                      checks={"cells": f"n_cells = {cfg['pack']['cells_per_pack']}"}),
            [names.t("silver", "pack_build"), names.t("master", "pack"), names.t("master", "vehicle")])

        pub(silver("vehicle_build", upto=False).drop("_warnings", "_dq_issues", "_source_file", "_source_row", "_record_hash"),
            Spec("fact_vehicle_eol", ["vin"], transform="consume"), [names.t("silver", "vehicle_build")])

        pub(silver("telematics_daily", upto=False).drop("_warnings", "_dq_issues", "_source_file", "_source_row", "_record_hash"),
            Spec("fact_vehicle_daily", ["vin", "summary_date"], transform="consume"), [names.t("silver", "telematics_daily")])

        dtc_ref = spark.createDataFrame([(k, v["system"], v["kind"], v["battery_related"], v["safety_floor"])
                                         for k, v in catalog["dtc"].items()],
                                        "dtc_code string, dtc_system string, dtc_kind string, battery_related boolean, "
                                        "safety_floor boolean")
        fdtc = silver("dtc_event", upto=False).drop("_warnings", "_dq_issues", "_source_file", "_source_row", "_record_hash") \
            .join(F.broadcast(dtc_ref), "dtc_code", "left") \
            .withColumn("battery_related", F.coalesce("battery_related", F.lit(False))) \
            .withColumn("safety_floor", F.coalesce("safety_floor", F.lit(False)))
        pub(fdtc, Spec("fact_dtc", ["event_id"], transform="enrich"), [names.t("silver", "dtc_event")])

        edr_ref = spark.createDataFrame([(k, v["battery_related"], v["safety_floor"]) for k, v in catalog["edr_trigger"].items()],
                                        "trigger_kind string, battery_related boolean, safety_floor boolean")
        fedr = silver("edr_event", upto=False).where(F.col("status") == "EXTRACTED") \
            .drop("status", "error", "_extracted_at") \
            .join(F.broadcast(edr_ref), "trigger_kind", "left") \
            .withColumn("event_date", F.to_date("trigger_ts")) \
            .withColumn("arrival_lag_days", F.datediff("_business_date", F.to_date("trigger_ts")))
        pub(fedr, Spec("fact_edr_event", ["vin", "event_id"], transform="enrich",
                       checks={"arrives_after_event": "arrival_lag_days >= 0"}), [names.t("silver", "edr_event")])

        xr = master("xref").where(F.col("entity") == "vehicle")

        def resolved(df, system, col="vin"):
            x = xr.where(F.col("source_system") == system).select(
                F.col("source_key").alias(col), F.col("master_id").alias("resolved_vin"),
                F.col("status").alias("identity_status"))
            return df.join(x, col, "left").withColumn("identity_status", F.coalesce("identity_status", F.lit("UNRESOLVED")))

        sj = silver("service_job", upto=False).drop("_dq_issues")
        pub(resolved(sj, "DMS_JOBCARD"), Spec("fact_service_job", ["job_card_no"], transform="match"),
            [names.t("silver", "service_job"), names.t("master", "xref")])
        wc = silver("warranty_claim", upto=False).drop("_warnings", "_dq_issues", "_source_file", "_source_row", "_record_hash") \
            .withColumn("battery_related", F.col("category").isin(catalog["warranty_battery_categories"]))
        pub(resolved(wc, "WARRANTY"), Spec("fact_warranty_claim", ["claim_id"], transform="match"),
            [names.t("silver", "warranty_claim"), names.t("master", "xref")])

        audit.load(STAGE, "*", "COMPLETED", rows_out=sum(s["rows"] for s in summary.values()),
                   message=f"{len(summary)} tables in {time.time() - t0:.0f}s")
    except Exception as e:
        audit.load(STAGE, "*", "FAILED", message=C.error_summary(e))
        raise
    finally:
        audit.flush()
    print(f"GOLD_RESULT dates={len(dates)} as_of={as_of} seconds={time.time() - t0:.0f}", flush=True)
    for k, v in summary.items():
        print(f"GOLD_TABLE {k} {v['rows']} {v['snapshot']}", flush=True)
    return {k: v["rows"] for k, v in summary.items()}


def main() -> int:
    spark = C.get_spark("evx-build-gold")
    run(spark, sys.argv[1:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
