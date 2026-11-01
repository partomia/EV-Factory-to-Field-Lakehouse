"""
Stage 4 - Master: golden supplier, material batch, cell, pack and vehicle records, the cross-reference from every
source key, genealogy and the vehicle-pack link history. Recomputed in full from silver as of the business date
(rows with _business_date <= --to), so a re-run gives the same answer; the link history is event-sourced, so the
SCD2 versions come out of the same recompute.

  supplier          SAP A_Supplier (LIFNR), else the CoA's supplier; name from SAP, else the CoA
  material_batch    SAP A_Batch (CHARG) with goods receipt (BWART 101), inspection lot (QM values, ZZ_MOISTURE_PPM),
                    CoA and XRF; vendor_batch_conflict when SAP LICHA and the CoA's vendor batch differ
  cell              one row per cell from the MES steps: batch, line, material lots (powder, electrolyte, can), grade
  genealogy         pack -> module -> cell -> cell batch -> material lots; link_status
                      OK       the cell exists
                      ORPHAN   the cell's batch is known but the cell is not (planted: 2026-10-20, 2026-11-07)
                      PENDING  the cell's batch has not arrived (only at the edge of a partial load)
  pack              pack build with genealogy counts, majority cell batch, genealogy_status
                    (COMPLETE / ORPHAN_CELL / INCOMPLETE)
  vehicle           vehicle EOL with the DMS rider link; identity_status MATCHED / REVIEW / NO_DMS
  vehicle_pack_link one version per pack fitted: VEHICLE_EOL, then a PACK REPLACEMENT job card (valid_from,
                    valid_to, is_current); removed_pack_ok checks the card against the pack it replaced
  xref              (entity, source_system, source_key) -> master_id, rule, score, status MATCHED / REVIEW /
                    UNRESOLVED
  ref.identity_review  open items: a DMS VIN that is not an EOL VIN but one adjacent transposition or one
                    substitution away from an EOL VIN with no rider, built 0-30 days before the delivery
                    (VIN_TRANSPOSITION / VIN_EDIT, decision REVIEW, never merged automatically); an orphan cell
                    (NO_SUCH_CELL); a source key that resolves to nothing (NO_MATCH)

Usage:
  spark-submit build_master.py --business-date 2026-11-08
  spark-submit build_master.py --from 2026-02-09 --to 2026-11-05     (as of 2026-11-05)
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import evx_common as C  # noqa: E402
from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

STAGE = "master"
CELL_BATCH = r"^(CBCL[12][0-9]{6}[ABC])"
REVIEW_COLS = ["review_id", "entity", "source_system", "source_key", "candidate_id", "rule", "score", "context",
               "first_seen_date", "status"]


def latest(df, key, order=("_business_date",)):
    w = Window.partitionBy(*key).orderBy(*[F.col(c).desc_nulls_last() for c in order])
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def resolve(keys, entity, system, masters, rule="EXACT"):
    """Source keys (column `k`, with first_seen) against master ids (column `id`): MATCHED or UNRESOLVED."""
    j = keys.join(masters.select(F.col("id").alias("k"), F.lit(True).alias("_hit")), "k", "left")
    x = j.select(F.lit(entity).alias("entity"), F.lit(system).alias("source_system"), F.col("k").alias("source_key"),
                 F.when(F.col("_hit"), F.col("k")).alias("master_id"),
                 F.when(F.col("_hit"), F.lit(rule)).otherwise(F.lit("NO_MATCH")).alias("rule"),
                 F.when(F.col("_hit"), F.lit(1.0)).otherwise(F.lit(0.0)).alias("score"),
                 F.when(F.col("_hit"), F.lit("MATCHED")).otherwise(F.lit("UNRESOLVED")).alias("status"),
                 "first_seen")
    return x


def supplier_table(silver):
    sap = latest(silver("sap_supplier"), ["lifnr"]).select(
        F.col("lifnr").alias("supplier_id"), F.col("name1").alias("sap_name"), F.col("land1").alias("country"),
        F.col("ort01").alias("city"))
    coa = latest(silver("supplier_coa").where(F.col("supplier").isNotNull()), ["supplier"]).select(
        F.col("supplier").alias("supplier_id"), F.col("supplier_name").alias("coa_name"))
    ids = sap.select("supplier_id").union(coa.select("supplier_id")) \
        .union(silver("sap_batch").select(F.col("lifnr").alias("supplier_id"))).where("supplier_id IS NOT NULL").distinct()
    return ids.join(sap, "supplier_id", "left").join(coa, "supplier_id", "left").select(
        "supplier_id", F.coalesce("sap_name", "coa_name").alias("supplier_name"),
        F.when(F.col("sap_name").isNotNull(), "SAP").when(F.col("coa_name").isNotNull(), "COA").alias("name_source"),
        "country", "city", F.col("sap_name").isNotNull().alias("in_sap"), F.col("coa_name").isNotNull().alias("has_coa"))


def material_batch_table(silver):
    b = latest(silver("sap_batch"), ["charg"]).select(
        "charg", "matnr", "lifnr", "werks", F.col("hsdat").alias("mfg_date"), F.col("vfdat").alias("expiry_date"),
        F.col("licha").alias("vendor_batch"), F.col("_business_date").alias("first_seen_date"))
    gr = latest(silver("sap_material_document").where(F.col("bwart") == "101"), ["charg"]).select(
        "charg", F.col("budat").alias("gr_date"), F.col("menge").alias("gr_qty"), F.col("meins").alias("gr_uom"))
    il = latest(silver("sap_inspection_lot"), ["charg"]).select(
        "charg", "prueflos", F.col("qm_d50_um"), F.col("qm_impurity_fe_ppm"), "zz_moisture_ppm",
        F.col("vcode").alias("usage_decision"))
    coa = latest(silver("supplier_coa").where(F.col("sap_batch").isNotNull()), ["sap_batch"]).select(
        F.col("sap_batch").alias("charg"), "certificate_no", F.col("vendor_batch").alias("coa_vendor_batch"),
        F.col("fe_impurity_ppm").alias("coa_fe_ppm"), F.col("d50_um").alias("coa_d50_um"),
        F.col("moisture_ppm").alias("coa_moisture_ppm"), F.col("result").alias("coa_result"))
    xrf = latest(silver("spectrum_feature").where(F.col("status") == "EXTRACTED"), ["lot_id"]).select(
        F.col("lot_id").alias("charg"), F.col("fe_ka").alias("xrf_fe_ka"), F.col("ni_ka").alias("xrf_ni_ka"),
        F.col("fe_ni_ratio").alias("xrf_fe_ni_ratio"))
    mb = b.join(gr, "charg", "left").join(il, "charg", "left").join(coa, "charg", "left").join(xrf, "charg", "left")
    return mb.withColumn("vendor_batch_conflict", F.col("coa_vendor_batch").isNotNull()
                         & (F.col("coa_vendor_batch") != F.upper("vendor_batch"))) \
        .withColumn("has_coa", F.col("certificate_no").isNotNull()) \
        .withColumn("has_xrf", F.col("xrf_fe_ka").isNotNull())


def cell_table(silver):
    s = silver("cell_step")

    def at(step, expr):
        return F.max(F.when(F.col("step") == step, expr))

    return s.groupBy("cell_id").agg(
        F.max("batch_id").alias("batch_id"), F.max("line").alias("line"),
        at("mixing", F.col("params")["powder_lot"]).alias("powder_lot"),
        at("electrolyte_fill", F.col("params")["electrolyte_lot"]).alias("electrolyte_lot"),
        at("canning", F.col("params")["can_lot"]).alias("can_lot"),
        at("grading", F.col("params")["grade"]).alias("grade"),
        at("grading", F.col("result")).alias("grading_result"),
        F.min("start_ts").alias("first_step_ts"), F.max("end_ts").alias("last_step_ts"),
        F.countDistinct("step").alias("n_steps"), F.min("_business_date").alias("first_seen_date"))


def genealogy_table(silver, cell):
    pc = latest(silver("pack_cell"), ["pack_serial", "cell_id"])
    batches = cell.select(F.col("batch_id").alias("_cb")).distinct().withColumn("_batch_known", F.lit(True))
    g = pc.withColumn("_cb", F.regexp_extract("cell_id", CELL_BATCH, 1)) \
        .join(cell.select("cell_id", "batch_id", "powder_lot", "electrolyte_lot", "can_lot"), "cell_id", "left") \
        .join(batches, "_cb", "left")
    return g.select(
        "pack_serial", "module", "position", "cell_id", F.coalesce("batch_id", F.when(F.col("_cb") != "", F.col("_cb")))
        .alias("cell_batch"), "powder_lot", "electrolyte_lot", "can_lot",
        F.when(F.col("batch_id").isNotNull(), "OK").when(F.col("_batch_known"), "ORPHAN").otherwise("PENDING")
        .alias("link_status"), F.col("_business_date").alias("first_seen_date"))


def pack_table(silver, genealogy):
    pb = latest(silver("pack_build"), ["pack_serial"])
    stats = genealogy.groupBy("pack_serial").agg(
        F.count(F.lit(1)).alias("genealogy_cells"),
        F.sum(F.when(F.col("link_status") == "OK", 1).otherwise(0)).alias("cells_ok"),
        F.sum(F.when(F.col("link_status") == "ORPHAN", 1).otherwise(0)).alias("cells_orphan"),
        F.sum(F.when(F.col("link_status") == "PENDING", 1).otherwise(0)).alias("cells_pending"),
        F.countDistinct(F.when(F.col("link_status") == "OK", F.col("cell_batch"))).alias("n_cell_batches"))
    w = Window.partitionBy("pack_serial").orderBy(F.col("n").desc(), F.col("cell_batch"))
    major = genealogy.where(F.col("link_status") == "OK").groupBy("pack_serial", "cell_batch").agg(F.count(F.lit(1)).alias("n")) \
        .withColumn("_rn", F.row_number().over(w)).where("_rn = 1") \
        .select("pack_serial", F.col("cell_batch").alias("majority_cell_batch"), F.col("n").alias("majority_cells"))
    return pb.select("pack_serial", "pack_line", "build_ts", "shift", "config", "n_cells", "n_modules", "wb_station",
                     "wb_pull_min_g", "eol_test_ts", "eol_result", "bms_fw", F.col("_business_date").alias("first_seen_date")) \
        .join(stats, "pack_serial", "left").join(major, "pack_serial", "left") \
        .withColumn("genealogy_status", F.when(F.col("cells_orphan") > 0, "ORPHAN_CELL")
                    .when((F.col("cells_pending") > 0) | (F.col("cells_ok") < F.col("n_cells")), "INCOMPLETE")
                    .otherwise("COMPLETE"))


def vin_candidates(rider, ve):
    """DMS VINs that are not EOL VINs, against EOL VINs without a rider: same 14-character prefix, one adjacent
    transposition or one substitution, and the vehicle left EOL 0-30 days before the DMS delivery date."""
    eol = ve.select(F.col("vin").alias("v"), F.col("eol_ts").alias("_eol_ts"))
    unknown = rider.join(eol, rider.vin == eol.v, "left_anti")
    free = eol.join(rider.select("vin"), eol.v == rider.vin, "left_anti")
    u, v = F.col("vin"), F.col("v")
    transposed = (F.substring(u, 15, 1) == F.substring(v, 16, 1)) & (F.substring(u, 16, 1) == F.substring(v, 15, 1)) \
        & (F.substring(u, 17, 1) == F.substring(v, 17, 1)) & (u != v)
    lag = F.datediff("delivery_date", F.to_date("_eol_ts"))
    return unknown.join(free, F.substring(u, 1, 14) == F.substring(v, 1, 14)) \
        .where((transposed | (F.levenshtein(u, v) == 1)) & lag.between(0, 30)) \
        .select(F.col("vin").alias("dms_vin"), F.col("v").alias("candidate_vin"), "rider_id",
                F.when(transposed, "VIN_TRANSPOSITION").otherwise("VIN_EDIT").alias("rule"),
                F.when(transposed, 0.95).otherwise(0.85).alias("score"), F.col("_business_date").alias("first_seen"))


def vehicle_tables(silver):
    ve = latest(silver("vehicle_build"), ["vin"])
    rider = latest(silver("rider").where(F.col("vin").isNotNull()), ["rider_id"])
    cand = vin_candidates(rider, ve)
    exact = rider.join(ve.select("vin"), "vin", "inner").select(
        "vin", "rider_id", F.col("registration_no").alias("registration"), "delivery_date", "dealer_code", "city",
        "geohash5")
    exact = latest(exact.withColumn("_business_date", F.col("delivery_date")), ["vin"]).drop("_business_date")
    review = cand.select(F.col("candidate_vin").alias("vin"), F.lit(True).alias("_review"),
                         F.col("dms_vin").alias("review_dms_vin")).dropDuplicates(["vin"])

    jobs = silver("service_job")
    swaps = jobs.where((F.col("service_type") == "PACK REPLACEMENT") & F.col("installed_pack_serial").isNotNull()) \
        .join(ve.select("vin"), "vin", "inner") \
        .select("vin", F.col("installed_pack_serial").alias("pack_serial"),
                F.col("service_date").cast("timestamp_ntz").alias("valid_from"), F.lit("SERVICE_JOB").alias("source"),
                F.col("job_card_no").alias("source_ref"), F.col("removed_pack_serial"))
    eol = ve.select("vin", "pack_serial", F.col("eol_ts").alias("valid_from"), F.lit("VEHICLE_EOL").alias("source"),
                    F.lit(None).cast("string").alias("source_ref"), F.lit(None).cast("string").alias("removed_pack_serial"))
    w = Window.partitionBy("vin").orderBy("valid_from", "source")
    link = eol.unionByName(swaps) \
        .withColumn("version", F.row_number().over(w)) \
        .withColumn("valid_to", F.lead("valid_from").over(w)) \
        .withColumn("_prev_pack", F.lag("pack_serial").over(w)) \
        .withColumn("is_current", F.col("valid_to").isNull()) \
        .withColumn("removed_pack_ok", F.when(F.col("source") == "SERVICE_JOB",
                                              F.col("removed_pack_serial") == F.col("_prev_pack"))) \
        .drop("_prev_pack")
    current = link.where("is_current").select("vin", F.col("pack_serial").alias("current_pack_serial"),
                                               F.col("version").alias("pack_versions"))
    vehicle = ve.select("vin", "vehicle_line", "model", "eol_ts", F.col("pack_serial").alias("eol_pack_serial"),
                        "motor_serial", "mcu_serial", "vcu_fw", "bms_fw", "eol_result",
                        F.col("_business_date").alias("first_seen_date")) \
        .join(current, "vin", "left").join(exact, "vin", "left").join(review, "vin", "left") \
        .withColumn("identity_status", F.when(F.col("rider_id").isNotNull(), "MATCHED")
                    .when(F.col("_review"), "REVIEW").otherwise("NO_DMS")).drop("_review")
    return vehicle, link, cand, rider, ve


def run(spark, argv=None) -> dict:
    p = C.base_parser(__doc__.split("\n\n")[0])
    args = C.parse(p, argv)
    names = C.Names(args.db_prefix)
    C.ensure_databases(spark, names, ("master", "ref"))
    as_of = args.date_to
    audit = C.Audit(spark, names, "build_master", as_of, args.pipeline_run)
    audit.load(STAGE, "*", "STARTED", message=f"as of {as_of}")
    t0, summary = time.time(), {}

    def silver(table):
        return spark.table(names.t("silver", table)).where(F.col("_business_date") <= F.lit(as_of))

    def write(table, df, sources, layer="master", note_fn=None):
        started = C.now_utc()
        target = names.t(layer, table)
        before = C.snapshot_id(spark, target)
        C.replace_table(df.withColumn("_as_of_date", F.lit(as_of)), target)
        out = spark.table(target)
        n = out.count()
        note = note_fn(out) if note_fn else ""
        msg = f"{n} rows as of {as_of}{note}"
        audit.load(STAGE, table, "COMMITTED", rows_in=None, rows_out=n, snapshot_before=before,
                   snapshot_after=C.snapshot_id(spark, target), started_at=started, message=msg)
        audit.transform(f"master {table}", "survive" if layer == "master" else "match",
                        [names.t("silver", s) for s in sources], target, None, n, msg)
        summary[table] = {"rows": n, "note": note}
        print(f"  {table:<20} {msg}", flush=True)

    def counts(col):
        return lambda out: "; " + col + " " + str({r[0]: r[1] for r in out.groupBy(col).count().collect()})

    try:
        write("supplier", supplier_table(silver), ["sap_supplier", "supplier_coa", "sap_batch"], note_fn=counts("name_source"))
        write("material_batch", material_batch_table(silver),
              ["sap_batch", "sap_material_document", "sap_inspection_lot", "supplier_coa", "spectrum_feature"],
              note_fn=lambda o: f"; vendor batch conflicts {o.where('vendor_batch_conflict').count()}")
        write("cell", cell_table(silver), ["cell_step"], note_fn=counts("grade"))
        cell = spark.table(names.t("master", "cell"))
        write("genealogy", genealogy_table(silver, cell), ["pack_cell", "cell_step"], note_fn=counts("link_status"))
        gen = spark.table(names.t("master", "genealogy"))
        write("pack", pack_table(silver, gen), ["pack_build", "pack_cell"], note_fn=counts("genealogy_status"))
        vehicle, link, cand, rider, ve = vehicle_tables(silver)
        write("vehicle", vehicle, ["vehicle_build", "rider", "service_job"], note_fn=counts("identity_status"))
        write("vehicle_pack_link", link, ["vehicle_build", "service_job"], note_fn=counts("source"))

        m = lambda t, c: spark.table(names.t("master", t)).select(F.col(c).alias("id"))  # noqa: E731

        def keys(table, col, where=None):
            df = silver(table)
            df = df.where(where) if where is not None else df
            return df.where(F.col(col).isNotNull()).groupBy(F.col(col).alias("k")) \
                .agg(F.min("_business_date").alias("first_seen"))

        dms = keys("rider", "vin")
        dms_exact = dms.join(m("vehicle", "vin").withColumnRenamed("id", "k"), "k", "inner")
        cand_x = cand.select(F.lit("vehicle").alias("entity"), F.lit("DMS").alias("source_system"),
                             F.col("dms_vin").alias("source_key"), F.col("candidate_vin").alias("master_id"), "rule",
                             F.col("score").cast("double").alias("score"), F.lit("REVIEW").alias("status"), "first_seen")
        dms_rest = resolve(dms.join(dms_exact.select("k"), "k", "left_anti")
                           .join(cand.select(F.col("dms_vin").alias("k")), "k", "left_anti"), "vehicle", "DMS",
                           m("vehicle", "vin"))
        parts = [
            resolve(keys("vehicle_build", "vin"), "vehicle", "VEHICLE_EOL", m("vehicle", "vin"), "SOURCE_KEY"),
            resolve(dms_exact.select("k", "first_seen"), "vehicle", "DMS", m("vehicle", "vin"), "VIN_EXACT"),
            cand_x, dms_rest,
            resolve(keys("telematics_daily", "vin"), "vehicle", "TELEMATICS", m("vehicle", "vin")),
            resolve(keys("dtc_event", "vin"), "vehicle", "DTC", m("vehicle", "vin")),
            resolve(keys("warranty_claim", "vin"), "vehicle", "WARRANTY", m("vehicle", "vin")),
            resolve(keys("service_job", "vin"), "vehicle", "DMS_JOBCARD", m("vehicle", "vin")),
            resolve(keys("pack_build", "pack_serial"), "pack", "PACK_MES", m("pack", "pack_serial"), "SOURCE_KEY"),
            resolve(keys("vehicle_build", "pack_serial"), "pack", "VEHICLE_EOL", m("pack", "pack_serial")),
            resolve(keys("service_job", "installed_pack_serial"), "pack", "DMS_JOBCARD", m("pack", "pack_serial")),
            resolve(keys("sap_batch", "charg"), "material_batch", "SAP", m("material_batch", "charg"), "SOURCE_KEY"),
            resolve(keys("supplier_coa", "sap_batch"), "material_batch", "COA", m("material_batch", "charg")),
            resolve(keys("spectrum_feature", "lot_id"), "material_batch", "XRF", m("material_batch", "charg")),
            resolve(keys("sap_material_document", "charg", F.col("bwart") == "101"), "material_batch", "SAP_GR",
                    m("material_batch", "charg")),
            resolve(keys("sap_batch", "lifnr"), "supplier", "SAP", m("supplier", "supplier_id")),
            resolve(keys("supplier_coa", "supplier"), "supplier", "COA", m("supplier", "supplier_id")),
        ]
        xref = parts[0]
        for x in parts[1:]:
            xref = xref.unionByName(x)
        regs = silver("rider").where(F.col("registration_no").isNotNull()) \
            .join(m("vehicle", "vin").withColumnRenamed("id", "vin"), "vin", "inner") \
            .groupBy(F.col("registration_no").alias("source_key"), "vin").agg(F.min("_business_date").alias("first_seen"))
        xref = xref.unionByName(regs.select(F.lit("vehicle").alias("entity"), F.lit("DMS_REGISTRATION").alias("source_system"),
                                            "source_key", F.col("vin").alias("master_id"), F.lit("RIDER_LINK").alias("rule"),
                                            F.lit(1.0).alias("score"), F.lit("MATCHED").alias("status"), "first_seen"))
        write("xref", xref, ["rider", "vehicle_build", "telematics_daily", "dtc_event", "warranty_claim", "service_job",
                             "pack_build", "sap_batch", "supplier_coa", "spectrum_feature", "sap_material_document"],
              note_fn=lambda o: "; " + str({f"{r[0]}/{r[1]}": r[2] for r in
                                             o.groupBy("entity", "status").count().orderBy("entity", "status").collect()}))

        xr = spark.table(names.t("master", "xref"))
        orphan = spark.table(names.t("master", "genealogy")).where(F.col("link_status") == "ORPHAN").select(
            F.lit("cell").alias("entity"), F.lit("PACK_MES").alias("source_system"), F.col("cell_id").alias("source_key"),
            F.lit(None).cast("string").alias("candidate_id"), F.lit("NO_SUCH_CELL").alias("rule"),
            F.lit(None).cast("double").alias("score"), F.concat(F.lit("pack "), F.col("pack_serial")).alias("context"),
            F.col("first_seen_date"))
        items = xr.where(F.col("status").isin("REVIEW", "UNRESOLVED")).select(
            "entity", "source_system", "source_key", F.col("master_id").alias("candidate_id"), "rule", "score",
            F.lit(None).cast("string").alias("context"), F.col("first_seen").alias("first_seen_date")).unionByName(orphan)
        review = items.withColumn("review_id", F.sha2(F.concat_ws("|", "entity", "source_system", "source_key",
                                                                F.coalesce("candidate_id", F.lit(""))), 256).substr(1, 16)) \
            .withColumn("status", F.lit("OPEN")).select(*REVIEW_COLS)
        write("identity_review", review, ["rider", "pack_cell"], layer="ref",
              note_fn=lambda o: "; " + str({r[0]: r[1] for r in o.groupBy("rule").count().collect()}))
        audit.load(STAGE, "*", "COMPLETED", rows_out=sum(s["rows"] for s in summary.values()),
                   message=f"as of {as_of} in {time.time() - t0:.0f}s")
    except Exception as e:
        audit.load(STAGE, "*", "FAILED", message=C.error_summary(e))
        raise
    finally:
        audit.flush()
    print(f"MASTER_RESULT as_of={as_of} seconds={time.time() - t0:.0f}", flush=True)
    for k, v in summary.items():
        print(f"MASTER_TABLE {k} {v['rows']}{v['note']}", flush=True)
    return summary


def main() -> int:
    spark = C.get_spark("evx-build-master")
    run(spark, sys.argv[1:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
