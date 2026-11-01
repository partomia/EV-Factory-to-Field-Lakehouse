"""
Stage 3 - Silver: typed, UTC, conformed rows per business date from bronze and the extracted objects.

  typed      every contract column (contracts/sources) as its type; timestamps are parsed with their offset and
             stored as timestamp_ntz in UTC (the session time zone); a value that fails its cast becomes NULL and
             is named in _dq_issues
  dedup      one row per contract key per business date (the last file and row win); dropped rows are counted
  MES        silver.cell_step with params as a map
  riders     silver.rider keeps geohash-5 only; the precise home location stays in bronze
  documents  pack build -> silver.pack_build and silver.pack_cell (cell -> module -> pack); vehicle EOL ->
             silver.vehicle_build
  SAP        one table per OData entity set (silver.sap_batch, ...): /Date(ms)/ -> date, quantities -> decimal;
             fields outside the contract are kept in _extra_fields, and typed when silver knows them
             (ZZ_MOISTURE_PPM on inspection lots, from 2026-11-08)
  extracted  silver.doc_extract by document title -> silver.service_job, silver.supplier_coa,
             silver.quality_report (free text upper-cased, as OCR reads it, so text-layer and scanned cards agree)

Each table gets one commit replacing the business dates processed. ref.load_audit: STARTED, COMMITTED per
table, COMPLETED or FAILED.

Usage:
  spark-submit build_silver.py --business-date 2026-11-08
  spark-submit build_silver.py --from 2026-02-09 --to 2026-11-05
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import evx_common as C  # noqa: E402
from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

STAGE = "silver"
TS_FMT = "yyyy-MM-dd'T'HH:mm:ssXXX"
LINEAGE = ["_business_date", "_batch_id", "_source_file", "_source_row", "_record_hash"]

TYPED = {  # bronze entity -> silver table (contract columns typed)
    "cell_mes_step": "cell_step",
    "cell_qc_inspection": "qc_inspection",
    "formation_curve_point": "formation_curve_point",
    "telematics_daily": "telematics_daily",
    "dtc_history": "dtc_event",
    "warranty_claims": "warranty_claim",
    "dms_riders": "rider",
}
DROP = {"rider": ["home_lat", "home_lon"]}

SAP_TABLE = {"A_Material": "sap_material", "A_Supplier": "sap_supplier", "A_Batch": "sap_batch",
             "A_InspectionLot": "sap_inspection_lot", "A_ProductionOrder": "sap_production_order",
             "A_MaterialDocument": "sap_material_document"}
SAP_TYPES = {"HSDAT": "odata_date", "VFDAT": "odata_date", "ENSTEHDAT": "odata_date", "BUDAT": "odata_date",
             "GSTRP": "odata_date", "MENGE": "decimal(18,3)", "GAMNG": "decimal(18,3)", "MJAHR": "int",
             "QM_D50_UM": "double", "QM_IMPURITY_FE_PPM": "double", "ZZ_MOISTURE_PPM": "double"}
SAP_EXTRA = {"A_InspectionLot": ["ZZ_MOISTURE_PPM"]}

PACK_DOC = ("pack_serial string, pack_line string, build_ts string, shift string, config string, "
            "cells array<struct<cell_id:string,module:string,position:int>>, "
            "wire_bond struct<station:string,bonds:int,pull_min_g:double,pull_mean_g:double,pull_p05_g:double>, "
            "eol struct<test_ts:string,capacity_ah:double,insulation_mohm:double,cell_v_spread_mv:double,"
            "bms_fw:string,result:string>")
VEHICLE_DOC = ("vin string, vehicle_line string, model string, eol_ts string, pack_serial string, "
               "motor_serial string, mcu_serial string, vcu_fw string, bms_fw string, "
               "tests struct<functional:string,brake_n:double,insulation_mohm:double,charge_handshake:string>, "
               "result string")

DOC_TABLE = {"SERVICE JOB CARD": "service_job", "CERTIFICATE OF ANALYSIS": "supplier_coa",
             "8D PROBLEM SOLVING REPORT": "quality_report", "FIELD RETURN TEARDOWN REPORT": "quality_report"}
DOC_META = ["doc_id", "file_name", "method", "_business_date", "_batch_id"]


def ts(col):
    return F.to_timestamp(col, TS_FMT).cast("timestamp_ntz")


def blank_null(col):
    v = F.trim(col)
    return F.when(v != "", v)


def dedup(df, key):
    w = Window.partitionBy(*key, "_business_date").orderBy(F.col("_source_file").desc(), F.col("_source_row").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def dq_array(checks):
    checks = [c for c in checks if c is not None]
    if not checks:
        return F.array().cast("array<string>")
    return F.filter(F.array(*checks), lambda x: x.isNotNull())


def typed_entity(bronze, contract: dict):
    cols, checks = [], []
    for c in contract["columns"]:
        raw = F.col(c["name"])
        t = C.typed(raw, c)
        if c["type"] == "timestamp":
            t = t.cast("timestamp_ntz")
        cols.append(t.alias(c["name"]))
        if c["type"] != "string":
            checks.append(F.when((F.trim(raw) != "") & t.isNull(), F.lit(f"cast:{c['name']}")))
    return dedup(bronze.select(*cols, "_warnings", dq_array(checks).alias("_dq_issues"), *LINEAGE), contract["key"])


def sap_value(m, field: str):
    v = blank_null(m[field])
    kind = SAP_TYPES.get(field, "string")
    if kind == "odata_date":
        ms = F.regexp_extract(v, r"/Date\((-?\d+)", 1)
        return F.to_date(F.from_unixtime((ms.cast("bigint") / 1000).cast("bigint")))
    return v if kind == "string" else v.cast(kind)


def sap_entity(bronze, entity_set: str, contract: dict):
    known = contract["known_fields"][entity_set]
    fields = known + SAP_EXTRA.get(entity_set, [])
    m = F.from_json("payload", "map<string,string>")
    rows = bronze.where(F.col("entity_set") == entity_set).withColumn("_m", m)
    cols, checks = [], []
    for f in fields:
        v = sap_value(F.col("_m"), f)
        cols.append(v.alias(f.lower()))
        if SAP_TYPES.get(f, "string") != "string":
            checks.append(F.when(blank_null(F.col("_m")[f]).isNotNull() & v.isNull(), F.lit(f"cast:{f}")))
    extra = F.map_filter(F.col("_m"), lambda k, _v: ~k.isin(known))
    df = rows.select("record_key", "delta_token", *cols, extra.alias("_extra_fields"), "_warnings",
                     dq_array(checks).alias("_dq_issues"), *LINEAGE)
    return dedup(df, ["record_key"])


def pack_tables(bronze):
    d = bronze.where(F.col("payload").isNotNull()).withColumn("d", F.from_json("payload", PACK_DOC))
    build = d.select(
        F.col("d.pack_serial").alias("pack_serial"), F.col("d.pack_line").alias("pack_line"),
        ts(F.col("d.build_ts")).alias("build_ts"), F.col("d.shift").alias("shift"), F.col("d.config").alias("config"),
        F.size("d.cells").alias("n_cells"), F.size(F.array_distinct("d.cells.module")).alias("n_modules"),
        F.col("d.wire_bond.station").alias("wb_station"), F.col("d.wire_bond.bonds").alias("wb_bonds"),
        F.col("d.wire_bond.pull_min_g").alias("wb_pull_min_g"), F.col("d.wire_bond.pull_mean_g").alias("wb_pull_mean_g"),
        F.col("d.wire_bond.pull_p05_g").alias("wb_pull_p05_g"), ts(F.col("d.eol.test_ts")).alias("eol_test_ts"),
        F.col("d.eol.capacity_ah").alias("eol_capacity_ah"), F.col("d.eol.insulation_mohm").alias("eol_insulation_mohm"),
        F.col("d.eol.cell_v_spread_mv").alias("eol_cell_v_spread_mv"), F.col("d.eol.bms_fw").alias("bms_fw"),
        F.col("d.eol.result").alias("eol_result"),
        dq_array([F.when(F.col("d").isNull(), F.lit("parse:payload")),
                  F.when(F.col("d.build_ts").isNotNull() & ts(F.col("d.build_ts")).isNull(), F.lit("cast:build_ts"))])
        .alias("_dq_issues"), *LINEAGE)
    cells = d.select(F.col("d.pack_serial").alias("pack_serial"), ts(F.col("d.build_ts")).alias("build_ts"),
                     F.posexplode("d.cells").alias("_i", "c"), *LINEAGE) \
        .select("pack_serial", F.col("c.cell_id").alias("cell_id"), F.col("c.module").alias("module"),
                F.col("c.position").alias("position"), "build_ts", *LINEAGE)
    return dedup(build, ["pack_serial"]), dedup(cells, ["pack_serial", "cell_id"])


def vehicle_table(bronze):
    d = bronze.where(F.col("payload").isNotNull()).withColumn("d", F.from_json("payload", VEHICLE_DOC))
    df = d.select(
        *[F.col(f"d.{c}").alias(c) for c in ("vin", "vehicle_line", "model")], ts(F.col("d.eol_ts")).alias("eol_ts"),
        *[F.col(f"d.{c}").alias(c) for c in ("pack_serial", "motor_serial", "mcu_serial", "vcu_fw", "bms_fw")],
        F.col("d.tests.functional").alias("test_functional"), F.col("d.tests.brake_n").alias("test_brake_n"),
        F.col("d.tests.insulation_mohm").alias("test_insulation_mohm"),
        F.col("d.tests.charge_handshake").alias("test_charge_handshake"), F.col("d.result").alias("eol_result"),
        dq_array([F.when(F.col("d").isNull(), F.lit("parse:payload")),
                  F.when(F.col("d.eol_ts").isNotNull() & ts(F.col("d.eol_ts")).isNull(), F.lit("cast:eol_ts"))])
        .alias("_dq_issues"), *LINEAGE)
    return dedup(df, ["vin"])


def doc_tables(docs):
    d = docs.where(F.col("status") == "EXTRACTED") \
        .withColumn("f", F.from_json("fields", "map<string,string>")) \
        .withColumn("s", F.from_json("sections", "map<string,string>")) \
        .withColumn("_table", F.coalesce(*[F.when(F.col("doc_title") == t, F.lit(v)) for t, v in DOC_TABLE.items()]))

    def fld(name, kind="string", src="f"):
        v = blank_null(F.col(src)[name])
        if kind == "upper":
            return F.upper(v)
        if kind == "date":
            return F.to_date(v)
        return v if kind == "string" else v.cast(kind)

    job = d.where(F.col("_table") == "service_job").select(
        fld("job_card_no").alias("job_card_no"), fld("date", "date").alias("service_date"),
        fld("vin", "upper").alias("vin"), fld("registration", "upper").alias("registration"),
        fld("odometer_km", "double").alias("odometer_km"), fld("service_type", "upper").alias("service_type"),
        fld("service_centre", "upper").alias("service_centre"), fld("technician", "upper").alias("technician"),
        fld("parts_replaced", "upper").alias("parts_replaced"), fld("complaint", "upper", "s").alias("complaint"),
        fld("diagnosis", "upper", "s").alias("diagnosis"), *DOC_META)
    job = job.withColumn("removed_pack_serial", blank_null(F.regexp_extract("diagnosis", r"REMOVED PACK (PK[A-Z0-9]+)", 1))) \
        .withColumn("installed_pack_serial", blank_null(F.regexp_extract("diagnosis", r"INSTALLED PACK (PK[A-Z0-9]+)", 1)))
    coa = d.where(F.col("_table") == "supplier_coa").select(
        fld("certificate_no", "upper").alias("certificate_no"), fld("supplier", "upper").alias("supplier"),
        fld("supplier_name").alias("supplier_name"), fld("material", "upper").alias("material"),
        fld("vendor_batch", "upper").alias("vendor_batch"), fld("sap_batch", "upper").alias("sap_batch"),
        fld("date", "date").alias("coa_date"), fld("d50_um", "double").alias("d50_um"),
        fld("fe_impurity_ppm", "double").alias("fe_impurity_ppm"), fld("moisture_ppm", "double").alias("moisture_ppm"),
        fld("specification").alias("specification"), fld("result", "upper").alias("result"), *DOC_META)
    report = d.where(F.col("_table") == "quality_report").select(
        fld("report_no", "upper").alias("report_no"), fld("report_date", "date").alias("report_date"),
        fld("report_type", "upper").alias("report_type"), F.col("doc_title"),
        fld("supplier", "upper").alias("supplier"), fld("sap_batch", "upper").alias("sap_batch"),
        fld("material", "upper").alias("material"), fld("claim_no", "upper").alias("claim_no"),
        fld("vin", "upper").alias("vin"), fld("pack_serial", "upper").alias("pack_serial"),
        fld("cell_batch", "upper").alias("cell_batch"), fld("root_cause_category", "upper").alias("root_cause_category"),
        F.col("sections"), *DOC_META)
    unmapped = d.where(F.col("_table").isNull())
    return {"service_job": job, "supplier_coa": coa, "quality_report": report}, unmapped


def run(spark, argv=None) -> dict:
    p = C.base_parser(__doc__.split("\n\n")[0])
    args = C.parse(p, argv)
    names = C.Names(args.db_prefix)
    C.ensure_databases(spark, names, ("silver", "ref"))
    days = C.date_range(args.date_from, args.date_to)
    dates = [d.isoformat() for d in days]
    in_dates = F.col("_business_date").isin(days)
    audit = C.Audit(spark, names, "build_silver", args.date_to, args.pipeline_run)
    audit.load(STAGE, "*", "STARTED", message=f"dates {dates[0]}..{dates[-1]}")
    t0, summary = time.time(), {}
    contracts = C.contracts()

    def bronze(entity):
        return spark.table(names.t("bronze", entity)).where(in_dates)

    def write(table, df, source, rows_in, transform="standardise", note=""):
        started = C.now_utc()
        target = names.t("silver", table)
        before = C.snapshot_id(spark, target)
        if "_batch_id" not in df.columns:
            df = df.withColumn("_batch_id", F.concat(F.lit("B"), F.date_format("_business_date", "yyyyMMdd")))
        C.write_dates(spark, df, target, dates)
        out = spark.table(target).where(in_dates)
        agg = out.agg(F.count(F.lit(1)).alias("n"),
                      (F.sum(F.when(F.size("_dq_issues") > 0, 1).otherwise(0)) if "_dq_issues" in out.columns
                       else F.lit(0)).alias("dq")).collect()[0]
        n, dq = int(agg["n"]), int(agg["dq"] or 0)
        msg = f"{rows_in} in, {n} out, {rows_in - n if rows_in is not None else 0} deduplicated, {dq} with dq issues{note}"
        audit.load(STAGE, table, "COMMITTED", rows_in=rows_in, rows_out=n, rows_rejected=dq, snapshot_before=before,
                   snapshot_after=C.snapshot_id(spark, target), started_at=started, message=msg)
        audit.transform(f"silver {table}", transform, source, target, rows_in, n, msg)
        summary[table] = {"rows_in": rows_in, "rows_out": n, "dq": dq}
        print(f"  {table:<24} {msg}", flush=True)

    try:
        for entity, table in TYPED.items():
            b = bronze(entity)
            df = typed_entity(b, contracts[entity])
            if table == "cell_step":
                df = df.withColumn("params", F.expr("str_to_map(params, ';', '=')")) \
                    .withColumn("duration_s", F.expr("timestampdiff(SECOND, start_ts, end_ts)"))
            df = df.drop(*DROP.get(table, []))
            write(table, df, names.t("bronze", entity), b.count())

        b = bronze("pack_build_doc")
        build, cells = pack_tables(b)
        n_pack = b.count()
        write("pack_build", build, names.t("bronze", "pack_build_doc"), n_pack, "normalise")
        n_cells = b.select(F.size(F.from_json("payload", PACK_DOC)["cells"]).alias("n")).agg(F.sum("n")).collect()[0][0]
        write("pack_cell", cells, names.t("bronze", "pack_build_doc"), int(n_cells or 0), "normalise")
        b = bronze("vehicle_eol_doc")
        write("vehicle_build", vehicle_table(b), names.t("bronze", "vehicle_eol_doc"), b.count(), "normalise")

        b = bronze("sap_odata_record")
        sets = {r[0]: r[1] for r in b.groupBy("entity_set").count().collect()}
        for entity_set, table in SAP_TABLE.items():
            write(table, sap_entity(b, entity_set, contracts["sap_odata_record"]),
                  names.t("bronze", "sap_odata_record"), sets.get(entity_set, 0))
        unknown_sets = sorted(set(sets) - set(SAP_TABLE))

        docs = spark.table(names.t("silver", "doc_extract")).where(in_dates)
        tables, unmapped = doc_tables(docs)
        by_table = {r[0]: r[1] for r in docs.where(F.col("status") == "EXTRACTED")
                    .withColumn("_t", F.coalesce(*[F.when(F.col("doc_title") == t, F.lit(v)) for t, v in DOC_TABLE.items()]))
                    .groupBy("_t").count().collect()}
        for table, df in tables.items():
            write(table, df.withColumn("_dq_issues", F.array().cast("array<string>")),
                  names.t("silver", "doc_extract"), by_table.get(table, 0), "extract")
        n_unmapped = unmapped.count()
        audit.load(STAGE, "*", "COMPLETED", rows_in=sum((s["rows_in"] or 0) for s in summary.values()),
                   rows_out=sum(s["rows_out"] for s in summary.values()),
                   rows_rejected=sum(s["dq"] for s in summary.values()),
                   message=f"{len(dates)} dates in {time.time() - t0:.0f}s; unmapped documents {n_unmapped}; "
                           f"unknown OData entity sets {unknown_sets}")
    except Exception as e:
        audit.load(STAGE, "*", "FAILED", message=C.error_summary(e))
        raise
    finally:
        audit.flush()
    print(f"SILVER_RESULT dates={len(dates)} seconds={time.time() - t0:.0f} unmapped_docs={n_unmapped} "
          f"unknown_sets={','.join(unknown_sets) or '-'}", flush=True)
    for k, v in summary.items():
        print(f"SILVER_TABLE {k} {v['rows_in']} {v['rows_out']} {v['dq']}", flush=True)
    return summary


def main() -> int:
    spark = C.get_spark("evx-build-silver")
    run(spark, sys.argv[1:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
