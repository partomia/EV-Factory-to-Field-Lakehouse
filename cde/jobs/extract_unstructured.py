"""
Stage 2 - Extract: rows from the objects bronze catalogued as VALID or DUPLICATE (bronze.doc_object).

  PDF   job cards, CoA, 8D and teardown reports -> silver.doc_extract (title, LABEL: value fields, sections);
        an image-only page is read by OCR (method OCR), counted in ref.load_audit
  PNG   X-ray -> silver.qc_image_metric (overhang blob rows and box); film -> streak columns, defect patch
  JDX   incoming XRF spectrum -> silver.spectrum_feature (Ni / Mn / Co / Fe / Cu K-alpha peaks, Fe/Ni ratio)
  EDR   event recorder dump -> silver.edr_event (trigger time, buffer extremes; arrival date vs event time)

Executors read the landed bytes (binaryFile) and run extract/ (standard library); an extraction error is a row
with status FAILED and its reason, never a failed batch. One commit per table replacing the dates present.

Usage:
  spark-submit extract_unstructured.py --business-date 2026-11-08
  spark-submit extract_unstructured.py --from 2026-02-09 --to 2026-11-05
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import evx_common as C  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

STAGE = "extract"
META = "doc_id string, source string, file_name string, status string, error string, _business_date string"
TABLES = {
    "doc_extract": META + ", doc_title string, method string, pages int, text_pages int, fields string, "
                          "sections string",
    "qc_image_metric": META + ", image_kind string, subject_id string, mean_gray double, edge_bright_px int, "
                              "overhang_px int, overhang_box string, streak_cols int, defect_patch boolean",
    "spectrum_feature": META + ", lot_id string, points int, ni_ka double, mn_ka double, co_ka double, "
                               "fe_ka double, cu_ka double, fe_ni_ratio double",
    "edr_event": META + ", vin string, event_id string, trigger_kind string, trigger_ts timestamp_ntz, hz int, "
                        "n_pre int, n_post int, duration_s double, cell_t_max_pre_c double, cell_t_max_post_c double, "
                        "cell_v_min_v double, cell_dv_max_mv double, pack_i_max_a double, soc_at_trigger double, "
                        "speed_at_trigger_kph double",
}
KIND = {"dms_jobcards": "doc_extract", "supplier_coa": "doc_extract", "supplier_8d": "doc_extract",
        "qc_xray": "qc_image_metric", "qc_film": "qc_image_metric", "spectra": "spectrum_feature",
        "edr": "edr_event"}
NCOLS = {t: len(s.split(", ")) for t, s in TABLES.items()}


def _extract(rows):
    """(table, row tuple matching TABLES[table] without _business_date at the end ... appended by the driver)."""
    from datetime import datetime

    from extract.doc_fields import read
    from extract.edr import parse as parse_edr
    from extract.jcamp import parse as parse_jcamp
    from extract.metrics import edr_summary, film_metrics, xray_metrics, xrf_peaks
    from extract.png import png_decode

    for r in rows:
        data, path, doc_id = bytes(r.content), r.path, r.doc_id
        src, name, day = r.source, r.file_name, r.day
        table = KIND[src]
        base = [doc_id, src, name]
        empty = [None] * (NCOLS[table] - 6)
        try:
            if table == "doc_extract":
                d = read(data)
                vals = [d["title"], d["method"], d["pages"], d["text_pages"], json.dumps(d["fields"], sort_keys=True),
                        json.dumps(d["sections"], sort_keys=True)]
            elif table == "qc_image_metric":
                w, h, px = png_decode(data)
                stem = name.rsplit(".", 1)[0]
                if src == "qc_xray":
                    m = xray_metrics(w, h, px)
                    vals = ["XRAY", stem, m["mean_gray"], m["edge_bright_px"], m["overhang_px"], m["overhang_box"],
                            None, None]
                else:
                    m = film_metrics(w, h, px)
                    vals = ["FILM", stem, m["mean_gray"], None, None, None, m["streak_cols"], m["defect_patch"]]
            elif table == "spectrum_feature":
                s = parse_jcamp(data.decode("ascii", errors="replace"))
                p = xrf_peaks(s["x"], s["y"])
                vals = [s["header"].get("$LOT"), len(s["x"]), p["ni_ka"], p["mn_ka"], p["co_ka"], p["fe_ka"],
                        p["cu_ka"], p["fe_ni_ratio"]]
            else:
                e = parse_edr(data)
                m = edr_summary(e)
                vals = [e["vin"], e["event_id"], e["trigger"], datetime.strptime(m["trigger_ts"], "%Y-%m-%d %H:%M:%S"),
                        e["hz"], e["n_pre"], e["n_post"], m["duration_s"], m["cell_t_max_pre_c"],
                        m["cell_t_max_post_c"], m["cell_v_min_v"], m["cell_dv_max_mv"], m["pack_i_max_a"],
                        m["soc_at_trigger"], m["speed_at_trigger_kph"]]
            yield table, tuple(base + ["EXTRACTED", None, day] + vals)
        except Exception as e:  # noqa: BLE001 - recorded on the row
            yield table, tuple(base + ["FAILED", f"{type(e).__name__}: {e}"[:300], day] + empty)


def run(spark, argv=None) -> dict:
    p = C.base_parser(__doc__.split("\n\n")[0])
    args = C.parse(p, argv)
    names = C.Names(args.db_prefix)
    C.ensure_databases(spark, names, ("silver", "ref"))
    dates = [d.isoformat() for d in C.date_range(args.date_from, args.date_to)]
    landing = C.as_uri(args.landing)
    audit = C.Audit(spark, names, "extract_unstructured", args.date_to, args.pipeline_run)
    audit.load(STAGE, "*", "STARTED", message=f"dates {dates[0]}..{dates[-1]}")
    t0, summary = time.time(), {}
    try:
        catalog = (spark.table(names.t("bronze", "doc_object"))
                   .where(F.col("_business_date").isin(dates) & F.col("ingest_status").isin("VALID", "DUPLICATE"))
                   .select("doc_id", "source", "file_name", "source_path",
                           F.col("_business_date").cast("string").alias("day")))
        n_cat = catalog.count()
        fs = C.filesystem(spark, landing)
        folders = [f"{landing}/{s}/{d}" for s in sorted(KIND) for d in dates if d in set(fs.list_dirs(f"{landing}/{s}"))]
        content = (spark.read.format("binaryFile").option("pathGlobFilter", "*.{png,pdf,jdx,edr}").load(folders)
                   .select(F.regexp_replace("path", r"^file:/+", "/").alias("p"), "content"))
        joined = catalog.withColumn("p", F.regexp_replace("source_path", r"^file:/+", "/")).join(content, "p") \
            .withColumnRenamed("p", "path")
        out = joined.rdd.mapPartitions(_extract).cache()
        for table, schema in TABLES.items():
            started = C.now_utc()
            target = names.t("silver", table)
            before = C.snapshot_id(spark, target)
            df = spark.createDataFrame(out.filter(lambda r, t=table: r[0] == t).map(lambda r: r[1]), schema) \
                .withColumn("_business_date", F.to_date("_business_date")) \
                .withColumn("_extracted_at", F.current_timestamp().cast("timestamp_ntz"))
            df = df.withColumn("_batch_id", F.concat(F.lit("B"), F.date_format("_business_date", "yyyyMMdd")))
            stats = {r["status"]: r["count"] for r in df.groupBy("status").count().collect()}
            extra = ""
            if table == "doc_extract":
                methods = {r["method"]: r["count"] for r in df.groupBy("method").count().collect()}
                extra = f"; methods {methods}"
            C.write_dates(spark, df, target, dates)
            n = sum(stats.values())
            audit.load(STAGE, table, "COMMITTED", rows_in=n, rows_out=stats.get("EXTRACTED", 0),
                       rows_rejected=stats.get("FAILED", 0), snapshot_before=before,
                       snapshot_after=C.snapshot_id(spark, target), started_at=started,
                       message=f"{stats}{extra}")
            audit.transform(f"extract {table}", "extract", names.t("bronze", "doc_object"), target, n,
                            stats.get("EXTRACTED", 0), f"{stats}{extra}")
            summary[table] = {"rows_in": n, "accepted": stats.get("EXTRACTED", 0), "rejected": stats.get("FAILED", 0)}
            print(f"  {table:<18} {n:>7} rows {stats}{extra}", flush=True)
        out.unpersist()
        joined_n = sum(s["rows_in"] for s in summary.values())
        if joined_n != n_cat:
            raise RuntimeError(f"{n_cat} catalogued objects but {joined_n} read from landing")
        audit.load(STAGE, "*", "COMPLETED", rows_in=n_cat, rows_out=sum(s["accepted"] for s in summary.values()),
                   rows_rejected=sum(s["rejected"] for s in summary.values()),
                   message=f"{len(dates)} dates in {time.time() - t0:.0f}s")
    except Exception as e:
        audit.load(STAGE, "*", "FAILED", message=C.error_summary(e))
        raise
    finally:
        audit.flush()
    print(f"EXTRACT_RESULT dates={len(dates)} seconds={time.time() - t0:.0f}", flush=True)
    for k, v in summary.items():
        print(f"EXTRACT_TABLE {k} {v['rows_in']} {v['accepted']} {v['rejected']}", flush=True)
    return summary


def main() -> int:
    spark = C.get_spark("evx-extract-unstructured")
    C.ship_packages(spark)
    run(spark, sys.argv[1:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
