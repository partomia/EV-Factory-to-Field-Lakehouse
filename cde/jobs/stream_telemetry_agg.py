"""
Streaming silver: bronze.telemetry_reading (an Iceberg table read as a stream) to silver.telemetry_window, as a
CDE Spark Structured Streaming job (rsingh-evx-stream-agg). Adapted from OGX's stream_telemetry_agg.py.

  normalise  firmware that reports cell temperatures in degrees F (BMS-*-F) is converted to degrees C; the
             window counts the converted readings (n_fahrenheit), so the fault stays visible
  windows    15-minute tumbling windows per VIN on event time, with a 10-minute watermark: a reading up to 10
             minutes late updates its window; later than that it is dropped (the stream recon counts it LATE)
  measures   readings, distance (odometer span), speed mean / max, SoC first / last / min / max / std, pack
             current max, cell temperature max / min, cell voltage spread max, late readings (sent > 60 s after
             the event), UNCERTAIN / BAD quality, BMS firmware
  flags      is_stuck_soc (SoC std 0 over at least stuck_soc_min_readings while moving), is_sparse (fewer
             than min_readings_per_window)
  write      foreachBatch in update mode: each batch's changed windows are MERGEd into silver.telemetry_window
             on (vin, window_start), so a late reading corrects its window instead of duplicating it

  spark-submit stream_telemetry_agg.py [--checkpoints URI] [--trigger "60 seconds"|available-now]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import evx_common as C  # noqa: E402
from pyspark.sql import DataFrame  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

STREAM = C.load_json("config/streaming.json")
SPARK_CFG = STREAM["spark"]
QUERY = "stream-agg"
WINDOW_SCHEMA = (
    "vin string, window_start timestamp_ntz, window_end timestamp_ntz, n bigint, distance_km double, "
    "speed_mean double, speed_max double, soc_first double, soc_last double, soc_min double, soc_max double, "
    "soc_std double, current_max double, cell_t_max double, cell_t_min double, cell_dv_max_mv double, "
    "n_late bigint, n_fahrenheit bigint, n_uncertain bigint, n_bad bigint, bms_fw string, geohash5 string, "
    "first_event_ts timestamp_ntz, last_event_ts timestamp_ntz, is_stuck_soc boolean, is_sparse boolean, "
    "window_date date, _updated_at timestamp_ntz, _batch string")


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db-prefix", default=C.DEFAULT_PREFIX)
    p.add_argument("--checkpoints", default=C.sibling(C.DEFAULT_LANDING, "checkpoints"))
    p.add_argument("--trigger", default=f"{SPARK_CFG['trigger_seconds']} seconds")
    p.add_argument("--await-seconds", type=int, default=0)
    p.add_argument("--source", default=None, help="ignored (the source is the bronze table)")
    p.add_argument("--input", default=None, help="ignored")
    return p


def normalised(df: DataFrame) -> DataFrame:
    fahrenheit = F.col("bms_fw").endswith(STREAM["units"]["fahrenheit_fw_suffix"])

    def to_c(c):
        return F.when(fahrenheit, (F.col(c) - 32) * 5 / 9).otherwise(F.col(c))

    return df.select(
        "vin", F.col("event_ts").cast("timestamp").alias("ts"), "speed_kmh", "odometer_km", "soc_pct",
        "pack_current_a", to_c("cell_t_max").alias("cell_t_max"), to_c("cell_t_min").alias("cell_t_min"),
        (F.col("cell_v_max_mv") - F.col("cell_v_min_mv")).alias("dv"), "quality", "bms_fw", "geohash5",
        fahrenheit.alias("was_f"), (F.col("lateness_s") > 60).alias("is_late"))


def windowed(df: DataFrame) -> DataFrame:
    return (df.withWatermark("ts", SPARK_CFG["watermark"])
            .groupBy("vin", F.window("ts", SPARK_CFG["window"]).alias("w"))
            .agg(F.count("*").alias("n"), (F.max("odometer_km") - F.min("odometer_km")).alias("distance_km"),
                 F.avg("speed_kmh").alias("speed_mean"), F.max("speed_kmh").alias("speed_max"),
                 F.min_by("soc_pct", "ts").alias("soc_first"), F.max_by("soc_pct", "ts").alias("soc_last"),
                 F.min("soc_pct").alias("soc_min"), F.max("soc_pct").alias("soc_max"),
                 F.stddev_pop("soc_pct").alias("soc_std"), F.max("pack_current_a").alias("current_max"),
                 F.max("cell_t_max").alias("cell_t_max"), F.min("cell_t_min").alias("cell_t_min"),
                 F.max("dv").alias("cell_dv_max_mv"),
                 F.sum(F.col("is_late").cast("int")).cast("bigint").alias("n_late"),
                 F.sum(F.col("was_f").cast("int")).cast("bigint").alias("n_fahrenheit"),
                 F.sum((F.col("quality") == "UNCERTAIN").cast("int")).cast("bigint").alias("n_uncertain"),
                 F.sum((F.col("quality") == "BAD").cast("int")).cast("bigint").alias("n_bad"),
                 F.max("bms_fw").alias("bms_fw"), F.max("geohash5").alias("geohash5"),
                 F.min("ts").alias("first_event_ts"), F.max("ts").alias("last_event_ts")))


def finish(df: DataFrame, batch_key: str) -> DataFrame:
    ntz = lambda c: F.col(c).cast("timestamp_ntz")  # noqa: E731
    return df.select(
        "vin", F.col("w.start").cast("timestamp_ntz").alias("window_start"),
        F.col("w.end").cast("timestamp_ntz").alias("window_end"), "n", "distance_km", "speed_mean", "speed_max",
        "soc_first", "soc_last", "soc_min", "soc_max", "soc_std", "current_max", "cell_t_max", "cell_t_min",
        "cell_dv_max_mv", "n_late", "n_fahrenheit", "n_uncertain", "n_bad", "bms_fw", "geohash5",
        ntz("first_event_ts").alias("first_event_ts"), ntz("last_event_ts").alias("last_event_ts"),
        ((F.col("n") >= SPARK_CFG["stuck_soc_min_readings"]) & (F.coalesce("soc_std", F.lit(1.0)) < 1e-9)
         & (F.col("distance_km") > 0)).alias("is_stuck_soc"),
        (F.col("n") < SPARK_CFG["min_readings_per_window"]).alias("is_sparse"),
        F.to_date(F.col("w.start")).alias("window_date"),
        F.current_timestamp().cast("timestamp_ntz").alias("_updated_at"), F.lit(batch_key).alias("_batch"))


def merge_batch(names: C.Names):
    target = names.t("silver", "telemetry_window")

    def batch(df: DataFrame, batch_id: int) -> None:
        spark = df.sparkSession
        out = finish(df, f"{QUERY}:{batch_id}")
        C.ensure_table(spark, target, WINDOW_SCHEMA, ["window_date"])
        out.createOrReplaceTempView("evx_window_updates")
        spark.sql(f"""
            MERGE INTO {target} t USING evx_window_updates s
            ON t.vin = s.vin AND t.window_start = s.window_start AND t.window_date = s.window_date
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *""")
        print(f"STREAM_AGG_BATCH {batch_id} windows={out.count()}", flush=True)

    return batch


def run(spark, argv=None):
    args, _ = parser().parse_known_args(argv)
    names = C.Names(args.db_prefix)
    C.ensure_databases(spark, names, ("bronze", "silver"))
    source = names.t("bronze", "telemetry_reading")
    readings = (spark.readStream.option("streaming-skip-delete-snapshots", "true")
                .option("streaming-skip-overwrite-snapshots", "true").table(source))
    w = (windowed(normalised(readings)).writeStream.queryName(QUERY).outputMode("update")
         .foreachBatch(merge_batch(names)).option("checkpointLocation", f"{C.as_uri(args.checkpoints)}/telemetry_agg"))
    w = w.trigger(availableNow=True) if args.trigger == "available-now" else w.trigger(processingTime=args.trigger)
    q = w.start()
    print(f"{QUERY}: started (from {source}, watermark {SPARK_CFG['watermark']}, window {SPARK_CFG['window']}, "
          f"trigger {args.trigger})", flush=True)
    if args.await_seconds:
        q.awaitTermination(args.await_seconds)
        q.stop()
    else:
        q.awaitTermination()
    return q


def main() -> int:
    spark = C.get_spark("evx-stream-agg")
    run(spark, sys.argv[1:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
