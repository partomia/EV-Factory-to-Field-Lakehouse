"""
Streaming silver: bronze.factory_line_event (an Iceberg table read as a stream) to silver.line_cycle_window, as a
CDE Spark Structured Streaming job (rsingh-evx-stream-line-window); checkpoint <checkpoints>/line_window.

  windows    15-minute tumbling windows per station on mes_ts, the MES receive time (the trusted clock), with
             the line topic's watermark (config/streaming.json, 10 minutes): a station whose PLC clock is skewed
             keeps its windows; an event later than the watermark is dropped from the windows (bronze keeps it,
             flagged beyond_watermark)
  measures   cycles, mean / p95 / max cycle_s, counts per status (OK, FAULT, BLOCKED, STARVED), mean PLC - MES
             offset (event_ts - mes_ts, seconds: a skewed clock shows here, it is not corrected here), late
             events (sent > 60 s after mes_ts), first / last mes_ts
  write      foreachBatch in update mode: each batch's changed windows are MERGEd into silver.line_cycle_window
             on (station_id, window_start, window_date); a replayed batch rewrites the same values

  spark-submit stream_line_window.py [--checkpoints URI] [--trigger "60 seconds"|available-now] [--await-seconds N]
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
JOB = "rsingh-evx-stream-line-window"
SPEC = STREAM["jobs"][JOB]
LINE = STREAM["topics"]["line"]
QUERY = "stream-line-window"
WINDOW_SCHEMA = (
    "station_id string, line_id string, area string, window_start timestamp_ntz, window_end timestamp_ntz, "
    "cycles bigint, cycle_s_mean double, cycle_s_p95 double, cycle_s_max double, n_ok bigint, n_fault bigint, "
    "n_blocked bigint, n_starved bigint, plc_offset_s_mean double, n_late bigint, first_mes_ts timestamp_ntz, "
    "last_mes_ts timestamp_ntz, window_date date, _updated_at timestamp_ntz, _batch string")


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db-prefix", default=C.DEFAULT_PREFIX)
    p.add_argument("--checkpoints", default=C.sibling(C.DEFAULT_LANDING, "checkpoints"))
    p.add_argument("--trigger", default=f"{STREAM['spark']['trigger_seconds']} seconds")
    p.add_argument("--await-seconds", type=int, default=0)
    p.add_argument("--source", default=None, help="ignored (the source is the bronze table)")
    p.add_argument("--input", default=None, help="ignored")
    return p


def windowed(df: DataFrame) -> DataFrame:
    ts = F.col("mes_ts").cast("timestamp")
    status = F.col("status")
    return (df.select("station_id", "line_id", "area", "cycle_s", "status", "lateness_s", ts.alias("ts"),
                      (F.unix_millis(F.col("event_ts").cast("timestamp")) - F.unix_millis(ts)).alias("offset_ms"))
            .withWatermark("ts", LINE["watermark"])
            .groupBy("station_id", F.window("ts", SPEC["window"]).alias("w"))
            .agg(F.max("line_id").alias("line_id"), F.max("area").alias("area"), F.count("*").alias("cycles"),
                 F.avg("cycle_s").alias("cycle_s_mean"), F.percentile_approx("cycle_s", 0.95).alias("cycle_s_p95"),
                 F.max("cycle_s").alias("cycle_s_max"),
                 *[F.sum((status == s).cast("int")).cast("bigint").alias(f"n_{s.lower()}")
                   for s in ("OK", "FAULT", "BLOCKED", "STARVED")],
                 (F.avg("offset_ms") / 1000.0).alias("plc_offset_s_mean"),
                 F.sum((F.col("lateness_s") > 60).cast("int")).cast("bigint").alias("n_late"),
                 F.min("ts").alias("first_mes_ts"), F.max("ts").alias("last_mes_ts")))


def finish(df: DataFrame, batch_key: str) -> DataFrame:
    ntz = lambda c: F.col(c).cast("timestamp_ntz")  # noqa: E731
    return df.select(
        "station_id", "line_id", "area", F.col("w.start").cast("timestamp_ntz").alias("window_start"),
        F.col("w.end").cast("timestamp_ntz").alias("window_end"), "cycles", "cycle_s_mean",
        F.col("cycle_s_p95").cast("double").alias("cycle_s_p95"), "cycle_s_max", "n_ok", "n_fault", "n_blocked",
        "n_starved", "plc_offset_s_mean", "n_late", ntz("first_mes_ts").alias("first_mes_ts"),
        ntz("last_mes_ts").alias("last_mes_ts"), F.to_date(F.col("w.start")).alias("window_date"),
        F.current_timestamp().cast("timestamp_ntz").alias("_updated_at"), F.lit(batch_key).alias("_batch"))


def merge_batch(names: C.Names):
    target = names.t("silver", "line_cycle_window")
    assert target.startswith("rsingh_evx_"), target

    def batch(df: DataFrame, batch_id: int) -> None:
        spark = df.sparkSession
        out = finish(df, f"{QUERY}:{batch_id}")
        C.ensure_table(spark, target, WINDOW_SCHEMA, ["window_date"])
        out.createOrReplaceTempView("evx_line_window_updates")
        spark.sql(f"""
            MERGE INTO {target} t USING evx_line_window_updates s
            ON t.station_id = s.station_id AND t.window_start = s.window_start AND t.window_date = s.window_date
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *""")
        print(f"STREAM_LINE_WINDOW_BATCH {batch_id} windows={out.count()}", flush=True)

    return batch


def run(spark, argv=None):
    args, _ = parser().parse_known_args(argv)
    C.configure(spark)   # UTC session time zone also on a new session (local chained runs)
    names = C.Names(args.db_prefix)
    C.ensure_databases(spark, names, ("bronze", "silver"))
    source = names.t("bronze", "factory_line_event")
    events = (spark.readStream.option("streaming-skip-delete-snapshots", "true")
              .option("streaming-skip-overwrite-snapshots", "true").table(source))
    cp = f"{C.as_uri(args.checkpoints)}/{SPEC['checkpoint']}"
    assert SPEC["checkpoint"] not in ("telemetry_bronze", "telemetry_agg", "line_bronze"), SPEC
    w = (windowed(events).writeStream.queryName(QUERY).outputMode("update")
         .foreachBatch(merge_batch(names)).option("checkpointLocation", cp))
    w = w.trigger(availableNow=True) if args.trigger == "available-now" else w.trigger(processingTime=args.trigger)
    q = w.start()
    print(f"{QUERY}: started (from {source}, watermark {LINE['watermark']} on mes_ts, window {SPEC['window']}, "
          f"trigger {args.trigger}, checkpoint {cp})", flush=True)
    if args.await_seconds:
        q.awaitTermination(args.await_seconds)
        q.stop()
    else:
        q.awaitTermination()
    return q


def main() -> int:
    run(C.get_spark("evx-stream-line-window"), sys.argv[1:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
