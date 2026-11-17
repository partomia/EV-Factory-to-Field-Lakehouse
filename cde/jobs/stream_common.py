"""
Shared code of the M2 streaming bronze jobs (rsingh-evx-stream-dtc, -stream-qc, -stream-line), the pattern of
stream_telemetry_bronze.py (whose functions it reuses unchanged; that job keeps its own code path):

  topics      config/streaming.json topics whose bronze_job is the job's name, nothing else (evx.alert.priority
              has none and is never subscribed)
  source      Kafka (subscribe to those topics only) or --source files (<input>/<topic>/*.jsonl)
  validate    contracts/stream/<topic>.json; rejects -> the job's own quarantine table (topics.<k>.quarantine)
  write       bronze.<topics.<k>.table>, partitioned by event_date, event_hour of the topic's event-time field
              (event_ts; mes_ts for line), with Kafka lineage, lateness_s and beyond_watermark (later than the
              topic's watermark)
  exactly     one Iceberg commit per table per micro-batch, snapshot property evx.stream-batch =
  once        <query id>:<batch id>; a replayed batch skips the tables that already hold it
  checkpoint  <checkpoints>/<jobs.<name>.checkpoint>, never one of the telemetry jobs' folders
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import evx_common as C  # noqa: E402
from pyspark.sql import DataFrame  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from stream_telemetry_bronze import (KAFKA_COLS, QUARANTINE_SCHEMA, append_once, kafka_options,  # noqa: E402
                                     kafka_ts_ntz, parse_topic)

STREAM = C.load_json("config/streaming.json")
TELEMETRY_CHECKPOINTS = ("telemetry_bronze", "telemetry_agg")


def job_topics(job: str) -> dict:
    """{key: topic config} of the topics this job subscribes to."""
    return {k: v for k, v in STREAM["topics"].items() if v.get("bronze_job") == job}


def checkpoint_name(job: str) -> str:
    name = STREAM["jobs"][job]["checkpoint"]
    assert re.fullmatch(r"[a-z0-9_]+", name) and name not in TELEMETRY_CHECKPOINTS, name
    return name


def seconds(interval: str) -> int:
    """'30 minutes' / '60 seconds' -> seconds."""
    n, unit = interval.split()
    return int(n) * {"second": 1, "minute": 60, "hour": 3600}[unit.rstrip("s")]


def parser(doc: str):
    p = argparse.ArgumentParser(description=doc)
    p.add_argument("--db-prefix", default=C.DEFAULT_PREFIX)
    p.add_argument("--source", default="kafka", choices=["kafka", "files"])
    p.add_argument("--input", default=None, help="--source files: the producers' --out folder")
    p.add_argument("--checkpoints", default=C.sibling(C.DEFAULT_LANDING, "checkpoints"))
    p.add_argument("--trigger", default=f"{STREAM['spark']['trigger_seconds']} seconds")
    p.add_argument("--starting-offsets", default=STREAM["spark"]["starting_offsets"])
    p.add_argument("--max-offsets", type=int, default=STREAM["spark"]["max_offsets_per_trigger"])
    p.add_argument("--await-seconds", type=int, default=0, help="stop after N seconds (0: run until stopped)")
    return p


def read_source(spark, args, names: list[str]) -> DataFrame:
    """kafka_topic, kafka_partition, kafka_offset, kafka_ts, record_key, record_value for the given topics."""
    if args.source == "files":
        frames = []
        for topic in names:
            folder = f"{C.as_uri(args.input)}/{topic}"
            if folder.startswith("file://"):
                Path(folder[len("file://"):]).mkdir(parents=True, exist_ok=True)
            raw = (spark.readStream.schema("key string, timestamp bigint, value string")
                   .option("maxFilesPerTrigger", 24).json(folder))
            frames.append(raw.select(F.lit(topic).alias("kafka_topic"), F.lit(0).alias("kafka_partition"),
                                     F.lit(None).cast("bigint").alias("kafka_offset"),
                                     F.timestamp_millis("timestamp").alias("kafka_ts"),
                                     F.col("key").alias("record_key"), F.col("value").alias("record_value")))
        out = frames[0]
        for f in frames[1:]:
            out = out.unionByName(f)
        return out
    raw = (spark.readStream.format("kafka").options(**kafka_options(spark))
           .option("subscribe", ",".join(names)).option("startingOffsets", args.starting_offsets)
           .option("maxOffsetsPerTrigger", args.max_offsets).option("failOnDataLoss", "false").load())
    return raw.select(F.col("topic").alias("kafka_topic"), F.col("partition").alias("kafka_partition"),
                      F.col("offset").alias("kafka_offset"), F.col("timestamp").alias("kafka_ts"),
                      F.col("key").cast("string").alias("record_key"), F.col("value").cast("string").alias("record_value"))


def process(names: C.Names, contracts: dict, topics: dict, query: dict, label: str):
    def batch(df: DataFrame, batch_id: int) -> None:
        while "id" not in query:   # set once start() returns
            time.sleep(0.1)
        key = f"{query['id']}:{batch_id}"
        df = kafka_ts_ntz(df).persist()
        counts = {}
        now = F.current_timestamp().cast("timestamp_ntz")
        for k, t in topics.items():
            parsed = parse_topic(df.where(F.col("kafka_topic") == t["name"]), contracts[t["name"]])
            ev = t["event_time"]
            good = (parsed.where(F.size("reject_reasons") == 0).drop("reject_reasons", "record_value", "record_key")
                    .withColumn("event_date", F.to_date(ev)).withColumn("event_hour", F.hour(ev))
                    .withColumn("lateness_s", (F.unix_millis(F.col("kafka_ts").cast("timestamp"))
                                               - F.unix_millis(F.col(ev).cast("timestamp"))) / 1000.0)
                    .withColumn("beyond_watermark", F.col("lateness_s") > seconds(t["watermark"]))
                    .withColumn("_ingested_at", now))
            bad = parsed.where(F.size("reject_reasons") > 0).select(
                *KAFKA_COLS, "record_key", "record_value", "reject_reasons",
                F.coalesce(F.to_date(F.get_json_object("record_value", f"$.{ev}")),
                           F.to_date("kafka_ts")).alias("event_date"), now.alias("_ingested_at"))
            counts[k] = append_once(good, names.t("bronze", t["table"]), key, ["event_date", "event_hour"])
            counts[f"{k}_quarantine"] = append_once(bad, names.t("bronze", t["quarantine"]), key, ["event_date"])
        df.unpersist()
        print(f"STREAM_BATCH {label} {batch_id} " + " ".join(f"{k}={v}" for k, v in counts.items()), flush=True)

    return batch


def start(stream_df: DataFrame, writer_fn, query_name: str, checkpoint: str, trigger: str, output_mode=None):
    w = stream_df.writeStream.queryName(query_name)
    if output_mode:
        w = w.outputMode(output_mode)
    w = w.foreachBatch(writer_fn).option("checkpointLocation", checkpoint)
    w = w.trigger(availableNow=True) if trigger == "available-now" else w.trigger(processingTime=trigger)
    return w.start()


def wait(q, await_seconds: int):
    if await_seconds:
        q.awaitTermination(await_seconds)
        q.stop()
    else:
        q.awaitTermination()
    return q


def run_bronze(spark, job: str, doc: str, argv=None):
    args, _ = parser(doc).parse_known_args(argv)
    C.configure(spark)   # UTC session time zone also on a new session (local chained runs)
    names = C.Names(args.db_prefix)
    topics = job_topics(job)
    assert topics and all(t["name"].startswith("evx.") and not t.get("derived") for t in topics.values()), topics
    C.ensure_databases(spark, names, ("bronze",))
    for t in topics.values():
        assert t["quarantine"] != "stream_quarantine", t
        C.ensure_table(spark, names.t("bronze", t["quarantine"]), QUARANTINE_SCHEMA, ["event_date"])
    contracts = C.stream_contracts()
    assert job.startswith("rsingh-evx-"), job
    label = job[len("rsingh-evx-"):]
    cp = f"{C.as_uri(args.checkpoints)}/{checkpoint_name(job)}"
    query = {}
    q = start(read_source(spark, args, [t["name"] for t in topics.values()]),
              process(names, contracts, topics, query, label), label, cp, args.trigger)
    query["id"] = str(q.id)
    print(f"{label}: started (query {q.id}, {args.source}, topics {','.join(t['name'] for t in topics.values())}, "
          f"trigger {args.trigger}, checkpoint {cp})", flush=True)
    return wait(q, args.await_seconds)
