"""
The telemetry producer as a CDE Spark job (rsingh-evx-stream-produce): stream/producer/telemetry.py's seeded
readings and control counts for the live subset (rsingh_evx_ref.generator_fleet), written to Kafka through
Spark's Kafka sink. CDE reaches the brokers on 9093 where a laptop cannot. Adapted from OGX's stream_produce.py.

  --setup            create the evx.* topics if absent (Kafka AdminClient over py4j: partitions and retention
                     from config/streaming.json, the brokers' default replication)
  --dates 2026-11-06[,2026-11-07]   backfill these dates (send-time order, as fast as the sink takes them)
  --mode none        topics only

Kafka settings as for stream_telemetry_bronze.py (spark.evx.kafka.*). Messages are buffered on the driver and
written in batches of --batch records.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import evx_common as C  # noqa: E402

C._import_path()
from stream.producer import telemetry as T  # noqa: E402
from stream_telemetry_bronze import STREAM, kafka_options  # noqa: E402


class SparkKafkaSink:
    def __init__(self, spark, opts: dict, batch: int):
        self.spark, self.opts, self.batch = spark, opts, batch
        self.buf: list[tuple[str, str, str]] = []
        self.sent = 0

    def send(self, topic: str, msg: dict, send_ts: float) -> None:
        self.buf.append((topic, msg.get("vin") or msg.get("topic"), json.dumps(msg, separators=(",", ":"))))
        self.sent += 1
        if len(self.buf) >= self.batch:
            self.flush()

    def flush(self) -> None:
        if not self.buf:
            return
        df = self.spark.createDataFrame(self.buf, "topic string, key string, value string")
        df.write.format("kafka").options(**self.opts).save()
        self.buf = []

    def close(self) -> None:
        self.flush()


def create_topics(spark, opts: dict) -> dict:
    jvm = spark._jvm
    props = jvm.java.util.Properties()
    for k, v in opts.items():
        props.put(k[len("kafka."):], v)
    admin = jvm.org.apache.kafka.clients.admin.AdminClient.create(props)
    try:
        existing = set(admin.listTopics().names().get())
        out, new = {}, []
        for t in STREAM["topics"].values():
            assert t["name"].startswith("evx."), t["name"]
            if t["name"] in existing:
                out[t["name"]] = "EXISTS"
                continue
            nt = jvm.org.apache.kafka.clients.admin.NewTopic(
                t["name"], jvm.java.util.Optional.of(t["partitions"]), jvm.java.util.Optional.empty())
            cfg = jvm.java.util.HashMap()
            cfg.put("retention.ms", str(t["retention_hours"] * 3600_000))
            new.append(nt.configs(cfg))
            out[t["name"]] = "CREATED"
        if new:
            lst = jvm.java.util.ArrayList()
            for nt in new:
                lst.add(nt)
            admin.createTopics(lst).all().get()
        return out
    finally:
        admin.close()


def fleet(spark, names: C.Names) -> list[dict]:
    rows = spark.table(names.t("ref", "generator_fleet")).where("live_subset").collect()
    return T.load_fleet({k: (v.isoformat() if isinstance(v, date) else v) for k, v in r.asDict().items()} for r in rows)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db-prefix", default=C.DEFAULT_PREFIX)
    ap.add_argument("--setup", action="store_true")
    ap.add_argument("--mode", choices=["backfill", "none"], default="backfill")
    ap.add_argument("--dates", default=C.CFG["calendar"]["live_dates"][0])
    ap.add_argument("--interval", type=int, default=T.P["interval_s"])
    ap.add_argument("--batch", type=int, default=250_000)
    args, _ = ap.parse_known_args(argv if argv is not None else sys.argv[1:])
    spark = C.get_spark("evx-stream-produce")
    opts = kafka_options(spark)
    print(f"brokers {opts['kafka.bootstrap.servers']} ({opts['kafka.security.protocol']})", flush=True)
    if args.setup:
        print("STREAM_TOPICS", json.dumps(create_topics(spark, opts)), flush=True)
    if args.mode == "none":
        return 0
    live = fleet(spark, C.Names(args.db_prefix))
    run_id, t0 = f"backfill-{uuid.uuid4().hex[:8]}", time.time()
    sink = SparkKafkaSink(spark, opts, args.batch)
    days = [date.fromisoformat(x) for x in args.dates.split(",")]
    print(f"backfill {args.dates} for {len(live):,} live vehicles at {args.interval} s, run {run_id}", flush=True)
    T.backfill(sink, live, days, args.interval, run_id, log=lambda s: print(s, flush=True))
    sink.close()
    print(f"STREAM_PRODUCED messages={sink.sent} vehicles={len(live)} seconds={time.time() - t0:.0f} run={run_id}",
          flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
