"""
The stream producers as a CDE Spark job (rsingh-evx-stream-produce), written to Kafka through Spark's Kafka sink.
CDE reaches the brokers on 9093 where a laptop cannot. Adapted from OGX's stream_produce.py.

  --topics telemetry[,dtc,qc,line]   which producers run (default telemetry, the M1 behaviour), in that order:
                     telemetry  stream/producer/telemetry.py, the live subset (rsingh_evx_ref.generator_fleet)
                     dtc        stream/producer/dtc.py, the same live subset
                     qc         stream/producer/qc.py, the date's QC exports in the generated tree (--generated)
                     line       stream/producer/line.py, generators.twin.station_cycle_events (--scale)
                     each with its per-minute control totals on evx.control (field topic)
  --setup            create every configured evx.* topic that does not exist yet, evx.alert.priority included
                     (Kafka AdminClient over py4j: partitions and retention from config/streaming.json, the
                     brokers' default replication). An existing topic is never altered (no partition or
                     retention change); it is reported EXISTS
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
from stream.producer import common as K  # noqa: E402
from stream.producer import telemetry as T  # noqa: E402
from stream_telemetry_bronze import STREAM, kafka_options  # noqa: E402


class SparkKafkaSink:
    def __init__(self, spark, opts: dict, batch: int):
        self.spark, self.opts, self.batch = spark, opts, batch
        self.buf: list[tuple[str, str, str]] = []
        self.sent = 0

    def send(self, topic: str, msg: dict, send_ts: float) -> None:
        self.buf.append((topic, K.record_key(msg), json.dumps(msg, separators=(",", ":"))))
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


PRODUCERS = ("telemetry", "dtc", "qc", "line")


def parse_topics(arg: str) -> list[str]:
    keys = [x.strip() for x in arg.split(",") if x.strip()]
    bad = [k for k in keys if k not in PRODUCERS]
    if bad or not keys:
        raise SystemExit(f"--topics: unknown {bad}; choose from {','.join(PRODUCERS)}")
    return [k for k in PRODUCERS if k in keys]


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
    ap.add_argument("--topics", default="telemetry", help="comma list of telemetry, dtc, qc, line")
    ap.add_argument("--generated", default=C.sibling(C.DEFAULT_LANDING, "generated"), help="qc: the generated tree")
    ap.add_argument("--scale", type=float, default=None, help="line: twin scale (default generator.scale, else 1)")
    args, _ = ap.parse_known_args(argv if argv is not None else sys.argv[1:])
    spark = C.get_spark("evx-stream-produce")
    opts = kafka_options(spark)
    print(f"brokers {opts['kafka.bootstrap.servers']} ({opts['kafka.security.protocol']})", flush=True)
    if args.setup:
        print("STREAM_TOPICS", json.dumps(create_topics(spark, opts)), flush=True)
    if args.mode == "none":
        return 0
    topics = parse_topics(args.topics)
    live = fleet(spark, C.Names(args.db_prefix)) if {"telemetry", "dtc"} & set(topics) else []
    run_id, t0 = f"backfill-{uuid.uuid4().hex[:8]}", time.time()
    sink = SparkKafkaSink(spark, opts, args.batch)
    days = [date.fromisoformat(x) for x in args.dates.split(",")]
    log = lambda s: print(s, flush=True)  # noqa: E731
    print(f"backfill {args.dates} topics {','.join(topics)} for {len(live):,} live vehicles, run {run_id}", flush=True)
    sent = {}
    for key in topics:
        before = sink.sent
        if key == "telemetry":
            T.backfill(sink, live, days, args.interval, run_id, log=log)
        elif key == "dtc":
            from stream.producer import dtc as D

            D.backfill(sink, live, days, run_id, log=log)
        elif key == "qc":
            from stream.producer import qc as Q

            Q.backfill(sink, C.filesystem(spark, args.generated), C.as_uri(args.generated), days, run_id, log=log)
        elif key == "line":
            from stream.producer import line as L

            cfg = C.config()
            scale = args.scale if args.scale is not None else float(cfg.get("generator", {}).get("scale", 1.0))
            L.backfill(sink, cfg, days, scale, run_id, log=log)
        sink.flush()
        sent[key] = sink.sent - before
    sink.close()
    print("STREAM_PRODUCED_TOPICS " + " ".join(f"{k}={v}" for k, v in sent.items()), flush=True)
    print(f"STREAM_PRODUCED messages={sink.sent} vehicles={len(live)} seconds={time.time() - t0:.0f} run={run_id}",
          flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
