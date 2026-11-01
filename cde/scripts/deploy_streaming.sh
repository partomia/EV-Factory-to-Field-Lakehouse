#!/usr/bin/env bash
# The CDE Spark Structured Streaming jobs (adapted from OGX):
#   rsingh-evx-stream-telemetry  Kafka evx.vehicle.telemetry, evx.control -> bronze.telemetry_reading / stream_control
#   rsingh-evx-stream-agg        bronze.telemetry_reading -> silver.telemetry_window (watermark, MERGE)
# and the producer, rsingh-evx-stream-produce (create only; run it with
#   cde job run --name rsingh-evx-stream-produce --arg=--setup --arg=--dates --arg=2026-11-06)
#
#   set -a; source .env; set +a
#   ./cde/scripts/deploy_streaming.sh create     # create or update the jobs (no run)
#   ./cde/scripts/deploy_streaming.sh start      # start both streams (resume from the checkpoints)
#   ./cde/scripts/deploy_streaming.sh drain      # one available-now run of each (what is on the topic, then stop)
#   ./cde/scripts/deploy_streaming.sh stop       # kill the running stream runs (checkpoints kept)
#   ./cde/scripts/deploy_streaming.sh status
#
# Kafka: SASL_SSL / PLAIN with the workload user (EVX_KAFKA_* from .env). The password is passed as Spark conf
# spark.evx.kafka.password, which Spark redacts in the UI and logs; it is never echoed here. The Kafka connector
# comes from spark.jars.packages. The CA: the image's JVM truststore. Nothing is deleted: checkpoints stay under
# CHECKPOINTS. Only rsingh-evx-* jobs are touched.

set -euo pipefail

ACTION="${1:-status}"
REPO_NAME="${REPO_NAME:-rsingh-evx-pipeline}"
JOB_PREFIX="rsingh-evx"
CHECKPOINTS="${CHECKPOINTS:-s3a://federal-buk-574bcea0/data/IB/rsingh_evx/checkpoints}"
KAFKA_PACKAGE="${KAFKA_PACKAGE:-org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4}"
STREAMS=("${JOB_PREFIX}-stream-telemetry" "${JOB_PREFIX}-stream-agg")

RESOURCES=(--driver-cores 1 --driver-memory 2g --executor-cores "${EXECUTOR_CORES:-2}"
           --executor-memory "${EXECUTOR_MEMORY:-4g}" --min-executors 1 --initial-executors 1
           --max-executors "${MAX_EXECUTORS:-2}" --conf spark.sql.shuffle.partitions=12)

kafka_conf() {
  : "${EVX_KAFKA_BOOTSTRAP:?source .env first}" "${EVX_KAFKA_USER:?}" "${EVX_KAFKA_PASSWORD:?}"
  KCONF=(--conf "spark.jars.packages=${KAFKA_PACKAGE}"
         --conf "spark.evx.kafka.bootstrap=${EVX_KAFKA_BOOTSTRAP}"
         --conf "spark.evx.kafka.protocol=${EVX_KAFKA_SECURITY_PROTOCOL:-SASL_SSL}"
         --conf "spark.evx.kafka.mechanism=${EVX_KAFKA_SASL_MECHANISM:-PLAIN}"
         --conf "spark.evx.kafka.user=${EVX_KAFKA_USER}"
         --conf "spark.evx.kafka.password=${EVX_KAFKA_PASSWORD}")
}

upsert() {
  local name=$1 file=$2; shift 2
  [[ "${name}" == ${JOB_PREFIX}-* ]] || { echo "refusing to touch ${name}"; exit 2; }
  if cde job describe --name "${name}" &>/dev/null; then
    echo "==> Updating ${name}"
    cde job update --name "${name}" --application-file "${file}" "${RESOURCES[@]}" "$@" >/dev/null
  else
    echo "==> Creating ${name} (${file})"
    cde job create --name "${name}" --type spark --mount-1-resource "${REPO_NAME}" \
      --application-file "${file}" "${RESOURCES[@]}" "$@" >/dev/null
  fi
}

running_ids() {
  cde run list --filter "job[eq]$1" --filter "status[rlike]^(starting|running)$" 2>/dev/null \
    | python3 -c "import json,sys; print(' '.join(str(r['id']) for r in json.load(sys.stdin)))"
}

run_id() { python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])'; }

case "${ACTION}" in
  create)
    kafka_conf
    upsert "${STREAMS[0]}" cde/jobs/stream_telemetry_bronze.py "${KCONF[@]}" --arg=--checkpoints --arg="${CHECKPOINTS}"
    upsert "${STREAMS[1]}" cde/jobs/stream_telemetry_agg.py --arg=--checkpoints --arg="${CHECKPOINTS}"
    upsert "${JOB_PREFIX}-stream-produce" cde/jobs/stream_produce.py "${KCONF[@]}"
    ;;
  start)
    for j in "${STREAMS[@]}"; do
      if [[ -n "$(running_ids "$j")" ]]; then echo "$j: already running"; else
        echo "$j: run $(cde job run --name "$j" | run_id)"; fi
    done
    ;;
  drain)
    for j in "${STREAMS[@]}"; do
      if [[ -n "$(running_ids "$j")" ]]; then echo "$j: already running"; else
        echo "$j: run $(cde job run --name "$j" --arg=--trigger --arg=available-now | run_id)"; fi
    done
    ;;
  stop)
    for j in "${STREAMS[@]}"; do
      for id in $(running_ids "$j"); do echo "$j: killing run ${id}"; cde run kill --id "${id}"; done
    done
    ;;
  status)
    for j in "${STREAMS[@]}" "${JOB_PREFIX}-stream-produce"; do
      cde run list --filter "job[eq]$j" 2>/dev/null | python3 -c "
import json,sys
runs=sorted(json.load(sys.stdin), key=lambda r: r['id'])[-3:]
print('$j:', ', '.join(f\"run {r['id']} {r['status']}\" for r in runs) or 'never run')"
    done
    ;;
  *) echo "usage: $0 create|start|drain|stop|status"; exit 2 ;;
esac
