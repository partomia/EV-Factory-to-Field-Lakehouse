#!/usr/bin/env bash
# The CDE Spark Structured Streaming jobs (adapted from OGX), listed in config/streaming.json "jobs":
#   group telemetry (M1)
#     rsingh-evx-stream-telemetry    Kafka evx.vehicle.telemetry, evx.control -> bronze.telemetry_reading / stream_control
#     rsingh-evx-stream-agg          bronze.telemetry_reading -> silver.telemetry_window (watermark, MERGE)
#   group m2
#     rsingh-evx-stream-dtc          Kafka evx.vehicle.dtc   -> bronze.vehicle_dtc_event  (quarantine dtc_quarantine)
#     rsingh-evx-stream-qc           Kafka evx.factory.qc    -> bronze.factory_qc_event   (quarantine qc_quarantine)
#     rsingh-evx-stream-line         Kafka evx.factory.line  -> bronze.factory_line_event (quarantine line_quarantine)
#     rsingh-evx-stream-line-window  bronze.factory_line_event -> silver.line_cycle_window (watermark, MERGE)
# and the producer, rsingh-evx-stream-produce (create only; run it with a topic filter, e.g.
#   cde job run --name rsingh-evx-stream-produce --arg=--setup --arg=--topics --arg=dtc,qc,line --arg=--dates --arg=2026-11-06)
# evx.alert.priority is written by NiFi and read by no job here.
#
#   set -a; source .env; set +a
#   ./cde/scripts/deploy_streaming.sh create             # create or update every job (no run)
#   ./cde/scripts/deploy_streaming.sh start [GROUP]      # continuous runs (resume from the checkpoints)
#   ./cde/scripts/deploy_streaming.sh drain [GROUP]      # one available-now run each, in dependency order, waiting
#   ./cde/scripts/deploy_streaming.sh drain-m2           # = drain m2 (dtc, qc, line, then line-window)
#   ./cde/scripts/deploy_streaming.sh stop [GROUP]       # kill the running runs (checkpoints kept)
#   ./cde/scripts/deploy_streaming.sh status
# GROUP: all (default) | telemetry | m2. A drain refuses to start (before running anything) while any job of the
# group has a starting or running run: an available-now run must never share a checkpoint with a continuous one.
#
# Sizes: the telemetry jobs and the producer keep their M1 sizes (driver 1 / 2 GB, executors 2 cores / 4 GB, up
# to 2; 283k messages a day). The M2 jobs use config/platform.json resources.defaults.cde_stream: driver 1 / 2 GB,
# one executor 1 core / 2 GB (a few thousand messages a day).
#
# Kafka: SASL_SSL / PLAIN with the workload user (EVX_KAFKA_* from .env). The password is passed as Spark conf
# spark.evx.kafka.password, which Spark redacts in the UI and logs; it is never echoed here. The Kafka connector
# comes from spark.jars.packages. The CA: the image's JVM truststore. Nothing is deleted: checkpoints stay under
# CHECKPOINTS. Only rsingh-evx-* jobs are touched.

set -euo pipefail

ACTION="${1:-status}"
GROUP="${2:-all}"
[[ "${ACTION}" == drain-m2 ]] && { ACTION=drain; GROUP=m2; }
REPO_NAME="${REPO_NAME:-rsingh-evx-pipeline}"
JOB_PREFIX="rsingh-evx"
CHECKPOINTS="${CHECKPOINTS:-s3a://federal-buk-574bcea0/data/IB/rsingh_evx/checkpoints}"
KAFKA_PACKAGE="${KAFKA_PACKAGE:-org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4}"
CONFIG="$(cd "$(dirname "$0")/../.." && pwd)/config/streaming.json"

RESOURCES=(--driver-cores 1 --driver-memory 2g --executor-cores "${EXECUTOR_CORES:-2}"
           --executor-memory "${EXECUTOR_MEMORY:-4g}" --min-executors 1 --initial-executors 1
           --max-executors "${MAX_EXECUTORS:-2}" --conf spark.sql.shuffle.partitions=12)
RESOURCES_M2=(--driver-cores 1 --driver-memory 2g --executor-cores 1 --executor-memory 2g
              --min-executors 1 --initial-executors 1 --max-executors 1 --conf spark.sql.shuffle.partitions=4)

# "<job> <file> <kafka 0|1> <group>" per line, in dependency order (each job after the one it reads)
stream_jobs() {
  python3 - "${CONFIG}" "$1" <<'EOF'
import json, sys
cfg, group = json.load(open(sys.argv[1])), sys.argv[2]
if group not in ("all", "telemetry", "m2"):
    sys.exit(f"unknown group {group}")
seen = []
for name, j in cfg["jobs"].items():
    assert name.startswith("rsingh-evx-stream-"), name
    assert j.get("after") in (None, *seen), f"{name} listed before {j['after']}"
    seen.append(name)
    kafka = int(any(t.get("bronze_job") == name for t in cfg["topics"].values()))
    if group in ("all", j["group"]):
        print(name, j["file"], kafka, j["group"])
EOF
}

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
    cde job update --name "${name}" --application-file "${file}" "$@" >/dev/null
  else
    echo "==> Creating ${name} (${file})"
    cde job create --name "${name}" --type spark --mount-1-resource "${REPO_NAME}" \
      --application-file "${file}" "$@" >/dev/null
  fi
}

running_ids() {
  cde run list --filter "job[eq]$1" --filter "status[rlike]^(starting|running)$" 2>/dev/null \
    | python3 -c "import json,sys; print(' '.join(str(r['id']) for r in json.load(sys.stdin)))"
}

run_id() { python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])'; }
run_status() { cde run describe --id "$1" 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])'; }

case "${ACTION}" in
  create)
    kafka_conf
    while read -r name file kafka group; do
      sizes=("${RESOURCES[@]}"); [[ "${group}" == m2 ]] && sizes=("${RESOURCES_M2[@]}")
      extra=(); [[ "${kafka}" == 1 ]] && extra=("${KCONF[@]}")
      upsert "${name}" "${file}" "${sizes[@]}" ${extra[@]+"${extra[@]}"} --arg=--checkpoints --arg="${CHECKPOINTS}"
    done < <(stream_jobs all)
    upsert "${JOB_PREFIX}-stream-produce" cde/jobs/stream_produce.py "${RESOURCES[@]}" "${KCONF[@]}"
    ;;
  start)
    for j in $(stream_jobs "${GROUP}" | cut -d' ' -f1); do
      if [[ -n "$(running_ids "$j")" ]]; then echo "$j: already running"; else
        echo "$j: run $(cde job run --name "$j" --arg=--checkpoints --arg="${CHECKPOINTS}" | run_id)"; fi
    done
    ;;
  drain)
    # In order: a window job reads its bronze table, so it starts after the bronze drain has committed.
    list=$(stream_jobs "${GROUP}" | cut -d' ' -f1)
    for j in ${list}; do
      ids=$(running_ids "$j")
      if [[ -n "${ids}" ]]; then echo "$j: a continuous run is active (${ids}); refusing to drain ${GROUP}"; exit 1; fi
    done
    for j in ${list}; do
      id=$(cde job run --name "$j" --arg=--checkpoints --arg="${CHECKPOINTS}" --arg=--trigger --arg=available-now | run_id)
      echo "$j: run ${id}"
      for _ in $(seq 1 "${DRAIN_POLLS:-60}"); do
        s=$(run_status "${id}"); [[ "$s" == starting || "$s" == running ]] || break; sleep 10
      done
      echo "$j: ${s}"; [[ "$s" == succeeded ]] || exit 1
    done
    ;;
  stop)
    for j in $(stream_jobs "${GROUP}" | cut -d' ' -f1); do
      for id in $(running_ids "$j"); do echo "$j: killing run ${id}"; cde run kill --id "${id}"; done
    done
    ;;
  status)
    for j in $(stream_jobs all | cut -d' ' -f1) "${JOB_PREFIX}-stream-produce"; do
      cde run list --filter "job[eq]$j" 2>/dev/null | python3 -c "
import json,sys
runs=sorted(json.load(sys.stdin), key=lambda r: r['id'])[-3:]
print('$j:', ', '.join(f\"run {r['id']} {r['status']}\" for r in runs) or 'never run')"
    done
    ;;
  *) echo "usage: $0 create|start|drain|drain-m2|stop|status [all|telemetry|m2]"; exit 2 ;;
esac
