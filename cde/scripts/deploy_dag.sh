#!/usr/bin/env bash
# Register or update the Airflow DAG as a `--type airflow` CDE job from the git resource.
# `cde repository sync` alone does not refresh a registered DAG. The DAG is created paused
# (is_paused_upon_creation) and this script pauses it again if CDE opened it.
#
#   ./cde/scripts/deploy_dag.sh
#   cde job run --name rsingh-evx-orchestration --config-json '{"business_date": "2026-11-06"}'

set -euo pipefail

REPO_NAME="${REPO_NAME:-rsingh-evx-pipeline}"
DAG_JOB_NAME="${DAG_JOB_NAME:-rsingh-evx-orchestration}"
DAG_PATH="cde/dags/evx_dag.py"

cde repository sync --name "${REPO_NAME}"

if cde job describe --name "${DAG_JOB_NAME}" &>/dev/null; then
  echo "==> Updating ${DAG_JOB_NAME}"
  cde job update --name "${DAG_JOB_NAME}" --dag-file "${DAG_PATH}" --mount-1-resource "${REPO_NAME}"
else
  echo "==> Creating ${DAG_JOB_NAME}"
  cde job create --name "${DAG_JOB_NAME}" --type airflow --dag-file "${DAG_PATH}" --mount-1-resource "${REPO_NAME}"
fi

cde job schedule pause --name "${DAG_JOB_NAME}" || true
echo "DAG evx_factory_to_field registered, paused. Airflow needs about 30s to parse."
echo "  cde job run --name ${DAG_JOB_NAME} --config-json '{\"business_date\": \"2026-11-06\"}'"
