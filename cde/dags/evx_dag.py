"""
Airflow DAG (CDE): one business date of the EV factory-to-field lakehouse.

  land -> bronze -> extract -> silver -> master -> gold -> reprocess_late, <enabled use cases> -> recon -> freshness
  scheduled runs also produce and drain that date's streams before gold, one job at a time:
    stream_produce (--topics telemetry,dtc,qc,line) -> stream_telemetry -> stream_agg
    -> stream_dtc -> stream_qc -> stream_line -> stream_line_window

Feature and score tasks come from every usecases/*/config.yaml with enabled: true.
The score task publishes the champion parquet already mounted on rsingh-evx-uc01-score.
Outcomes and WAP inside gold are M2. Generate is not a nightly stage: the files for a date
must already be in the generated tree before land runs.

Schedule 23:30 UTC. The window is the five live dates (2026-11-06 .. 2026-11-10): the first
interval ends on the 6th and the last starts on the 9th. Created paused. Manual runs take
params.business_date and skip the stream, so a replay does not produce the topic again.

  cde job run --name rsingh-evx-orchestration --config-json '{"business_date": "2026-11-06"}'
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta
from pathlib import Path

JOB_PREFIX = "rsingh-evx"
DB_PREFIX = "rsingh_evx"
LANDING = "s3a://federal-buk-574bcea0/data/IB/rsingh_evx/landing"
CHECKPOINTS = "s3a://federal-buk-574bcea0/data/IB/rsingh_evx/checkpoints"
SCORE_INPUT = "/app/mount/scores/scores.parquet"
SCHEDULE = "30 23 * * *"
STREAM_TOPICS = "telemetry,dtc,qc,line"
# first interval ends 2026-11-06 23:30 UTC; the last starts on END
START = datetime(2026, 11, 5, 23, 30)
END = datetime(2026, 11, 9, 23, 30)


def enabled_use_cases(root: Path | None = None) -> list[str]:
    """use_case ids whose config.yaml says enabled: true, in path order."""
    base = root or Path(__file__).resolve().parents[2]
    found = []
    for cfg in sorted((base / "usecases").glob("*/config.yaml")):
        text = cfg.read_text()
        if not re.search(r"(?m)^enabled:\s*true\s*$", text):
            continue
        match = re.search(r"(?m)^use_case:\s*(\S+)\s*$", text)
        if match:
            found.append(match.group(1))
    return found


try:
    from airflow import DAG
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.python import ShortCircuitOperator
    from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator
except ImportError:  # laptop tests import the helpers; Airflow is on CDE
    DAG = None


if DAG is not None:
    SCHEDULED = "dag_run.run_type == 'scheduled'"
    BUSINESS_DATE = ("{{ data_interval_end.strftime('%Y-%m-%d') if " + SCHEDULED +
                     " else params.business_date }}")

    default_args = {
        "owner": "evx-data",
        "depends_on_past": False,
        "retries": 0,
        "execution_timeout": timedelta(minutes=90),
    }

    with DAG(
        dag_id="evx_factory_to_field",
        description="One business date: land through gold, then each enabled use case, then recon",
        default_args=default_args,
        schedule_interval=SCHEDULE,
        start_date=START,
        end_date=END,
        catchup=False,
        is_paused_upon_creation=True,
        max_active_runs=1,
        dagrun_timeout=timedelta(hours=4),
        params={"business_date": "2026-11-06"},
        tags=["lakehouse", "iceberg", "evx"],
    ) as dag:

        def stage(task_id: str, job: str, *extra: str, **kwargs) -> CDEJobRunOperator:
            # run-time args replace the job's own, so every task passes the full set
            return CDEJobRunOperator(
                task_id=task_id,
                job_name=f"{JOB_PREFIX}-{job}",
                overrides={"spark": {"args": ["--business-date", BUSINESS_DATE, "--landing", LANDING,
                                              "--db-prefix", DB_PREFIX, "--pipeline-run", "{{ run_id }}", *extra]}},
                wait=True,
                **kwargs,
            )

        land = stage("land_sources", "land")
        bronze = stage("ingest_bronze", "bronze")
        extract = stage("extract_unstructured", "extract")
        silver = stage("build_silver", "silver")
        master = stage("build_master", "master")
        gold = stage("build_gold", "gold")
        reprocess = stage("reprocess_late", "reprocess")
        recon = stage("reconcile", "recon", trigger_rule="all_done")
        freshness = stage("freshness", "freshness", trigger_rule="all_done")
        batch_complete = EmptyOperator(task_id="batch_complete", trigger_rule="all_success")

        is_scheduled = ShortCircuitOperator(
            task_id="is_scheduled", python_callable=lambda dag_run, **_: dag_run.run_type == "scheduled",
            ignore_downstream_trigger_rules=False)

        def stream_job(task_id: str, job: str, *args: str) -> CDEJobRunOperator:
            return CDEJobRunOperator(
                task_id=task_id, job_name=f"{JOB_PREFIX}-{job}",
                overrides={"spark": {"args": list(args)}}, wait=True)

        produce = stream_job("stream_produce", "stream-produce", "--db-prefix", DB_PREFIX,
                             "--mode", "backfill", "--topics", STREAM_TOPICS, "--dates", BUSINESS_DATE)
        common = ("--db-prefix", DB_PREFIX, "--checkpoints", CHECKPOINTS, "--trigger", "available-now")
        stream_bronze = stream_job("stream_telemetry", "stream-telemetry", *common)
        stream_agg = stream_job("stream_agg", "stream-agg", *common)
        # each bronze drain before its window job; one at a time on the shared virtual cluster
        m2_streams = [stream_job(f"stream_{j.replace('-', '_')}", f"stream-{j}", *common)
                      for j in ("dtc", "qc", "line", "line-window")]
        stream_done = EmptyOperator(task_id="stream_done", trigger_rule="none_failed")

        scored = []
        for use_case in enabled_use_cases():
            features = stage(f"{use_case}_features", f"{use_case}-features")
            # the publish job has no --business-date; the mounted parquet is the champion snapshot
            score = CDEJobRunOperator(
                task_id=f"{use_case}_score",
                job_name=f"{JOB_PREFIX}-{use_case}-score",
                overrides={"spark": {"args": ["--input", SCORE_INPUT, "--db-prefix", DB_PREFIX,
                                              "--pipeline-run", "{{ run_id }}"]}},
                wait=True,
            )
            features >> score
            scored.append(score)
            gold >> features

        land >> bronze >> extract >> silver >> master >> gold
        chain = [produce, stream_bronze, stream_agg, *m2_streams, stream_done]
        is_scheduled >> chain[0]
        for a, b in zip(chain, chain[1:]):
            a >> b
        stream_done >> gold
        gold >> reprocess >> recon
        if scored:
            scored >> recon
        recon >> batch_complete
        recon >> freshness
