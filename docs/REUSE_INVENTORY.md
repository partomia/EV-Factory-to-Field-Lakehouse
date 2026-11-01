# Reuse inventory

Everything this repo takes from the sibling demos on the `federal` environment: **OGX**
(O-G-Asset-Integrity-Lakehouse, the primary template), **CXR** (Chest-X-ray-Triage) and **GDL**
(General-DataLakehouse). Started from OGX's `docs/REUSE_INVENTORY.md`. No secret values appear here: secrets
live only in the git-ignored `.env` (OGX's `.env` renamed `OGX_*` → `EVX_*`) and in CAI project / Airflow
variables.

Status: **verified read-only 2026-11-01** = checked against the live platform or the sibling file on that date;
**to verify** = carried over, checked before first use.

## Connections, credentials, conventions

| What | Where found (repo:path) | EVX env var or config key | Reuse | Status |
|---|---|---|---|---|
| Environment `federal-cdp-env`, AWS `ap-south-1` | `cdp environments describe-environment` | `config/platform.json` `environment` | as-is | verified read-only 2026-11-01 |
| CDW Impala `coordinator-federal-impala-1.dw-federal-cdp-env.dp5i-5vkq.cloudera.site:443`, HTTP, `cliservice`, LDAP | OGX:config/lakehouse.json `impala` | `config/lakehouse.json` `impala` | as-is | VW running verified read-only 2026-11-01; login to verify |
| Impala workload user and password | OGX:.env `OGX_IMPALA_*` | `EVX_IMPALA_USER`, `EVX_IMPALA_PASSWORD` | renamed | to verify |
| Impala client impyla, `use_ssl`, `use_http_transport`; ImpalaStore / SparkStore | OGX:lakehouse/store.py | `lakehouse/store.py` | adapted | to verify |
| Impala cannot write Iceberg `timestamptz`: ref tables use `timestamp_ntz` in UTC | OGX/GDL PROJECT_LOG | `cde/jobs/evx_common.py` | as-is | lesson |
| Impala reserved words (`format`, `method`, `change`, `matched`) | OGX:tests | `tests/test_impala_sql.py` | as-is | lesson |
| CAI workspace `federal-cml-ws` (2.0.58), host | `cdp ml list-workspaces`; OGX:.env `OGX_CAI_HOST` | `EVX_CAI_HOST` | renamed | verified read-only 2026-11-01 (API v2 read with the key) |
| CAI API v2 key | OGX:.env `OGX_CAI_API_KEY` | `EVX_CAI_API_KEY`; GitHub secret `CAI_API_KEY` | renamed | verified read-only 2026-11-01 (project list) |
| CAI CPU groups: m5.2xlarge 1–10 (2 now), m5.8xlarge 4–8 (4 now); GPU g5.12xlarge 0–1 (0) | `cdp ml describe-workspace` | `config/platform.json` `resources` | new | verified read-only 2026-11-01 |
| CAI runtime PBJ JupyterLab Python 3.11, `resolve_runtime()` paging | OGX:ci/cai_jobs.py | `ci/cai_jobs.py` | as-is | to verify (latest runtime tag) |
| CAI project setup over API v2 from the public Git URL | OGX:ci/setup_cai.py | `ci/setup_cai.py`, project `rsingh-ev-factory-to-field` | adapted | to verify |
| CAI job chain, sizes, timeouts, env | OGX:ci/cai_jobs.py | jobs `evx-uc01-00..09`, `evx-uc02-02-baseline`, `stop-evx-project` | adapted | to verify |
| Sync-code job (`git reset --hard`, `EXPECTED_GIT_SHA`, pip once per requirements hash) | OGX:ci/sync_code.py | `ci/sync_code.py` | as-is | to verify |
| GitHub → CAI trigger, status normalisation | OGX:ci/trigger_cai_pipeline.py, .github/workflows/cai-mlops.yml | same paths | adapted | to verify |
| Data Visualization (8.1.4), connection `federal-impala-1`, API key | OGX:config/lakehouse.json `dataviz_url`, `dataviz_connection`; .env `OGX_VIZ_API_KEY` | `config/lakehouse.json`; `EVX_VIZ_API_KEY` | renamed | instance running verified read-only 2026-11-01; key to verify |
| Data Viz primary keys in use: Coll 7236, Mule 7419, Spend 8240, Churn 9242, CASA 9414, GDL 9508, CXR 12414, OGX 13507, loan-approval (MCC) 14505 | every sibling `dataviz/*.json` | EVX core 15000–15999, uc01 16000+, uc02 16500+ | new | verified read-only 2026-11-01 (re-check before the first import) |
| CDE service `federal-cde`, VC `federal-cde-vc` (`dex-app-bxjjm2cr`), Spark 3.5.4, tier ALLP, quota 20 CPU / 80 GiB, r5.4xlarge 2–50 nodes | `cdp de describe-vc`, `describe-service` | `EVX_CDE_VCLUSTER_ENDPOINT`; CLI profile `~/.cde/config.yaml` | renamed | verified read-only 2026-11-01 |
| CDE: Iceberg 1.5.2, `spark_catalog` Hive-backed, tables through the common helper | OGX:cde/jobs/ogx_common.py | `cde/jobs/evx_common.py` | adapted | to verify (WAP spike) |
| CDE git resource + one Spark job per stage | OGX:cde/scripts/deploy_jobs.sh | resource `rsingh-evx-pipeline`, jobs `rsingh-evx-*` | adapted | to verify |
| Airflow DAG as an airflow job; `max_active_runs=1`, `dagrun_timeout`, per-task timeout | OGX:cde/dags/ogx_dag.py; CXR:cde/dags/cxr_dag.py | `cde/dags/evx_dag.py`, `rsingh-evx-orchestration` | adapted | to verify |
| DAG → CAI job run with environment, polling, `retries=0` | CXR:cde/dags/cxr_dag.py | DAG task `cai_score_uc01` | adapted | read 2026-11-01 |
| Airflow Variables over the REST API (Knox token), own keys only | OGX:cde/scripts/set_airflow_variables.py | variables `EVX_CAI_*` | adapted | to verify |
| Sibling CDE schedules: Spend 21:00 and Churn 22:00 UTC active; OGX / MCC ended; CXR, Mule, Coll, CASA paused | `cde job list` | EVX nightly 23:30 UTC | new | verified read-only 2026-11-01 |
| Data lake Knox gateway (Atlas v2, Ranger public v2), basic auth | OGX:config/lakehouse.json `datalake_api` | `config/lakehouse.json`; `EVX_WORKLOAD_USER`, `EVX_WORKLOAD_PASSWORD` | renamed | to verify |
| Atlas Iceberg types `iceberg_table` / `iceberg_column`; Ranger `cm_tag`; masked users `federal01`, `federal07` | OGX:scripts/governance.py, config/governance.json | `config/governance.json`: `EVX_SENSITIVE_*`, `rsingh-evx-*` | adapted | to verify |
| Data lake CRN | OGX:.env `OGX_DL_CRN` | `EVX_DL_CRN` | renamed | to verify |
| S3 `s3a://federal-buk-574bcea0/data/IB/rsingh_<code>/{landing,raw,checkpoints,reports}/` | OGX:config/lakehouse.json | `.../rsingh_evx/...` | adapted | to verify |
| Database naming `<prefix>_<layer>`, `load_audit`, `transform_log`, `recon_results`, MATCHED / EXPLAINED / MISMATCH | OGX/GDL common | `rsingh_evx_{bronze,silver,master,gold,features,semantic,ref}` | adapted | to verify |
| Kafka `federal-kafka` (7.3.2, AVAILABLE), brokers :9093 SASL_SSL PLAIN, Schema Registry, SMM through Knox | OGX:.env `OGX_KAFKA_*`, `OGX_SCHEMA_REGISTRY_URL`, `OGX_SMM_API_URL` | `EVX_KAFKA_*`, `EVX_SCHEMA_REGISTRY_URL`, `EVX_SMM_API_URL` | renamed | Data Hub verified read-only 2026-11-01; ACLs for `evx.*` to verify |
| Environment root CA (PEM, outside the repo) | OGX:.env `OGX_KAFKA_CA_PEM` (absolute path) | `EVX_KAFKA_CA_PEM` | renamed | to verify |
| Broker port 9093 closed from the laptop | OGX PROJECT_LOG | Kafka work from CDE or SMM REST | lesson | lesson |
| NiFi `federal-nifi` (NiFi 2, AVAILABLE) REST and Registry through Knox | OGX:config/streaming.json `nifi` | `config/streaming.json` | adapted | Data Hub verified read-only 2026-11-01 |
| Hugging Face token, `HF_HOME` | OGX:.env | `EVX_HF_TOKEN`, `HF_HOME` | renamed | to verify |
| `cdp` CLI 0.9.163 at `~/Library/Python/3.9/bin/cdp` (not on PATH) | local | scripts use that path or `EVX_CDP_CLI` | new | verified 2026-11-01 |

## Code to copy (row per copied file; "planned" until copied)

| EVX file | Source | Adaptation | Status |
|---|---|---|---|
| `scripts/hooks/commit-msg` | OGX:scripts/hooks/commit-msg | + brand-name check on the message | copied 2026-11-01 |
| `scripts/hooks/pre-commit` | OGX:scripts/hooks/pre-commit | + brand-name check on staged files and names | copied 2026-11-01 |
| `scripts/install_hooks.sh` | OGX:scripts/install_hooks.sh | unchanged logic | copied 2026-11-01 |
| `.gitignore`, `.env.example` | OGX | `EVX_*`, `docs/private/`, `models/llm/` | copied 2026-11-01 |
| `generators/formats.py`, `extract/pdf_text.py`, `extract/ocr.py`, `extract/font5x7.py` | OGX | none expected (stdlib PDF / PNG / OCR) | planned (phase 2–3) |
| `cde/jobs/ogx_common.py` → `evx_common.py`; `ingest_bronze.py`; `recon.py`; `run_stages.py`; `stream_*.py` | OGX | EVX tables, domains, `publish()` | planned (phase 3–6) |
| `cde/jobs/build_mdm.py` → `build_master.py` | GDL | EVX entities (batch, cell, pack, vehicle, supplier) | planned (phase 4) |
| `serve/promote_champion.py` → `mlcore/promote.py` | CXR | backtest + live-stability evidence, `EVX_APPROVED_BY` | planned (phase 10) |
| `serve/registry.py` → `mlcore/registry.py` | CXR | `use_case` tags, register before deploy | planned (phase 7) |
| `serve/explain.py` → `mlcore/explain.py` | CXR | LightGBM `pred_contrib` instead of occlusion | planned (phase 7) |
| `lakehouse/score_studies.py` → `usecases/uc01_pack_risk/score.py` | CXR | per-date scoring, snapshot pin, `ref.triage_run`-style run table, PSI | planned (phase 7) |
| `cde/jobs/build_outcomes.py` → `cde/jobs/outcomes.py` | CXR | risk-ranked vs claim-driven replay, TTC, `daily_model_summary` | planned (phase 10) |
| `gate/kpi_gate.py`, `features/feature_logic.py` hash, `serve/deploy_champion.py` | OGX | → `mlcore/` | planned (phase 7) |
| `guardrails/*`, `config/guardrails.yaml` | OGX | EVX guardrails (section 10 of PLAN.md) | planned (phase 7, 10) |
| `scripts/platform_ctl.py`, `config/platform.json`, `.cursor/rules/project-start-stop.mdc` | OGX | EVX names, `resources` block | planned (phase 2) |
| `scripts/run_local.py`, `scripts/run_semantic.py`, `scripts/governance.py` | OGX | EVX names, `--steps readiness` | planned (phase 2, 5) |
| `dataviz/build_dashboard.py` | OGX | keys 15000+, per-use-case blocks | planned (phase 8) |
| `nifi/deploy_flow.py` | OGX | `EVX - Alert Router` | planned (phase 9) |
| `app/launch_app.py` | OGX / CXR | page discovery from `usecases/*` | planned (phase 8) |
| `docs/FAILED_BATCH_DEMO.md`, `sql/time_travel.sql`, `sql/adhoc.sql` | GDL | EVX tables (late EDR before / after) | planned (phase 9, 12) |

## CAI runtime lessons (carried over, enforced by `tests/test_cai_contract.py`)

| Lesson | Where handled |
|---|---|
| Job kernel runs scripts without `__file__` | `_repo_root()` with `except NameError` |
| Extra `-f <file>` argument | `parse_known_args()` |
| Any `SystemExit`, even `sys.exit(0)`, is a failed run | `sys.exit` only on failure |
| Run status `ENGINE_SUCCEEDED`; run `arguments` ignored, `environment` applied | run parameters as environment variables |
| `exec`-ing Streamlit kills the app kernel | supervising `app/launch_app.py` |
| mlflow pinned in requirements breaks `mlflow-cml-plugin` | no mlflow in `requirements.txt` |
| `parent_job_id` and `schedule` are exclusive | contract test |
| 500 after the 30 s gateway deadline on `create_model_deployment` | look up the deployment |
| Model function must be wrapped in `cml.models_v1.cml_model` (PBJ) | contract test |
| Builds not finished in 600 s are replaced (3 tries) | `mlcore/deploy.py` |
| 2 vCPU / 8 GB jobs sat 37 min; 4 vCPU never scheduled | rule 14: capacity before request size |
| Never resize a CAI instance group while engines run | rule 14 |
