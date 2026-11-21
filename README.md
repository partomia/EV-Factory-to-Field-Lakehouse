# EV Factory-to-Field Lakehouse

Synthetic EV battery factory-to-field lakehouse on Cloudera (CDE, Impala, CAI, Data Visualization).

| Topic | Where |
|---|---|
| Build plan and milestones | [PLAN.md](PLAN.md) |
| CAI model selection, gate and deployment | [README_CAI.md](README_CAI.md) |
| CAI demo notebook (train → gate → production) | [notebooks/cai_demo.ipynb](notebooks/cai_demo.ipynb) |
| Quality workbench (Streamlit on CAI) | subdomain `evx-workbench` on the federal CAI workspace |
| Vision & documents workbench (images, EDR) | subdomain `evx-vision` — `app/launch_vision_app.py` |
| Dashboards (Factory, Traceability, Fleet) | `dataviz/build_dashboard.py` |
| Sync CAI project from git | `python ci/setup_cai.py --run evx-uc01-00-sync-code` |
| M1 walkthrough on CAI | `python ci/setup_cai.py --run evx-walkthrough-check` → `outputs/walkthrough.log` |
| M1 presenter runbook | [docs/DEMO_RUNBOOK.md](docs/DEMO_RUNBOOK.md) |
| Start / stop the live demo | `python scripts/platform_ctl.py status` then `start` or `stop` |
| M1 walkthrough check (Impala + CDV) | `.venv/bin/python scripts/m1_walkthrough_check.py` — [results](docs/M1_WALKTHROUGH_RESULTS.md) |

**Status (2026-11-20):** live on the closed window through 2026-11-10. Both CAI apps and `evx-uc01-service` are
up, NiFi `EVX - Alert Router` is running (hold processor stopped) and the six streaming jobs are running from their
checkpoints. The nightly schedule is armed but does not fire: its window ended 2026-11-09 23:30 UTC. Recon with M2
built passes for 2026-11-06 through 2026-11-10 (runs 1326, 1327, 1324, 1323, 1325); fault 9 now counts
DMS VINs, not candidate pairs. Fault 9 re-checked on 2026-10-28 (1328) and 11-07
(1329). The vision workbench launcher installs boto3 for image bytes and reads S3 with IDBroker credentials
for the workload user (the data lake uses RAZ), fetched with a Knox token because `aws-cab` rejected
basic auth with HTTP 401; a failed read shows its cause on the page. `dim_vehicle` was refreshed to 18,337 rows
without another warehouse restart. Stop with `platform_ctl.py stop`.

Data is synthetic. Do not present it as a customer's production data.
