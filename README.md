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
| Sync CAI project from git | `python ci/setup_cai.py` then job `evx-uc01-00-sync-code` |

Data is synthetic. Do not present it as a customer's production data.
