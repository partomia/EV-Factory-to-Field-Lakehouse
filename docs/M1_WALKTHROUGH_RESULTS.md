# M1 walkthrough results (2026-11-02)

Automated pass using `scripts/m1_walkthrough_check.py` — the same Impala SQL, dashboard
`--check`, and HTTP probes the presenter hits in the workbenches and CDV. Log in with SSO
for the Streamlit UI; this script verifies data and availability behind login.

```bash
set -a; source .env; set +a
.venv/bin/python scripts/m1_walkthrough_check.py
```

## HTTP

| URL | Result |
|---|---|
| [evx-workbench](https://evx-workbench.federal-cml.federal.dp5i-5vkq.cloudera.site/) | HTTP 200 |
| [evx-vision](https://evx-vision.federal-cml.federal.dp5i-5vkq.cloudera.site/) | HTTP 200 |

## Quality workbench (Impala)

| Page | Result |
|---|---|
| Vehicle risk worklist | PASS — sample top VIN `XEVS1K26050600027` |
| Safety floor | PASS — 3 thermal-floor rows |
| Batch containment | PASS — 5 powder lots |
| Pack 360 | PASS — drill on `XEVS1K26050500026` |

## Vision workbench (Impala)

| Page | Result |
|---|---|
| Cell X-ray viewer | PASS — 5 FAIL rows from `semantic.media_xray` |
| Film defect gallery | PASS — 5 rows |
| EDR event replay | PASS — 5 events |

## Scores and notebook

| Check | Result |
|---|---|
| Decision counts | ABSTAIN 10,936; WORKLIST 3,247; CLEAR 3,105; SAFETY_FLOOR 575 (17,863 total) |
| Champion | logistic; schema `576d28e42713` |
| `notebooks/cai_demo.ipynb` | 13 KB on repo; synced on CAI via `evx-uc01-00-sync-code` |

## Dashboards

`python dataviz/build_dashboard.py --check` — all 19 visuals returned rows on Impala
(Factory Quality & Yield, Traceability & Containment, Fleet Reliability).

## Manual SSO steps (presenter)

1. Open both workbench URLs; confirm three quality tabs and three vision tabs load.
2. Open CDV dashboards (same Impala connection `federal-impala-1`).
3. Run `notebooks/cai_demo.ipynb` on the CAI project.
4. Optional: one `evx-uc01-service` predict call from the notebook last cell.

Follow [DEMO_RUNBOOK.md](DEMO_RUNBOOK.md) for narrative and timings (~20 min).

## On CAI (after sync)

```bash
python3 ci/setup_cai.py --run evx-uc01-00-sync-code
python3 ci/setup_cai.py --run evx-walkthrough-check
```

Job **`evx-walkthrough-check`** runs **`ci/run_walkthrough.py`** (Impala checks + notebook/baseline; HTTP and
CDV `--check` run from the laptop). Results: **`outputs/walkthrough.log`** on the project.
