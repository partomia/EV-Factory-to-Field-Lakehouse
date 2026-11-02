# M1 demo runbook (~20 minutes)

Before and after the session: README and `scripts/platform_ctl.py status`, `start`, and `stop`
afterwards. Data is synthetic; do not present it as a customer's production lakehouse.

## Links

| What | Where |
|---|---|
| Quality workbench | https://evx-workbench.federal-cml.federal.dp5i-5vkq.cloudera.site/ |
| Vision workbench | https://evx-vision.federal-cml.federal.dp5i-5vkq.cloudera.site/ |
| Dashboards (Data Visualization) | **EVX Factory Quality & Yield**, **EVX Traceability & Containment**, **EVX Fleet Reliability** (connection `federal-impala-1`, keys 15000+) |
| CAI project | `rsingh-ev-factory-to-field` — notebook `notebooks/cai_demo.ipynb`, jobs `evx-uc01-*`, model `evx-uc01-service` |
| CDE | jobs `rsingh-evx-*`, DAG `evx_factory_to_field` (paused) |
| Hue / Impala | databases `rsingh_evx_*` |
| Repo | https://github.com/partomia/EV-Factory-to-Field-Lakehouse |

Sync the CAI project after a git push: `python ci/setup_cai.py --run evx-uc01-00-sync-code`.

## 1. The problem (2 min)

A quality engineer must decide which vehicles to hold before dispatch when evidence is split across
SAP batches, cell MES, X-ray and film PNGs, pack genealogy, field DTCs and EDR dumps — each
system naming the same cell or pack differently. Today they review in calendar order.

## 2. Risk-ranked worklist (4 min): Quality workbench → Vehicle risk worklist

- Open **EVX Quality and Reliability Workbench** → **Vehicle risk worklist**.
- Top rows: high calibrated `pack_risk_score`, `decision` WORKLIST, `action` always **NONE** (no
  automated dispatch hold).
- Filter a **safety-floor** vehicle: thermal DTC or EDR on or before the score date → P1 context
  even when the model score is low (575 vehicles on the history population).
- **Abstain** band: most of the population sits between the worklist and clear thresholds after
  isotonic calibration; one open genealogy (planted orphan) abstains.
- Counts to quote: worklist 3,247; clear 3,105; abstain 10,936; safety floor 575 (17,863 scored).

## 3. Containment (3 min): Batch containment

- Pick a powder or cell batch linked to worklist or safety-floor vehicles.
- Show supplier context, batch-level defect rates, and which packs tied to the batch are on the
  worklist — containment before field release, not after warranty claims.

## 4. Pack 360 (4 min): structured + unstructured context

- Select a worklist VIN (e.g. one with high X-ray score from the overhang cause).
- **52 cells** graded, wire-bond minimum, pack EOL, genealogy to vehicle.
- X-ray verdicts and scores; optional PNG with overhang box when landing bytes are reachable.
- 90-day telemetry window and field events on the same vehicle.

## 5. Vision workbench (3 min): images and EDR

- **EVX Vision and Documents Workbench** → **Cell X-ray viewer**: FAIL verdicts first, overhang
  box on the PNG when S3/landing read works; structured row still shows score and pack context.
- **Film defect gallery**: streak and patch metrics from `silver.qc_image_metric`.
- **EDR event replay**: metadata and safety-floor flag (time-series chart is M2).

## 6. Dashboards (2 min)

- **Factory Quality & Yield**: FPY and inline QC rates by line and shift.
- **Traceability & Containment**: batch → pack → vehicle drill, aligned with the workbench batch page.
- **Fleet Reliability**: FIR90 and matured positives vs the 90-day label window.

## 7. Model selection and guardrails (2 min): CAI notebook + README_CAI

- Open **`notebooks/cai_demo.ipynb`** on the CAI project (synced via `evx-uc01-00-sync-code`).
- Walk through: three candidates → **logistic wins on OOF PR-AUC** → gate PASS → rejection paths
  (stricter gate, CI salt, AUROC regression).
- Optional live call to **`evx-uc01-service`** when keys are set; compare to `gold.uc01_pack_risk_score`.
- **`README_CAI.md`**: deploy rules and what register/deploy refuses.

## 8. Close (1 min)

Everything is in a public repo (no secrets in git; gitleaks hook), one commit per phase, live
steps in `docs/PROJECT_LOG.md`. End the session with:

```bash
set -a; source .env; set +a
python scripts/platform_ctl.py stop
python scripts/platform_ctl.py status   # apps stopped, model undeployed, evx schedules cleared
```

## If something is off

- Workbench error on every tab: missing `EVX_IMPALA_USER` / `EVX_IMPALA_PASSWORD` in the CAI
  project environment → set them and restart the application (~30 s).
- First tab load is slow (Impala cold); later loads use Streamlit cache (~5 min TTL).
- Vision PNG missing but table present: landing/S3 bytes not reachable from the CAI session;
  metadata and Impala joins still demo the medallion path.
- Model endpoint errors: workbench reads scores from Impala; endpoint is optional for the demo.
- Dashboard empty: re-run `python dataviz/build_dashboard.py --check` from a machine with
  `EVX_VIZ_API_KEY` and Impala credentials.
