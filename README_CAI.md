# CAI: how pack_risk_eol was selected, and what was rejected

This is the model-selection record for the Cloudera AI deployment of use case 1. The code that
does it is `usecases/uc01_pack_risk/train.py` and `mlcore/gate.py`. The demo notebook that
replays the choice, including a gate that says no, is `notebooks/cai_demo.ipynb`. The workbench
that shows the result is the CAI application **EVX Quality and Reliability Workbench**.

The population is synthetic. These figures are the synthetic history scored on 2026-11-05, not
a customer's production model.

## What "selected" means

A model is selected only when all of the following are true. Deployment is a later step and
does not get a vote in the choice.

1. The training rows are matured vehicles with complete genealogy. An open genealogy abstains
   at score time and is not a training row.
2. Packs are split by their majority cell batch. The latest fifth of batches is TEST. TEST is
   not used to pick the model, the calibration, or the thresholds.
3. Three candidates are fit inside each of five batch-grouped folds. The winner is the one with
   the highest pooled out-of-fold PR-AUC. AUROC is reported and is a gate, not the selection
   metric.
4. Isotonic calibration and the two thresholds are fit on the winner's out-of-fold scores only.
   The worklist threshold is the 90th percentile of those scores. The abstain threshold is the
   70th percentile (worklist 10% plus an abstain band of 20%).
5. `mlcore.gate.decide` returns no errors against `config/lakehouse.json` `model.gate`, and the
   out-of-fold PR-AUC has not dropped more than 0.02 below `usecases/uc01_pack_risk/baseline.json`.
6. The feature salt is `platform`. A `ci` salt can train and can pass the numeric gate, and it
   is still not deployable. `artifact["deployable"]` is true only when the salt is `platform`
   and the gate is empty.

Registration (`mlcore/registry.py`) runs before the model build. The same MLflow run does not
create a second registry version.

## The three candidates

Scored on 7,675 TRAIN packs (646 positives) and reported, not used for selection, on 1,983 TEST
packs (217 positives). Prevalence on TRAIN is 8.4%.

| Candidate | Out-of-fold PR-AUC | Out-of-fold AUROC | Result |
|---|---:|---:|---|
| logistic regression | 0.263 | 0.695 | selected |
| histogram gradient boosting | 0.212 | 0.657 | rejected, lower PR-AUC |
| LightGBM | 0.210 | 0.656 | rejected, lower PR-AUC |

Logistic wins because the planted factory effects are nearly linear. The gradient boosters are
not retuned to overturn that. Selection is one line: `winner = max(ranking, key=ranking.get)`
where `ranking` is out-of-fold PR-AUC.

The selected model then records: PR-AUC lift 3.13 over the 8.4% base rate, calibration error
0.006, mixed packs 9.6% (limit 15%), at least 122 positives in every fold. Schema hash
`576d28e42713`, salt `platform`. Worklist threshold 0.139, abstain threshold 0.051.

## The gate, and a rejection

`decide` fails the run when any of these is true. The limits live in `config/lakehouse.json`.

| Check | Limit | This champion |
|---|---|---|
| TRAIN positives | ≥ 100 | 646 |
| TEST positives | ≥ 100 | 217 |
| positives in the smallest fold | ≥ 100 | 122 |
| out-of-fold AUROC | ≥ 0.65 | 0.695 |
| out-of-fold PR-AUC lift | ≥ 1.5 | 3.13 |
| calibration error | ≤ 0.08 | 0.006 |
| mixed packs | ≤ 0.15 | 0.096 |
| PR-AUC versus the stored baseline | drop ≤ 0.02 | equal to the baseline |

A failed gate still writes `outputs/uc01/pack_risk_eol/model.joblib` and prints one
`GATE_FAIL` line per check. `deployable` stays false, and `--write-baseline` does not replace
`baseline.json`. Nothing is registered and nothing is deployed.

Two rejections that are not a worse PR-AUC:

- **Too little data on the CI salt.** If matured vehicles are below 100 and the salt is not
  `platform`, training stops before a model is fit and prints `GATE_RESULT SKIPPED`. That run
  is not deployable.
- **A platform-looking gate on the wrong salt.** The numeric checks can pass on salt `ci`.
  Deployability is still false, and the scorer refuses the file (`ScoreRefused`) because the
  schema hash was computed with the other salt.

The notebook rebuilds a stricter gate in memory (AUROC floor 0.90) and shows it fail. That
does not change `config/lakehouse.json`.

## What deployment will not accept

These are refused after selection. They are not a reason to pick a different candidate.

- A model with `deployable` false, or a salt other than `platform`.
- A feature column list or schema hash that does not match the factory table.
- Any scored row whose `action` is not `NONE`. The service has no automated action.
- A build whose runtime was chosen as "the last image with version `2026.08.1-b5`". That
  version is shared by every runtime in the release, including Scala. The build must use the
  pinned Python 3.11 image in `ci/cai_jobs.py`.
- A replica that does not install `requirements-model.txt` inside the image. Packages the
  training job pip-installed are not in the model image. `cdsw-build.sh` installs the pinned
  wheels. Without them the replica stays up and every call returns `No module named 'joblib'`
  with `action` NONE.

The deployed service is `evx-uc01-service`, registry model `evx-uc01-pack-risk-eol` version 1,
1 vCPU / 2 GB. One call for a known vehicle returns the same decision, score, schema hash and
`action` NONE as `gold.uc01_pack_risk_score`.
