# Project log

## 2026-11-01 09:29 IST - Approvals

- The owner approved the whole APPROVALS NEEDED list (M1–M3) and the open questions: 52 cells per pack and the
  45 → 90 packs/day ramp; the decoupled business calendar; the 23:30 UTC nightly window; Qwen3-4B (fallback
  Phi-4-mini); `evx.*` Kafka rights assumed and tested live.
- `EVX_BRAND_NAME` set by the owner in `.env`. The history and tree were checked: the value is in no tracked file
  or commit message.
- **Sibling CAI resources may be turned off when `evx` needs the capacity** (owner, 2026-11-01). Logged here
  whenever done.
- **OGX: only the skill commits** (`tools/cursor-skills/cloudera-usecase-builder/`). Nothing else in OGX is changed
  or committed, including `docs/USE_CASE_PLAYBOOK.md`. **Plan change:** the prompt's Step 10 playbook update is
  dropped.
- Pushed `3551fc2` (plan, inventory, hooks) to the public repo.

## 2026-11-01 09:31 IST - Shared-capacity check (read-only)

- CDE `federal-cde-vc`: no run running or starting; sessions all killed. VC quota 20 CPU / 80 GiB.
- CAI `federal-cml-ws`: apps running: Churn (2), Spend, OGX workbench, MCC; no model deployment running.
- CDE schedules active: Spend 21:00 UTC, Churn 22:00 UTC.

## 2026-11-01 09:35 IST - WAP spike

- **CDE repository** `rsingh-evx-pipeline` (git, public repo) created and synced. **CDE job**
  `rsingh-evx-wap-spike` (driver 1 / 2 GB, 1 executor 1 / 2 GB).
- Run **980** (`--mode run`): about 2 min in starting, 2 min running, succeeded. All 11 Spark checks PASS: branch
  create, `spark.wap.branch` writes, `branch_<b>` writes, `main` isolation, audit on the branch,
  `fast_forward`, WAP-ID cherry-pick, `overwritePartitions` on a branch (both ways).
- Impala: `main` after the fast-forward = 142 (matches Spark). An unpublished branch is readable **by snapshot
  id only**, not by branch name.
- Atlas: the `iceberg_table` entity is ACTIVE with its create lineage; branch writes and procedures add no
  lineage edges.
- Run **981** (`--mode drop`): scratch table dropped; Impala shows no tables in `rsingh_evx_ref`.
- **Decision:** (a) branch WAP with the audit in Spark, behind `publish()`. Details in `docs/WAP_SPIKE.md`.
- Lesson: a warehouse-wide `INVALIDATE METADATA` on the shared Impala hung the client. Use only table-scoped
  `INVALIDATE METADATA <db>.<table>` or `REFRESH <db>.<table>`.
- Lesson: `cde run logs` defaults to `driver/stderr`. Job output printed with `print()` is in
  `--type driver/stdout`, and it can arrive as one long line with carriage returns.

## 2026-11-01 12:10 IST - Generator on CDE (default scale)

- Local CI scale (0.05): 1.6k packs, 21.6k objects, deterministic across runs; the Spark path (`cde/jobs/generate.py`)
  writes byte-identical objects to the plain runner. 11 unit tests pass; GitHub Actions green.
- Fixed before the first CDE run: cell production stopped 12 days before the last live date (no cell files on the
  live dates); material lots all started on the same days (no goods receipt on 2026-11-08, so the schema-change
  field had no rows). Late contamination and drift episodes were moved into the matured window (the old July 20
  / 27 episodes delivered after the 2026-08-07 label cutoff); one unmatured episode of each added in September.
- **CDE job** `rsingh-evx-generate` (driver 2 / 8 GB, 3 executors 2 / 8 GB: 8 cores / 32 GB). Headroom checked
  first: no active runs; 05:40 UTC, clear of the 21:00 / 22:00 UTC sibling schedules.
- Run **982** (logit base −3.9): succeeded, 1,231 s. 18,335 packs, 9,658 matured, 760 positives = **7.87 %**, below
  the 8–10 % band and the 800-positive floor.
- Run **983** (logit base −3.74, CSV exports repartitioned by date, manifests from a thread pool): succeeded,
  996 s. **18,335 packs, 9,658 matured, 863 positives = 8.94 %**; by cause contaminated 108, drift 110, overhang
  169, weak bond 81, usage 395. Every planted cause has positives in July deliveries (the TEST window).
  Objects 74,489 + 4,111 manifests under `rsingh_evx/generated/`: X-ray PNG 40,220, job-card PDF 15,222, cell MES
  6,528, formation curves 3,281, EDR 1,775, cell QC 1,650, pack NDJSON 1,578, vehicle NDJSON 1,572, OData 1,103.
  Truth: `rsingh_evx_ref.generator_truth_pack` 18,335, `generator_truth_batch` 1,650, `generator_fleet` 17,646.
- Lesson: the streamed `driver/stdout` log lost the final result lines (and cut one long line at ≈ 1.2 KB). The
  complete log is in `cde run logs --id <n> --download-all`.
- Lesson: Spark CSV `partitionBy(date)` without a repartition committed 5.7k part files one by one (minutes).

## 2026-11-01 13:25 IST - Regenerate (film noise) and land history

- Local bronze on CI scale found every blank electrode-film PNG flagged `DUPLICATE` (identical bytes). Film images
  now carry sensor noise. Run **984** (`rsingh-evx-generate`, same arguments as 983) regenerated; succeeded at
  07:21 UTC with the same counts and truth as 983.
- **CDE job** `rsingh-evx-land` (driver 2 / 4 GB, 1 executor 1 / 2 GB). Run **985** (`--from 2026-02-09 --to
  2026-11-05`): succeeded, 07:21 to 07:48 UTC. **270 dates, 73,565 files, 2,291 MB** copied to
  `rsingh_evx/landing/`, each date with a `_manifest.json` (counts, sizes, SHA-256).
- Local extract (phase 3b) on the CI windows: every catalogued object extracted (130 live, 207 September),
  5 image-only job cards read by OCR on 2026-11-08 including the pack-swap card, the truncated PNG not extracted.
- Lesson: unit tests on the generator's own images found two metric bugs the sample run hid: the film defect is a
  drawn frame (≈80 bright pixels), not a filled patch, and a speckled can-wall pixel widened the X-ray box. Test
  extractors against generated objects for every planted case, not only on a sample.
- Lesson: a CDE job on a git repository resource takes `--application-file` relative to the repository
  (`cde/jobs/x.py`), not the `/app/mount/...` path; the absolute path fails with `object not found in the tree`.

## 2026-11-01 14:40 IST - Bronze and extract history on CDE

- Repository synced at `84fb1cc`. No active runs before either submission.
- **Bronze run 986** (`rsingh-evx-bronze`, 2026-02-09..11-05): succeeded, 07:45 to 08:24 UTC (1,772 s of job
  time; the object fingerprint pass over 2.3 GB took 643 s). MES 10,114,966 in / 10,114,249 ok / 717 quarantined
  (the planted 2026-09-15 trailer mismatch, the only MISMATCH of 8,028 controlled files); QC 1,032,581;
  formation curve points 4,751,400; telematics 1,926,341; OData 11,544; pack / vehicle documents 17,963 / 17,863;
  warranty 1,291. Objects 57,177: 57,176 VALID, 1 `PNG_TRUNCATED` (the planted 2026-10-12 X-ray). Impala
  counts (table-scoped `INVALIDATE METADATA`) equal the Spark counts; quarantine 718.
- **CDE job** `rsingh-evx-extract` (driver 2 / 8 GB, 3 executors 2 / 8 GB). **Run 987** (same window): succeeded,
  08:27 to 08:50 UTC. doc_extract 15,045 (all text layer; the image-only cards are live-only), qc_image_metric
  40,302, spectrum_feature 181, edr_event 1,648 = 57,176, equal to the VALID objects; no FAILED rows.
- Extraction against the generator's QC records: an overhang blob in every image the generator drew one in
  (9,157 FAIL + 110 sub-threshold), box identical in all 9,267, no blob in 30,430 clean images. XRF Fe peak
  274–380 on exactly 6 lots (the 6 contaminated lots) against 17–56 elsewhere. Films: 106 of 605 with a defect frame.
- Lesson: `blob` and `fields` are reserved words in Impala; quote or rename them in ad-hoc SQL.
- Lesson: `cde run logs --download-all` hung once after a long run; the streamed stdout had the short marker
  lines, so print results as short marker lines and fall back to the streamed log.

## 2026-11-01 19:45 IST - Silver on CDE; generator id collisions found; master locally

- **CDE job** `rsingh-evx-silver` (driver 2 / 8 GB, 3 executors 2 / 8 GB). **Run 988** (2026-02-09..11-05):
  succeeded, 12:51 to 13:03 UTC (520 s). 19 tables, no `_dq_issues` rows: cell_step 10,114,249, qc_inspection
  1,032,581, formation curve points 4,751,400, telematics 1,926,341, pack_build 17,963, pack_cell 934,076,
  vehicle_build 17,863, service_job 14,337, supplier_coa 604, quality_report 104, six SAP entity tables.
- The per-key dedup dropped rows it should not have: dtc_event 35, warranty_claim 1, sap_inspection_lot 16.
  Impala showed the cause: generator ids built from `vin[-9:]` repeat between pack lines 1 and 2 (DTC, EDR, job
  card, claim, teardown ids), and inspection-lot numbers repeat across materials with the same lot suffix and
  day. Registrations (4 random digits per city), rider ids and motor / MCU serials could collide too. All are now
  unique by construction (line digit + EOL date + sequence; material index in lot numbers);
  `test_source_keys_unique_per_date` parses every generated file with the bronze parsers and fails on the old
  generator. The silver results of run 988 are superseded by the regenerate below.
- Planted date-keyed faults are placed once per generator partition, so a date can hold several (CI scale: 3 VIN
  typos, 5 image-only cards, 2 orphan cells). The generator now records each instance
  (`ref.generator_truth_fault`); the local checks compare catches with that list.
- **Master** (`cde/jobs/build_master.py`) locally over the CI windows 2026-09-01..09-16 and 2026-10-24..11-08
  (wide enough to hold the cells behind their packs): every planted orphan cell, VIN typo (REVIEW, not merged)
  and pack swap (two link versions per vehicle, removed pack verified) caught, nothing else; `check_local`
  bronze, extract, silver, master OK; 25 unit tests pass. Pushed `01d39a1`.
- Regenerate chain started on CDE (ids changed, so every layer is rebuilt): generate run **989** at 14:14 UTC, no
  active runs before it.
- Lesson: a per-key dedup that reports what it dropped is a cheap generator test; a non-zero count on synthetic
  data with no planted duplicates is a bug upstream, not noise.

## 2026-11-01 21:20 IST - Regenerate and land on CDE; gold, semantic and SDX locally; SDX tags and masks live

- **Generate run 989**: succeeded, 14:14 to 14:33 UTC (1,040 s): 74,489 objects, 4,111 manifests; truth 18,335
  packs, 1,650 batches, 17,646 fleet rows, **15 planted fault instances** (`ref.generator_truth_fault`), label
  rate 0.0894 (863 of 9,658 matured).
- **Land run 990** (2026-02-09..11-05): succeeded, 14:34 to 15:18 UTC (2,606 s): 270 dates, 73,708 files,
  2,291 MB. Bronze run **991** started 15:40 UTC.
- **Gold** (`build_gold.py`, every table through `publish()`: Spark audit of null / duplicate keys and SQL
  checks, then one commit): 5 dimensions, 3 SCD2 dimensions, the genealogy bridge, 11 facts. Locally over both
  CI windows the gold checks pass: one current version per key and unbroken validity chains, current packs after
  swaps equal the planted swaps, MISSING images equal the corrupt PNGs, ORPHAN_CELL packs equal the orphans, cell
  production day and shift equal the generator's batch truth. Pushed `1b28dd9`, CI green. CDE job
  `rsingh-evx-gold` created (driver 2 / 8 GB, 3 executors 2 / 8 GB).
- **Semantic** (`sql/semantic/`, `scripts/run_semantic.py`): `kpi_fpy_cell`, `kpi_fpy_pack`,
  `kpi_fir90_build_week`, `kpi_fir90_supplier_batch` and three base views; the consistency check compares each
  KPI total with an independent count on gold and the two FIR90 cuts with each other. Local: all 11 checks OK,
  cell FPY 0.9896, pack FPY 1.0; FIR90 has no matured vehicles in the CI windows (it is exercised on CDE).
  Pushed `4c0e851`.
- **SDX** (`scripts/governance.py`, adapted from OGX): `apply --only typedefs,masking` created
  `EVX_SENSITIVE_PII` / `_CONTACT` / `_LOCATION` and Ranger `cm_tag` policies `rsingh-evx-sensitive-pii`
  (MASK_HASH), `-contact` (MASK), `-location` (MASK_NULL) for `federal01`, `federal07`; then 16 columns tagged in
  bronze `dms_riders` and silver `rider`, `service_job`; `verify` OK. Master, gold and semantic columns and the
  glossary follow after their CDE runs. Pushed `881b71e`.
- Lesson: a fact derived from an earlier step (cell production day from the mixing step) is unknown when that
  step's date is outside the loaded window; local windows hit this, the full history does not. Check "known or
  source not loaded" instead of "not null", and keep the KPI to rows whose grain is known.
- Lesson: `writeTo().overwritePartitions()` does not add new columns to an existing Iceberg table; added
  `add_new_columns` (ALTER TABLE ADD COLUMNS) before date writes and appends, the additive (minor) contract change.
- Lesson: `cde run logs` stdout lags the run's end by about 20 s; the marker lines were missing on the first read.

## 2026-11-01 22:35 IST - Bronze and extract rebuilt on CDE; recon; telemetry stream live on Kafka

- **Bronze run 991** (2026-02-09..11-05): succeeded (1,716 s). MES 10,114,249 rows, 717 quarantined (the one
  planted trailer-count file, 09-15); `doc_object` 57,320 objects, 1 `PNG_TRUNCATED` (the planted 10-12 PNG).
- **Extract run 992**: succeeded, 16:12 to 16:39 UTC (1,570 s), 270 dates: `doc_extract` 15,188,
  `qc_image_metric` 40,302, `spectrum_feature` 181, `edr_event` 1,648, each equal to its source count.
- **Recon** (`cde/jobs/recon.py`, `config/expected_recon.json`): bronze to silver to gold counts and warranty sums,
  every exception matched to a planted fault (EXPLAINED / MISMATCH with a fault id) or failing the run
  (UNEXPECTED, MISSING). Local over both CI windows OK; pushed `b0caaee`. Job `rsingh-evx-recon` created.
- **Telemetry stream** (Phase 6, pushed `06efc93`): seeded producer for the live subset (planted degrees-F firmware
  from 11-08, stuck SoC on 11-09), per-minute control totals on `evx.control`; streaming bronze with one Iceberg
  commit per micro-batch (snapshot property `evx.stream-batch`, a replayed batch is skipped) and a quarantine
  table; 15-minute windows per VIN (10-minute watermark, degrees F normalised) MERGEd into
  `silver.telemetry_window`. Local over 11-06..11-09: 268,994 messages, 0 quarantined, `check_local stream` OK.
- **Live on CDE / Kafka**: `deploy_streaming.sh create` (jobs `rsingh-evx-stream-telemetry`, `-stream-agg`,
  `-stream-produce`). Produce run **993** with `--setup`: topics `evx.vehicle.telemetry` (6 partitions) and
  `evx.control` **CREATED** (the workload user holds create / write / read on `evx.*`, tested live), then 283,706
  messages for 2,084 live vehicles on 11-06 in 16 s. Bronze drain **994**: one batch, 282,924 readings + 782
  control, 0 quarantined. Window drain **995**: 12,938 windows. Impala: 0 minutes whose control total differs from
  bronze, every event id unique, window `sum(n)` = 282,924, no degrees-F or stuck-SoC windows (none planted on
  11-06), max cell temperature 44.3 C.
- Silver run **996** (2026-02-09..11-05) started 17:04 UTC.
- Lesson: Kafka on 9093 is closed from the laptop; produce from a CDE Spark job (`df.write.format("kafka")`) and
  create topics with the Kafka AdminClient over py4j from the same job, so no client or CA is needed locally.
- Lesson: a drain of two chained available-now streams must run in order; the window job started alongside the
  bronze job would see an empty bronze table and finish with nothing. `drain` now waits for each run.

## 2026-11-02 00:25 IST - History chain on CDE; semantic KPIs and the glossary live

- **Silver run 996** (2026-02-09..11-05): succeeded, 17:03 to 17:19 UTC (756 s), 270 dates, no unmapped documents.
- **Master run 997** (as of 2026-11-05): succeeded, 17:32 to 17:40 UTC (300 s). 1,022,861 cells, 934,076 genealogy
  links (1 ORPHAN), 17,963 packs (1 ORPHAN_CELL), 17,863 vehicles (1 REVIEW, 17,171 MATCHED, 691 NO_DMS),
  identity review VIN_EDIT and NO_SUCH_CELL.
- **Gold run 998**: succeeded, 17:41 to 17:49 UTC (415 s), 270 dates. `fact_cell_process` 10,114,249,
  `fact_cell_grading` 968,517, `fact_pack_eol` 17,963, `fact_vehicle_daily` 1,926,341.
  `fact_telemetry_window` is empty: telemetry starts 2026-11-06, after this window.
- **Recon run 999** failed: `ref.schema_change` does not exist, because bronze creates it only when a new field
  is seen and the planted one (`ZZ_MOISTURE_PPM`) is on 2026-11-08. Fixed in `7201c05` (skip a missing exception
  source; bronze now creates the empty table). **Recon run 1000** succeeded, 18:11 to 18:17 UTC (321 s):
  4,489 MATCH, 2 EXPLAINED, 2 MISMATCH, 1 INFO, 0 failing. The four history faults: trailer 09-15 (fault 2),
  corrupt PNG 10-12 (fault 3), orphan cell 10-20 (fault 5), VIN typo 10-28 (fault 9).
- **Semantic on Impala** (`run_semantic.py`): 7 views. All 11 checks OK. Cell FPY 0.9700 (939,453 of 968,517),
  pack FPY 0.9997 (17,957 of 17,963), FIR90 53.53 per 1,000 (517 of 9,658 matured vehicles). The week cut and
  the supplier-batch cut agree.
- **SDX** `governance.py apply`: 10 more columns tagged (gold rider and service job, master and gold vehicle
  registration) and glossary "EVX Data Products" (5 domain terms on the gold tables, 3 KPI terms on the semantic
  views). `verify` OK. Masking policies were already in place, so the new tags are masked for federal01 and federal07.
- **Against generator truth** (history only): cells 1,022,861 = sum of batch truth; cell production date and shift
  disagree on 0 batches; pack EOL result disagrees on 0 packs; the history orphan and the history VIN typo match
  exactly. Truth lists 17,957 packs in the window and gold has 17,963: the 6 extra are the EOL failures, which the
  generator does not write into `generator_truth_pack`.
- Lesson: an exception table created only when the exception happens is absent on a window that predates it, and a
  recon that queries it unconditionally fails before it can report anything. Skip a missing source (a fault whose
  date is inside the window still comes out MISSING) and create the empty table on every run.

## 2026-11-02 00:40 IST - Factory features for pack_risk_eol on CDE

- **Feature job** `rsingh-evx-uc01-features` run **1001**: succeeded, 18:58 to 19:04 UTC.
  `features.pack_factory_v1` has 17,863 vehicles (one per current vehicle, every pack linked to 52 cells) and no
  label columns. `gold.uc01_pack_eol_example` carries the label. Hash salt `platform` (CI uses `ci`, so a
  laptop-built hash cannot be served). Pushed `5cdc9f0`.
- Matured vehicles 9,658, the same count as FIR90. Positives among them: **863**, the generator's planted label
  rate (0.0894). 1,071 vehicles have a battery event inside 90 days including those not yet matured.
  Genealogy open: 1, the planted orphan. Local CI run: 191 rows, salt `ci`, checks passed; 38 tests pass.
- The safety floor at EOL is 0. A powder lot is built before any vehicle from it has failed in the field, so a
  sibling's thermal event is never known at this vehicle's EOL. Scoring will apply the floor from thermal events
  known at the score date. It is not a feature.
- Lesson: the shared feature table and the label live apart, and a test checks that the factory SQL names no
  outcome table (`fact_dtc`, warranty, EDR, telematics).

## 2026-11-02 08:30 IST - pack_risk_eol trained; the gate passes

- Feature rebuild **1016** (02:50 to 02:55 UTC) replaced the X-ray and film features. The first version used the
  sampled image's pixel overhang and joined film rows on cell id, and neither moved. The inspection score does:
  X-ray score max separates the overhang cause (AUROC 0.94) and the batch film score separates drift (0.99).
  K-value separates contamination (0.99) and minimum wire-bond pull separates the weak bond (0.97). Usage, 395 of
  the 863 positives, has no factory trace.
- **Train** on the 9,657 matured packs with complete genealogy (the one orphan abstains). Split by majority cell
  batch, latest 196 batches are TEST (1,983 packs, 217 positives), 785 batches are TRAIN (7,675 packs, 646
  positives, at least 122 in each of 5 folds). Mixed packs 9.6 %. Every planted cause has positives on both sides.
- Out-of-fold PR-AUC: logistic 0.263, histogram gradient boosting 0.212, LightGBM 0.210. **Logistic wins.**
  AUROC 0.695, PR-AUC lift 3.13 over the 8.4 % base rate, calibration error 0.006. The gate passes
  (`usecases/uc01_pack_risk/baseline.json`, schema hash `576d28e42713`). Worklist threshold 0.139 (top 10 % of
  calibrated out-of-fold scores), abstain band down to 0.051. The model file stays in
  `outputs/uc01/pack_risk_eol/` and is not in git. A CI-salt run cannot be deployed: the hash salt differs.
- Pushed `a31733a`.

## 2026-11-02 08:45 IST - pack_risk_eol scored; the M1 guardrails are on the history population

- Scored all 17,863 vehicles in `features.pack_factory_v1` as of 2026-11-05 with the logistic champion
  (schema `576d28e42713`, salt `platform`). The model file stays in `outputs/` and is not in git. The scored
  parquet was uploaded as the CDE files resource `rsingh-evx-uc01-scores`. Job `rsingh-evx-uc01-score` run
  **1019** published it: `gold.uc01_pack_risk_score` 17,863 rows, `gold.model_score` 17,863 rows,
  `ref.guardrail_event` with one `feature_hash` PASS for the run. Impala matches. Every `action` is `NONE`.
- Decisions: worklist 3,247 (score at or above 0.139), clear 3,105 (below 0.051), abstain 10,936, safety floor
  575. The abstain split is the band 10,814, out-of-distribution 121, and the one open genealogy (the planted
  orphan). The 575 safety-floor vehicles each had a thermal DTC or a thermal EDR on or before the score date;
  they are P1 whatever the model score. That floor is not the end-of-line sibling flag, which is still 0.
- The abstain band holds most of the population because isotonic calibration piles scores onto a few
  probabilities. The median calibrated score is 0.051 and the 80th and 90th percentiles are both 0.139, so
  the plateau between the two training thresholds is the band. The thresholds stay the training quantiles.
- Run 1017 failed because a files resource mounted at prefix `/scores` is at `/app/mount/scores/`, not
  `/scores/`. Run 1018 committed the score table and then failed the audit: `publish()` only accepts its
  declared transform types, and the write happens before that check. Run 1019 used `enrich` and succeeded.
- Pushed `ecd0636`, `0dae11e`.

## 2026-11-02 11:00 IST - pack_risk_eol registered and deployed

- CAI project `rsingh-ev-factory-to-field` (private, from the public git URL). Registry model
  `evx-uc01-pack-risk-eol` version 1, experiment `evx-uc01`. The same MLflow run does not create another version.
  `ref.model_event` is still unwritten: the promotion job is the writer, and that job does not exist yet.
- Service `evx-uc01-service` is deployed at 1 vCPU / 2 GB (build `d96b378c`, deployment `737799a5`, Python 3.11).
  One call for VIN `XEVS1K26052000026` returned ABSTAIN / `abstain_band` / score 0.0513 / schema `576d28e42713` /
  `action` NONE, the same row as `gold.uc01_pack_risk_score`.
- The first builds used the PBJ Scala 2.12 workbench. `ML_RUNTIME_FULL_VERSION` is the short release
  `2026.08.1-b5`, shared by every image in that release, and the last match was Scala. A 2 GB replica of that
  image failed in about 30 s; a 4 GB one stayed `deploying` with `updated_at` advancing. The Python 3.11 image
  deploys at the default 1 / 2. The runtime picker now keeps the pinned Python image.
- The first Python replica stayed up and answered `No module named 'joblib'`. The job's pip install lands in
  `~/.local`, which the model image does not use, and a build with no `cdsw-build.sh` finishes without installing
  `requirements-model.txt`. `cdsw-build.sh` now installs those pinned wheels.
- Sibling CAI resources stopped so this replica could be placed (owner approval, 2026-11-01). Applications:
  Churn Retention Console, Churn Data Visualization, Spend Analytics Application - Live, OGX Integrity Workbench,
  MCC Motor Claim Copilot. Model deployments: `credit-risk-model` (4 / 8), `churn-scorer` (4 / 16),
  `spend-scorer` (4 / 16), `mcc-approval` (1 / 2). `spend-persona-registry` (1 / 2) was left deployed.
  The CPU group `mlcpu-d697` was not resized (1 of 10 m5.2xlarge nodes). Restart the sibling apps and models
  when this session no longer needs the quota.

## 2026-11-02 11:25 IST - nightly DAG registered, paused

- CDE Airflow job `rsingh-evx-orchestration`, DAG `evx_factory_to_field`, file `cde/dags/evx_dag.py`.
  Schedule 23:30 UTC. The window is the five live dates: the first interval ends 2026-11-06 and the last
  starts 2026-11-09 (`schedule.start` 5 Nov 23:30, `schedule.end` 9 Nov 23:30). Paused, `nextExecution` empty,
  no run started. Pushed `abaa30a`.
- A scheduled run lands, bronzes, extracts, silvers, masters, drains that date's telemetry, then gold, then
  each `usecases/*/config.yaml` with `enabled: true` (uc01 features and the score publish), then recon.
  A manual run skips the telemetry replay. Generate is not a stage: the date's files have to be in the
  generated tree before land. The score task republishes the champion parquet mounted on the score job.
- Lesson: Impala rejects a scalar subquery in a JOIN `ON` clause. The newest score date has to be a derived
  table in `FROM`, which is what the workbench queries do.

## 2026-11-02 11:30 IST - workbench application running

- CAI application `EVX Quality and Reliability Workbench` (`edb1-59tj-z3fm-gro9`), subdomain `evx-workbench`,
  2 vCPU / 8 GB, status running. The URL answers 302 to the workbench login, which is the healthy check without
  a browser session. Pages: Vehicle risk worklist, Batch containment, Pack 360, discovered from
  `usecases/uc01_pack_risk/app_page.py`.
- The same SQL on Impala returned a safety-floor vehicle (`action` NONE), 52 graded cells, the X-ray verdicts,
  90 daily telemetry rows, and the top powder lots with worklist and safety-floor counts. The first application
  (`7f51-ocyo-dtha-8lr5`) failed in about 30 s because the runtime has no streamlit and an application does not
  run `cdsw-build.sh`. The launcher now installs streamlit and impyla when they are missing.
- The three dashboards are not imported yet.

## 2026-11-02 13:05 IST - consumption layer: CAI docs, demo notebook, dashboards live

- **README_CAI.md:** model selection before deployment (three candidates, logistic wins on OOF PR-AUC), gate checks,
  rejection paths (stricter gate, CI salt, non-regression), and what register/deploy refuses.
- **notebooks/cai_demo.ipynb:** Spend-style walkthrough — load champion metrics, candidate chart, gate PASS,
  in-memory AUROC 0.90 rejection, Impala decision counts, optional `evx-uc01-service` call when keys are set.
- **tests/test_cai_selection.py:** gate PASS on the published champion; AUROC 0.90 and PR-AUC regression reject.
- **Semantic views** `dash_pack_risk`, `dash_supplier_batch` (`sql/semantic/20_*.sql`, `21_*.sql`) for dashboards
  and the workbench batch page grain.
- **dataviz/build_dashboard.py:** three dashboards imported to Data Visualization (keys 15000+, connection
  `federal-impala-1`). `--check` passed all 19 visuals on Impala. Titles: EVX Factory Quality & Yield, EVX
  Traceability & Containment, EVX Fleet Reliability.
- **README.md:** pointers to PLAN, README_CAI, the notebook, workbench and dashboards.
- Workbench URL still returns HTTP 302 (healthy without SSO).

## 2026-11-02 14:50 IST - CAI git sync and vision workbench

- **ci/sync_code.py** (from OGX/Spend): `git fetch` + `reset --hard origin/main`, requirements hash install.
  Job **evx-uc01-00-sync-code** (`eiud-f2y2-gtnx-w8k3`) run succeeded; project at **`9e48376`** (`outputs/git_sha.txt`).
  **`notebooks/cai_demo.ipynb`** is on the project for notebook testing.
- **ci/push_files.py**: laptop upload of tracked files when sync cannot run first (OGX pattern).
- Second CAI application **EVX Vision and Documents Workbench** (`g1z8-ehs7-xdvz-jogt`), subdomain
  **`evx-vision`**, script `app/launch_vision_app.py`, status running. Pages: Cell X-ray viewer, Film defect
  gallery, EDR event replay (`usecases/uc01_pack_risk/media_page.py`). Semantic view **`media_xray`** (39,698 rows).

## 2026-11-02 15:10 IST - M1 ops: platform_ctl, demo runbook, stop job

- **`config/platform.json`**: project scope (two CAI apps, `evx-uc01-service`, `rsingh-evx-*` CDE jobs,
  NiFi group name for M2), platform shared services, resource defaults/ceilings from PLAN section 18.
- **`scripts/platform_ctl.py`** (from OGX): `status` / `stop` / `start`, project scope by default;
  `--scope platform --yes-shared` for Data Hubs, CDW, CDE VC and CAI workspace. NiFi skipped until
  `nifi/deploy_flow.py` exists.
- **`ci/project_stop.py`** + CAI job **`stop-evx-project`**: one-off scheduled stop (`--only cai,nifi`),
  clears its own cron after run.
- **`.cursor/rules/project-start-stop.mdc`** and **`docs/DEMO_RUNBOOK.md`**: ~20-minute M1 walkthrough
  (workbenches, dashboards, notebook, guardrails, `platform_ctl stop` at end).
- Pushed **`64dfc37`**. Sync job **evx-uc01-00-sync-code** run **uv3m2g0n3tblcrvo** succeeded (~325 s,
  project at `64dfc37`). CAI job **stop-evx-project** created (`f8xt-2tvp-mlxy-igtq`). Live check:
  both workbench apps running, model `evx-uc01-service` deployed.

## 2026-11-02 15:25 IST - M1 walkthrough pass and OGX skill structural update #1

- **`scripts/m1_walkthrough_check.py`**: automated walkthrough — HTTP 200 on both workbench URLs; all quality
  and vision Impala SQL; 19/19 dashboard visuals; decision counts 17,863; champion logistic. Results in
  **`docs/M1_WALKTHROUGH_RESULTS.md`**.
- **OGX skill** (`tools/cursor-skills/cloudera-usecase-builder/`): M1 structural update — EVX in reference table,
  two entry modes, milestone workflow, resource/model rules, **`lakehouse-backbone.md`**, expanded **`templates.md`**
  (consumers, platform resources, approval ceilings), EVX lessons in   **`reference.md`**. Symlink:
  `~/.cursor/skills/cloudera-usecase-builder`.

## 2026-11-02 15:32 IST - CAI sync and on-project walkthrough job

- Sync runs **szu0p5sztw0plyvg** (→ `50f4993`), **z7xf0mdbttdw0au6** (→ `862724c`), **ge77ur3wvsn0usbn**
  (→ `46d9eb1`). Project files verified: `scripts/m1_walkthrough_check.py`, `notebooks/cai_demo.ipynb`,
  `outputs/git_sha.txt`.
- CAI job **evx-walkthrough-check** (`gyh5-1h8i-p29z-v6xx`), script **`ci/run_walkthrough.py`**: Impala SQL
  for both workbenches passes on the project kernel; log at **`outputs/walkthrough.log`**. First run failed
  on gitignored `metrics.json`; fixed to use tracked **`baseline.json`** only on CAI.
- Sync **8pzgir6rynll3cz4** (→ **`e3fc8cc`**). Walkthrough run **fmxb1ej5mhp8s7us** **ENGINE_SUCCEEDED**:
  all Impala checks pass; **`outputs/walkthrough.log`** on the project.

## 2026-11-16 12:15 IST - Stop all (project scope)

- `platform_ctl.py stop`: stopped both CAI applications and the `evx-uc01-service` deployment (`737799a5`).
  CDE already idle (no runs, `rsingh-evx-orchestration` schedule paused, streams not running). Status confirmed.
- Shared services (Data Hubs, `federal-impala-1`, CDE VC, CAI workspace) left running for sibling demos.
- Data, Kafka topics and checkpoints kept; `platform_ctl.py start` resumes. Rule now maps "start all" / "stop all"
  to project-scope `start` / `stop`.

## 2026-11-18 01:30 IST - M2 phase 9 integration (local only) and plan change for fault 8

- **Plan change, fault 8 (late EDR):** the planted arrival moved from 2026-11-09 to 2026-11-10
  (`config/lakehouse.json` `faults.edr_late`, PLAN.md section 16, `config/expected_recon.json`).
  - An arrival on 11-09 is 60 to 74 h after the event, inside the normal 2 to 72 h upload range. A dump carries
    no upload time, so recon could not tell it from an ordinary late file. 11-10 (day 4) is past that range.
  - Only the planted dumps' landing folder changes; the generator's random draws are the same.
  - CDE dates 11-09 and 11-10 are **not** regenerated in this phase and keep the old placement. Fault 8 has
    milestone `M2-regen`, so it is not MISSING-checked until a later regenerate. `built_milestones` stays M1.
- **Lanes merged** on `0bd2fef` in order C (`181fa86`, WAP), D (`f9e397d`, production batch: D-a `212cf27` +
  D-b `04cca37` squashed), E (`174515e`, twin), A (`237ef8a`, streams), B (`d49d70a`, NiFi).
- **Integrator changes:**
  - `recon.py`: the A, E and D patches merged by hand, adding stream control totals, twin counts, clock skew
    and late reprocess. Lane A's topic literal is quoted with `lit()`.
  - DAG: lane A's stream chain, plus `reprocess_late` after gold and `freshness` after recon. Still paused,
    dates unchanged.
  - `build_silver.py`: `correct_clock_skew` wired into the line_event section.
  - `publish.py`: direct mode writes no `publish_log` row, exactly as M1.
  - `ci/project_stop.py`: clears its own schedule even when a layer exits early.
- **Tests:** `pytest -q` 243 passed, 0 skipped. Lane B's earlier skip (`tests/test_nifi_export.py::
  test_topic_names_match_streaming_json_when_lane_a_keys_exist`, waiting on lane A's topics) now runs and passes.
- Nothing registered or run on CDE, Kafka, NiFi, Impala or CAI.

## 2026-11-18 02:25 IST - M2 phase 9 live step (CDE, Kafka, NiFi), 2026-11-06 only

- **Code:** `main` at `f13f69c` pushed; CDE repository `rsingh-evx-pipeline` synced (file content checked).
- **Capacity:** OGX runs 1239 and 1240 killed by the owner (ended 20:24 UTC). Before every job: no starting or
  running run on `federal-cde-vc` (quota 20 CPU / 80 GiB). The VC's `actualCpuRequests` read a constant 27 CPU /
  103 GiB with and without runs, so it was not counted as usage.
- **WAP self-test:** job `rsingh-evx-wap-check` registered; run **1241** `WAP_SELFTEST result PASS 5/5`.
  - It covered a passing publish, a date emptied on the branch, a failed audit with main unchanged and the
    branch kept.
  - The scratch table `zz_wap_scratch` and its log rows were dropped in the same run.
  - Real gold stays `publish.mode` direct.
- **Topics:** run **1242** (`stream_produce --setup --mode none --topics dtc,qc,line`).
  - CREATED: `evx.vehicle.dtc`, `evx.factory.qc`, `evx.factory.line`, `evx.alert.priority`.
  - EXISTS and not altered: `evx.vehicle.telemetry`, `evx.control`.
  - Nothing produced.
- **Stream jobs:** `deploy_streaming.sh create` registered `rsingh-evx-stream-dtc`, `-qc`, `-line` and
  `-line-window` (driver 1 / 2 GB, one executor 1 core / 2 GB), and updated the M1 telemetry jobs with the same
  files and sizes.
- **NiFi:** `EVX - Alert Router` deployed with 5 processors STOPPED and VALID and 5 services enabled.
  - The root truststore SSL context is referenced, not modified.
  - Registry `evx-flows/evx_alert_router` version 1.
  - Re-exported to `nifi/flows/evx_alert_router.json`. The export scan passed (no password, token, keystore
    password, access key or credentials provider), and so did lane B's 38 tests.
- **Produce 2026-11-06:** run **1243** `--topics dtc,qc,line`, `STREAM_PRODUCED_TOPICS dtc=40 qc=6313 line=62476`
  (events plus their per-minute `evx.control` totals). No telemetry was sent.
- **Drains (available-now, one at a time):**
  - dtc **1244**: 20 rows, quarantine 0, checkpoint `dtc_bronze`.
  - qc **1245**: 5,017 rows, quarantine 0, checkpoint `qc_bronze`.
  - line **1246**: 61,036 rows, quarantine 0, checkpoint `line_bronze`.
  - line-window **1247**: `silver.line_cycle_window` 3,734 windows, sum(cycles) 61,036, checkpoint `line_window`.
- **Checks (Impala):**
  - `bronze.telemetry_reading` for 2026-11-06: 282,924 rows before and after (282,924 distinct event ids).
  - `stream_control`: 782 rows, unchanged; `stream_quarantine`: empty.
  - Each new bronze table holds only its own topic, and `dtc_quarantine`, `qc_quarantine` and `line_quarantine`
    are separate and empty.
- **Not done in this step:**
  - The telemetry drain was not run, so the M2 control totals for 11-06 are still on `evx.control`, not yet in
    `bronze.stream_control`.
  - Maintenance and freshness were not run (not on this phase's done list). No snapshot was expired.
  - The DAG was not redeployed or unpaused (window end 2026-11-09 23:30 UTC); `built_milestones` stays `["M1"]`.
  - The producer, the model and the CAI apps were not started. NiFi stays stopped.

## 2026-11-18 02:45 IST - Step 8 bookkeeping and known gap

- **Run ids:**
  - 1241: WAP check, PASS 5/5.
  - 1242: topic setup, nothing produced.
  - 1243: produce 2026-11-06, topics dtc, qc and line.
  - 1244, 1245, 1246, 1247: drains for dtc, qc, line and line-window.
- **NiFi:** registry `evx-flows/evx_alert_router` is at version 1.
- **Milestones:** `built_milestones` stays `["M1"]`.
- **Impala counts (2026-11-06):**
  - `bronze.telemetry_reading`: 282,924 before and after.
  - `vehicle_dtc_event`: 20.
  - `factory_qc_event`: 5,017.
  - `factory_line_event`: 61,036.
  - `silver.line_cycle_window`: 3,734 windows, 61,036 cycles.
  - `dtc_quarantine`, `qc_quarantine` and `line_quarantine`: 0 each.
  - `stream_control`: 782 rows, unchanged.
- **Capacity gate:** the virtual cluster's "in use" figure (`actualCpuRequests`) stayed at 27 CPU / 103 GiB with
  no run starting or running. It is not a headroom gate. Headroom is the quota minus the requests of active runs.
  No job is sized up to fit it.
- **Known gap:** the M2 control totals for 2026-11-06 are on `evx.control` but not yet in
  `bronze.stream_control`, which is short by those rows. Do not re-drain telemetry for it, and do not run
  `rsingh-evx-stream-produce` for 2026-11-06. `rsingh-evx-stream-telemetry` subscribes to `evx.control`, so its
  continuous run from the `telemetry_bronze` checkpoint at project start reads those rows. No telemetry is pending
  on `evx.vehicle.telemetry` for 2026-11-06.
- **Doc fix:** the `late_data._rule` note in `config/operations.json` now matches the code (late from event date
  + 4 for 72 h).

## 2026-11-18 03:05 IST - Project started at project scope (code at 36052ac)

- **Shared services:** all already up, so none was started and `--scope platform` was not used.
  - `federal-kafka`, `federal-flink`, `federal-nifi`: AVAILABLE.
  - `federal-impala-1`: Running.
  - `federal-cde-vc`: AppInstalled.
  - `federal-cml-ws`: finished.
- **Code sync:**
  - `cde repository sync` of `rsingh-evx-pipeline`; the content was checked by download.
  - CAI `evx-uc01-00-sync-code` run `9rav6af1wp4efnby` succeeded.
- **New DAG task jobs:** `rsingh-evx-reprocess` (`cde/jobs/reprocess_late.py`) and `rsingh-evx-freshness`
  (`cde/jobs/freshness.py`). They use the shape of `rsingh-evx-recon`: the repository mounted at `/`, a 2c / 8g
  driver and 3 executors of 2c / 8g.
- **Deploys:**
  - `deploy_streaming.sh create` updated the seven stream jobs.
  - `deploy_dag.sh` registered the DAG from git. START is 2026-11-05 23:30, END 2026-11-09 23:30 UTC, catchup
    False.
- **`platform_ctl.py start`:**
  - Unpaused `rsingh-evx-orchestration`. The schedule is active with no next run (the window is closed), and no
    DAG run fired.
  - Streaming runs from their checkpoints: telemetry 1248, agg 1249, dtc 1250, qc 1251, line 1252,
    line-window 1253, all running.
  - NiFi `EVX - Alert Router`: 4 processors running, the hold processor stopped.
  - `evx-uc01-service`: deployment 737799a5 could not be restarted, so a new deployment 7158cfa1 was made from
    build d96b378c, now deployed.
  - Both CAI apps APPLICATION_RUNNING.
  - The producer was not started, and no manual DAG run was made.
  - Before the start no run was active on the cluster. The 6 streaming runs request 12 CPU / 24 GiB of the
    20 / 80 GiB quota.
- **Known gap closed:** `rsingh-evx-stream-telemetry` read the M2 control totals from `evx.control`.
  `stream_control` now has line 1,440 rows (total 61,036), qc 1,296 (5,017), dtc 20 (20) and telemetry 782
  (282,924). Each total equals its bronze table. `telemetry_reading` for 2026-11-06 is still 282,924, and
  `stream_quarantine` is empty.

## 2026-11-18 03:25 IST - Step 10 fix-forward: SQL from config files is validated (8dfe440, aa86274)

- **What changed:** the final pass on `0bd2fef..da8ba4f` found two places that put values read from config files
  into SQL unchecked.
  - `scripts/check_local.py` (`check_stream_m2`): each topic's table, event-time and quarantine names must
    match `[a-z_]+`.
    - Topic names go through `lit()`. In `8dfe440` it stripped quotes. Since `aa86274` it raises `ValueError`
      unless the value matches `evx\.[a-z.]+`, and it never strips characters.
  - `nifi/deploy_flow.py` (`route_sql`): the reason must be THERMAL or CRITICAL. The input topic, the
    priority and the DTC codes must match `[A-Za-z0-9._-]+`.
    - The generated routing SQL is byte-identical to the committed export, so NiFi was not redeployed.
- **Tests:**
  - pytest 243 passed after each commit.
  - `check_local.py stream` OK on the local integration data.
  - gitleaks: no leaks over the full history. Brand check: 0 hits.
  - NiFi export `check_export`: PASS.
- **Sync to `aa86274`:**
  - `cde repository sync --name rsingh-evx-pipeline` printed nothing and exited 0. The repository is ready with
    signature `aa86274`, and its 32 top-level entries match `git ls-tree aa86274`.
  - CAI `evx-uc01-00-sync-code` run `68ydvp26aj72tpww` succeeded.
- **Not restarted:**
  - Streaming runs 1248-1253 kept running.
  - `evx-uc01-service`, both CAI apps and the NiFi `EVX - Alert Router` flow were left as they were.
  - No DAG run, reprocess, freshness, fault-8 regeneration or producer run.

## 2026-11-18 14:00 IST - Telemetry stream jobs resized (fb03579)

- **Change:** `rsingh-evx-stream-telemetry` and `rsingh-evx-stream-agg` go from up to 2 executors of 2 cores /
  4 GB to 1 executor of 1 core / 4 GB; driver 1 core / 2 GB unchanged. Recorded in `config/platform.json`
  `resources.changes`, so one batch job fits beside the six streams.
- **Requests before:** 1248 and 1249 at 3 CPU / 6 GiB each (up to 5 / 10 with two executors), 1250-1253 at
  2 CPU / 4 GiB each: 14 CPU / 28 GiB, up to 18 / 36 of the 20 CPU / 80 GiB quota.
- **Deploy:** `deploy_streaming.sh create` updated all seven job definitions. Only the telemetry group was stopped
  and started: 1248 and 1249 killed, replaced by **1254** (telemetry) and **1255** (agg), each 2 CPU / 6 GiB,
  resumed from their checkpoints. 1250-1253 kept running, unchanged.
- **Requests after:** 12 CPU / 28 GiB, headroom 8 CPU / 52 GiB.
- **Gate:** `bronze.telemetry_reading` for 2026-11-06 still 282,924 rows (282,924 distinct event ids);
  `bronze.stream_quarantine` still 0.

## 2026-11-18 14:20 IST - Full regenerate failed; generated tree deleted (run 1256)

- **Run 1256** `rsingh-evx-generate`, no arguments (full calendar, truth included), run-time size driver 2c / 8g
  and 2 executors 2c / 8g (6 CPU / 24 GiB, `resources.changes`, commit 9764f84). Running 08:39 UTC, failed
  08:45 UTC.
- It printed `generate scale=1.0 partitions=31`, deleted `rsingh_evx/generated/` (generate.py deletes before
  it generates) and failed in the first `collect`. No GENERATE_RESULT, nothing written, truth tables not replaced.
- **Cause:** `generators/twin.py` `twin_config()` reads `config/twin.json` relative to the package. On CDE the
  executors get only `generators.zip` (`ship_packages`), so the path is
  `/opt/spark/work-dir/generators.zip/config/twin.json` (`NotADirectoryError`). Run 989 predates the twin
  partitions; local runs pass because the executors see the repository.
- **State:** the generated tree is empty. Landing, bronze through gold and the `ref.generator_truth_*` tables are
  unchanged. No DAG run was started. The six streams kept running (12 CPU / 28 GiB). Job definitions unchanged.

## 2026-11-18 14:45 IST - Regenerate restored the tree (run 1257); manual DAG run 1258 ran no tasks

- **Run 1257** `rsingh-evx-generate` at `084e577` (config/twin.json read on the driver), no arguments, driver
  2c / 8g and 2 executors 2c / 8g (6 CPU / 24 GiB). Running 08:58 UTC, succeeded 09:12 UTC.
  - `GENERATE_RESULT scale=1.0 seconds=773`, 31 partitions. The generated tree that run 1256 deleted is restored:
    19 sources, for example `edr` 1,775 files, `line_event` 108, `twin_scenario` 240, `qc_xray` 40,220.
  - Truth tables replaced: `generator_truth_pack` 18,335, `generator_truth_batch` 1,650, `generator_fleet`
    17,646, `generator_truth_fault` 16, `generator_truth_twin` 401. Label rate 0.0894 (863 positives).
- **Run 1258** `rsingh-evx-orchestration`, manual, `business_date` 2026-11-09. Succeeded 09:14:20-09:14:26 UTC
  with **no tasks**: CDE triggers a manual run with the current time as its execution date (2026-11-18), after
  the DAG `end_date` 2026-11-09 23:30 UTC, so Airflow created no task instances. Nothing was rebuilt.
- **Decision:** the DAG window, schedule and catchup stay as they are, and the orchestration job is not run
  again. 2026-11-09 and 11-10 are rebuilt by running each batch job directly, one at a time, with the DAG's
  arguments and `--pipeline-run manual-20261109` / `manual-20261110`. No producer or stream job is run.

## 2026-11-18 23:20 IST - Direct chain for 2026-11-09 (runs 1259-1268): recon failed on fault 12

- Each job run alone, beside the six streams (1250-1255), `--business-date 2026-11-09 --landing
  .../rsingh_evx/landing --db-prefix rsingh_evx --pipeline-run manual-20261109` (uc01-score: `--input
  /app/mount/scores/scores.parquet`). Temporary sizes from `ab85ab5`.

| Run | Job | Result |
|---|---|---|
| 1259 | land | succeeded, 16 sources, 372 files, 19.6 MB |
| 1260 | bronze | succeeded, line_event 62,886, cell_mes_step 51,073, cell_qc_inspection 5,227, telematics_daily 0 |
| 1261 | extract | succeeded, doc_extract 166, qc_image_metric 113, edr_event 20 |
| 1262 | silver | succeeded, 0 rejects in every table, line_event 62,886 |
| 1263 | master | succeeded, as_of 2026-11-09, pack 18,056, vehicle 17,958 |
| 1264 | gold | succeeded, fact_line_cycle 62,886, fact_edr_event 20, fact_telemetry_window 0 |
| 1265 | reprocess | succeeded, ON_TIME, touched 11-07..11-09, 0 late dumps of 20 events |
| 1266 | uc01-features | succeeded, 17,863 rows, positives 1,071 |
| 1267 | uc01-score | succeeded, 17,863 rows published, score_date 2026-11-05 |
| 1268 | recon | **failed**: 21 rows, MATCH 18, EXPLAINED 1, INFO 1, UNEXPECTED 1 |

- **Recon 1268** (`recon-c3713bfaa9d6`): the driver log was cut at 17:43:59 before any print, so the rows were
  read from `ref.recon_result` and `ref.load_audit`. EXPLAINED `silver line_event skew 45` fault 11
  (`VA-EOL-02 5400 s`). UNEXPECTED `bronze sap_odata_record contract diff 1` (`ZZ_MOISTURE_PPM`).
- **Cause:** batch bronze on the cluster last loaded 2026-11-05, so 11-09 was the first load to see the field and
  `ref.schema_change` got `first_seen_date` 2026-11-09; fault 12 expects 2026-11-08.
- **Stuck SoC (fault 7):** no row; there is no stream telemetry for 11-09 (`fact_telemetry_window` 0). It stays
  unproven, with no producer run. `built_milestones` stays `["M1"]`, so recon does not mark it MISSING.
- **Repair:** land through recon for 11-06, 11-07 and 11-08 (never batch-loaded), then bronze through recon again
  for 11-09 (no land or extract), so the field is first seen on 11-08 and the 11-09 schema_change row is replaced.

## 2026-11-19 06:40 IST - Repair chain for 2026-11-06..11-08 (runs 1269-1298) and the 11-09 re-run to score (1299-1305)

- Same arguments and temporary sizes as runs 1259-1268, `--pipeline-run manual-2026110N`. One batch job at a time
  beside the six streams (1250-1255). Every run succeeded, 2026-11-18 18:04-20:59 UTC.

| Date | Runs | bronze line_event / cell_mes_step / cell_qc_inspection | extract doc / image / edr | master pack / vehicle | gold fact_telemetry_window | features rows / positives |
|---|---|---|---|---|---|---|
| 11-06 | 1269-1278 | 61,036 / 49,757 / 5,017 | 152 / 107 / 17 | 18,057 / 17,957 | 12,938 | 17,957 / 1,071 |
| 11-07 | 1279-1288 | 63,015 / 50,480 (799 quarantined) / 5,216 | 134 / 107 / 15 | 18,153 / 18,051 | 0 | 18,051 / 1,071 |
| 11-08 | 1289-1298 | 61,050 / 49,046 / 4,920 | 151 / 105 / 18 | 18,248 / 18,147 | 0 | 18,147 / 1,071 |

- Silver: 0 rejects; `line_event` equals bronze each date. Reprocess: ON_TIME, 0 late dumps each date. uc01-score
  (1277, 1287, 1297): 17,863 rows published, score_date 2026-11-05, schema `576d28e42713`.
- **Recon:**
  - 11-06 (1278, `recon-15881ff1044d`): 21 MATCH.
  - 11-07 (1288, `recon-85a89f81d4ed`): MATCH 17, EXPLAINED 2 (fault 1 delta-token duplicate, fault 9 identity
    match 3), MISMATCH 2 (fault 2 trailer count `CL1_formation_B.csv`, fault 5 orphan cell `PKPL12611070001`),
    INFO 1 (no_such_cell). No UNEXPECTED.
  - 11-08 (1298, `recon-72de734aea87`): MATCH 16, EXPLAINED 4 (fault 3 PNG decode, fault 12 contract diff
    `ZZ_MOISTURE_PPM`, fault 4 text layer 6, fault 10 link change 2). No UNEXPECTED.
- `ref.schema_change`: `ZZ_MOISTURE_PPM` (`A_InspectionLot`) now has `first_seen_date` 2026-11-08 (bronze 1290).
- **11-09 re-run, no land or extract:** bronze 1299, silver 1300, master 1301, gold 1302, reprocess 1303 (ON_TIME,
  0 late of 20), uc01-features 1304 (18,242 rows, positives 1,071), uc01-score 1305 (17,863 published). Recon
  pending.
- **Gate after bronze 1299:** `bronze.telemetry_reading` for 2026-11-06 still 282,924 rows (282,924 distinct event
  ids); `bronze.stream_quarantine` 0.

## 2026-11-19 09:40 IST - Recon for 2026-11-09 passed (run 1306)

- **Run 1306** `rsingh-evx-recon`, `--business-date 2026-11-09 --pipeline-run manual-20261109`, same arguments
  and temporary size as 1268. Succeeded 2026-11-19 00:59:46-01:01:31 UTC.
- Recon fails the run on any UNEXPECTED row (as 1268 did), so the success means no UNEXPECTED row: the
  `ZZ_MOISTURE_PPM` contract-diff failure of 1268 is cleared by the 11-08 first-seen repair (bronze 1290).
- Clock skew (fault 11, `VA-EOL-02 5400 s`) was already EXPLAINED on 1268. Stuck SoC (fault 7) stays absent: no
  stream telemetry for 11-09, no producer run.
- **Per-row counts unread.** The driver log was cut before the summary print again. `federal-impala-1` answers
  `SELECT 1`, but every query on `rsingh_evx_ref.recon_result` fails: Impala cannot reach the Hive Metastore
  (Kerberos, "Failure to initialize security context"). Tried once at 09:28 and once at 09:38 IST, then stopped.
  Nothing shared was changed. The row breakdown goes in a later entry if Impala recovers.

## 2026-11-19 21:30 IST - Direct chain for 2026-11-10 (runs 1307-1316), freshness 1317-1321, batch sizes restored

- **Chain 2026-11-10**, `--pipeline-run manual-20261110`, same arguments and temporary sizes as 11-09, one job at
  a time beside the six streams. Every run succeeded, 2026-11-19 04:08-04:50 UTC.

| Run | Job | Result line |
|---|---|---|
| 1307 | land | `LAND_RESULT dates=1 files=379 mb=19.7` |
| 1308 | bronze | `BRONZE_RESULT dates=1 seconds=123` |
| 1309 | extract | `EXTRACT_RESULT dates=1 seconds=77` |
| 1310 | silver | `SILVER_RESULT dates=1 unmapped_docs=0` |
| 1311 | master | `MASTER_RESULT as_of=2026-11-10` |
| 1312 | gold | `GOLD_RESULT dates=1 as_of=2026-11-10` |
| 1313 | reprocess | `REPROCESS_RESULT arrival_dates=1 touched=5 late_dumps=22 REPROCESSED=1` |
| 1314 | uc01-features | `FEATURE_RESULT rows=18335 matured=9658 positives=1075` |
| 1315 | uc01-score | `SCORE_PUBLISHED rows=17863 score_date=2026-11-05 schema=576d28e42713` |
| 1316 | recon | `RECON_RESULT dates=1 rows=17 EXPLAINED=1 MATCH=16 failing=0` |

- **Fault 8 (late EDR) proven on the cluster.** 1313: `REPROCESS_ROW arrival=2026-11-10 status=REPROCESSED
  touched=2026-11-06..2026-11-10 reopened=2026-11-06 late_dumps=22 of 57 events_before=31 events_after=88`.
  1316: `RECON_ROW 2026-11-10 gold fact_edr_event 'late reprocess' 22 EXPLAINED fault=8 re-opened 2026-11-06`.
  Both driver logs were complete.
- **Impala:** one try at 21:10 IST for the 11-09 and 11-10 `rsingh_evx_ref.recon_result` rows. `SELECT 1`
  answered; the table query failed with "Failed to connect to Hive Metastore" (Iceberg table load). Not retried,
  nothing shared changed. The 11-09 per-row breakdown stays unread; 1306 succeeded is the record.
- **Freshness** `rsingh-evx-freshness`, business-date mode (no `--live`: the dates are in the past, so wall-clock
  lag would be meaningless), `--pipeline-run manual-fresh-<date>`, one at a time, all succeeded:

| Run | Date | Result |
|---|---|---|
| 1317 | 11-06 | `products=7 GREEN=5 RED=2` (field, twin) |
| 1318 | 11-07 | `products=7 GREEN=4 RED=3` (cell, field, twin) |
| 1319 | 11-08 | `products=7 GREEN=4 RED=3` |
| 1320 | 11-09 | `products=7 GREEN=4 RED=3` |
| 1321 | 11-10 | `products=7 GREEN=4 RED=3` |

  - Batch products (supply, pack_vehicle, field_edr, service) GREEN every date; field_edr lag 1 business day on
    11-09 (SLA 3).
  - The RED products are stream-fed: `bronze.factory_qc_event` last 2026-11-06 23:51:30,
    `bronze.vehicle_dtc_event` 14:54:01, `bronze.factory_line_event` 18:29:58. Only 11-06 was ever produced and
    the producer stays off, so their lag grows with the business date. Expected, not a fault.
  - The first driver-log fetch held only the submit line; a second fetch after the runs ended had every line.
- **Sizes restored:** bronze, extract, silver, master, gold, recon, freshness, uc01-features and reprocess back to
  driver 2c / 8g, 3 executors 2c / 8g (`config/platform.json` `resources.changes` 2026-11-19), read back from
  `cde job describe`. Land, generate and the streams unchanged. One such job beside the streams is 20 CPU, the full
  quota.
- Producer, orchestration job and DAG window untouched. Six streams (1250-1255) still running.

## 2026-11-20 00:45 IST - M2 built in recon; faults 6 and 7 not MISSING-checked; recon 2026-11-09 passed (run 1323)

- **Decision:** the demo stays on the closed window through 2026-11-10 and the telemetry producer is not run.
  `config/expected_recon.json` `built_milestones` is now `["M1", "M2"]`. Faults 6 (degrees F, 11-08..11-10) and 7
  (stuck SoC, 11-09) move to milestone `M2-produce`, which is not built: both are read only from the stream table
  `silver.telemetry_window`, which has no rows after 2026-11-06, so with M2 built they would be MISSING. Dates and
  outcome EXPLAINED unchanged, so an observed row is still EXPLAINED. Fault 11 (clock skew) stays M2 and is checked.
  `tests/test_recon.py` accepts `M2-produce` and checks that 6 and 7 are not MISSING while 11 is. Commit `c5b4669`.
- **Sync:** `cde repository sync --name rsingh-evx-pipeline`, signature `c5b4669`. The synced
  `config/expected_recon.json` was downloaded before the run: faults 6 and 7 `M2-produce`, 11 `M2`.
- **Run 1322** `rsingh-evx-recon` 2026-11-09 at the restored size (8 CPU / 32 GiB) stayed in `starting` for 18
  minutes beside the six streams (12 CPU), together the full 20 CPU quota. Killed before it started; it wrote
  nothing.
- **Run 1323** same job and arguments, `--pipeline-run manual-20261109-m2`, run-time override 2 executors (6 CPU /
  24 GiB, as 1306 and 1316; definition left at 3 executors, `resources.changes` 2026-11-20). Succeeded 2026-11-19
  19:13:01-19:14:51 UTC. `RECON_RESULT dates=1 rows=19 EXPLAINED=1 MATCH=18 failing=0`; the one EXPLAINED row is
  `silver line_event 'skew' 45 fault=11 VA-EOL-02 5400 s`. No MISSING, no UNEXPECTED.
- **Capacity:** at the restored 3-executor size a batch job does not start beside the six streams. Run batch jobs
  with the 2-executor override while the streams run, or stop the streams first.
- **Impala**, one attempt this turn: `SELECT 1` succeeded; `SELECT count(*) FROM rsingh_evx_ref.recon_result`
  failed inside `HiveMetaStoreClient.<init>` while loading the Iceberg table (Metastore connection failure; the
  head of the trace with the Kerberos text was not kept). Not retried, nothing shared changed.
- Producer, orchestration job and DAG window untouched. Six streams (1250-1255), both apps, the model and the NiFi
  flow unchanged.
