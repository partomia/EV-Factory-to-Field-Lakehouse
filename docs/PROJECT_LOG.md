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
