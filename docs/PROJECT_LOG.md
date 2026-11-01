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
