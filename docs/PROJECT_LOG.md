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
