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
