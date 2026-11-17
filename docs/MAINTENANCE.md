# Iceberg table maintenance

CDE job `rsingh-evx-maintenance` (`cde/jobs/maintenance.py`) on resource `rsingh-evx-pipeline`. It runs on
its own schedule, `30 4 * * *` UTC, after the nightly DAG window. Settings are in `config/operations.json`
`maintenance`.

## What it does, per table

The job covers every Iceberg table in the `bronze`, `silver`, `master`, `gold`, `features`, `semantic` and
`ref` databases of the `rsingh_evx` prefix. For each table it runs these steps in order:

| step | procedure | rule |
|---|---|---|
| compact | `rewrite_data_files` | stream tables only (`compact.tables`), target 128 MB, at least 5 input files |
| manifests | `rewrite_manifests` | every table |
| branches | `ALTER TABLE … DROP BRANCH` | `wap_*` branches whose head is older than `stale_branch_days` (7), unless `ref.publish_log` shows the branch in flight |
| expire | `expire_snapshots` | older than `snapshots_days` (7), keeping `retain_last` (5); **never** on `no_expire` tables |
| orphans | `remove_orphan_files` | older than `orphan_days` (3), always measured from the real clock |

## Guards

- **Prefix.** Every table name is checked against `^rsingh_evx_[a-z0-9_]+\.[a-z0-9_]+$` before any
  procedure runs, and checked again right before each `CALL` and `DROP BRANCH`. A `--db-prefix` outside
  `rsingh_evx_` fails before the first procedure; a `--only` list with a bad name fails before Spark is
  asked anything.
- **Stream readers.** `bronze.telemetry_reading` and `bronze.factory_line_event` are read as streams by
  other jobs, so the job never expires their snapshots, and no reader checkpoint can point at an expired
  snapshot. They are still compacted: Iceberg stream readers skip the `replace` snapshot that compaction
  commits.
- **WAP branches.** A `wap_*` branch is kept if a `ref.publish_log` row for that table and branch has a
  non-terminal status, no `ended_at`, or a `started_at` inside the retention. Snapshots held by a kept branch
  survive the expiry. Use `--keep-branches` to report stale branches without dropping them.
- **Gold publish window.** A `gold.*` table is skipped while the UTC wall clock is inside
  `gold_publish_window_utc` (23:00 to 04:00, the nightly DAG at 23:30 with a 4 h timeout). The clock is read
  again before each gold table.
- **In-flight files.** `remove_orphan_files` only deletes files older than 3 days, measured from the real
  clock even when `--now` is given.
- SQL literals are quoted the way the app pages quote them; identifiers come from validated names only.

## Usage

```bash
spark-submit maintenance.py --dry-run                 # plan only; orphans as a dry run
spark-submit maintenance.py                           # every project table
spark-submit maintenance.py --only bronze.telemetry_reading,silver.line_event --keep-branches
```

Each step prints one `MAINT_*` line. The last line is `MAINT_RESULT mode=… tables=… skipped=…
expiry_skipped=… errors=…`. Any table that errors makes the job fail after the other tables have run.

## Local proof (`tests/test_d_maint.py`, local Spark 4.0.1 + Iceberg 1.10, SQLite JDBC catalog)

There were three tables with 8 commits each: `bronze.telemetry_reading`, `silver.maint_probe` (with branches
`wap_stale` and `wap_busy`, plus an in-flight `ref.publish_log` row for `wap_busy`) and `gold.maint_probe`.
The job ran with `--now` 10 days ahead:

```
MAINT_DONE bronze.telemetry_reading compact files=8->1
MAINT_SKIP bronze.telemetry_reading expire reason=stream-reader snapshots=10
MAINT_BRANCH_KEPT silver.maint_probe branch=wap_busy reason=publish-in-flight
MAINT_DONE silver.maint_probe dropped_branch=wap_stale
MAINT_DONE silver.maint_probe expire snapshots=9->6
MAINT_DONE silver.maint_probe orphan_files=1          # the 10-day-old orphan; a fresh one is kept
MAINT_DONE gold.maint_probe expire snapshots=9->5
```

Row counts were unchanged on all three tables. The dry run changed no snapshots. A run at 01:00 UTC
skipped `gold.maint_probe` (`reason=gold-publish-window`).

## Open points

- Stream tables keep every snapshot. The option INTERFACES allows, retaining only what is newer than the
  oldest reader checkpoint, needs the checkpoint offsets of lane A's jobs. Until that exists, the metadata
  of these tables grows by one snapshot per micro-batch.
- `scripts/platform_ctl.py` already pauses and unpauses every scheduled `rsingh-evx-*` CDE job, so this
  schedule is covered. But `start` skips the streaming jobs while any open-ended project schedule exists,
  and this schedule has no end date, so `start` would never restart the streams. The proposed patch limits
  that check to `cde_dag_jobs`.
