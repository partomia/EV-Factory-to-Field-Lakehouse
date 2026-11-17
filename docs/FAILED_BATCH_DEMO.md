# Failed-batch drill on silver

Shows that a silver batch failing in the middle of a table write leaves main as it was, and that the resumed
batch (the same command the nightly retry runs) ends with the same rows as a clean run.

`cde/jobs/failed_batch_drill.py`, CDE job `rsingh-evx-failed-batch-drill`. Run it by hand only; it is never in
the nightly DAG.

```bash
spark-submit failed_batch_drill.py --business-date 2026-11-06 [--fail-table sap_inspection_lot] [--no-clean-run]
```

## Phases

1. **Clean.** `build_silver` for the date, unchanged. Per silver table it records the date's rows, a content
   fingerprint of those rows, the snapshot id of main and the number of live data files. `--no-clean-run` takes
   the current main as the clean state.
2. **Fail.** `build_silver` again with one fault injected. When it writes `--fail-table`, the overwrite runs over
   four partitions and the task of partition 1 raises, so Spark aborts the write before Iceberg commits.
   `build_silver.py` is not changed: the drill wraps `evx_common.write_dates` for that one table only.
3. **Resume.** `build_silver` for the date, unchanged.

**Verdict PASS** when two things hold:
- After the failure, the failing table and every table written after it still have their clean snapshot and
  data files, and every table has its clean rows and fingerprint. Tables written earlier commit again with the
  same rows.
- After the resume, every table has its clean rows and fingerprint.

Each phase is logged in `ref.failed_batch_drill`, one row per table and phase. A rerun for the date replaces
its rows. `ref.load_audit` shows the silver run as FAILED with the injected error. The drill writes only
`rsingh_evx_*` tables.

## Local evidence (CI scale 0.05, 2026-11-06)

```
DRILL_RESULT business_date=2026-11-06 fail_table=sap_inspection_lot verdict=PASS tables=19 kept_snapshot=6
  recommitted_same_rows=13 rows_clean=4490 rows_failed=4490 rows_resumed=4490
```

| Phase | Tables | Rows | Same as clean |
|---|---|---|---|
| CLEAN | 19 | 4,490 | - |
| FAILED | 19 | 4,490 | 19 |
| RESUMED | 19 | 4,490 | 19 |

## Demo script (about 3 minutes)

1. Show the silver row counts and snapshot of `sap_inspection_lot` for the date in Impala.
2. Run the drill. Point at the `DRILL_FAILURE` line: the task failed and nothing was committed.
3. Show the same counts and snapshot again, then the `RESUMED` rows in `ref.failed_batch_drill`.
