# WAP feasibility spike

2026-11-01 · CDE job `rsingh-evx-wap-spike` (`cde/jobs/wap_spike.py`), runs 980 (`--mode run`) and 981
(`--mode drop`) · scratch table `rsingh_evx_ref.wap_spike_scratch` (created, probed, dropped).

Platform: CDE `federal-cde-vc`, Spark `3.5.4.1.26.732.1000-5`, Iceberg 1.5.2, `spark_catalog` =
`org.apache.iceberg.spark.SparkSessionCatalog` (Hive-backed), extensions include
`IcebergSparkSessionExtensions`. Impala `federal-impala-1`. Atlas through the data lake Knox gateway.

## Results

| # | Check | Result | Evidence |
|---|---|---|---|
| 0 | Create a format-v2 table with `write.wap.enabled=true` | PASS | 100 rows on `main` |
| 1a | `ALTER TABLE … CREATE BRANCH audit_b` | PASS | `refs` shows `audit_b` at the `main` snapshot |
| 1b | Write with `spark.wap.branch=audit_b` (`writeTo(table).append()`) | PASS | branch 150, `main` 100 |
| 1c | Write to the `branch_audit_b` identifier | PASS | branch 175, `main` 100 |
| 2 | Readers of `main` do not see the branch | PASS | `main` 100 rows, tags only `main` |
| 3 | Audit query on the branch in Spark (`VERSION AS OF 'audit_b'` and `table.branch_audit_b`) | PASS | per-date counts, distinct keys, nulls |
| 4a | `CALL spark_catalog.system.fast_forward(table, 'main', 'audit_b')` | PASS | `main` moved to the branch head, 175 rows |
| 4b | WAP-ID staged snapshot (`spark.wap.id`) + `cherrypick_snapshot` | PASS | staged snapshot not on `main` until cherry-picked (175 → 185) |
| 5 | Impala reads `main` after the fast-forward | PASS | `COUNT(*)` = 142 = Spark's `main` |
| 5 | Impala reads an unpublished branch by **snapshot id** (`FOR SYSTEM_VERSION AS OF <id>`) | PASS | 153 = Spark's branch count |
| 5 | Impala reads a branch by **name** (`FOR SYSTEM_VERSION AS OF 'name'`, `table.branch_name`) | FAIL | ParseException / cannot resolve table reference |
| 6a | `overwritePartitions` on a branch (`branch_<b>` identifier) | PASS | only partition 2026-01-02 replaced (50 → 7) on the branch; `main` unchanged until fast-forward |
| 6b | `overwritePartitions` with `spark.wap.branch` | PASS | partition 2026-01-03 replaced on the branch only |
| 6 | Atlas lineage | PARTIAL | `iceberg_table` entity ACTIVE with its S3 directory and the create process; branch writes and procedure calls add **no** lineage edges |
| 7 | Drop branches; drop the table | PASS | Impala: `rsingh_evx_ref` has no tables |

## Decision: (a) branch WAP with the audit in Spark

`lakehouse/publish.py` `publish(df, table, partition_cols, audit)`:
1. create the table on `main` if it does not exist (empty, with partitioning and sort order);
2. `CREATE BRANCH IF NOT EXISTS wap_<run>` at `main`;
3. `df.writeTo(f"{table}.branch_wap_<run>").overwritePartitions()`;
4. run the audit in Spark against `VERSION AS OF 'wap_<run>'` (contract, keys, counts vs source, recon checks);
5. pass → `fast_forward(table, 'main', 'wap_<run>')`, then drop the branch; fail → keep the branch for inspection
   (its snapshot id is logged to `ref.publish_log`, Impala can read it by snapshot id), `main` is untouched.

Constraints that follow:
- `fast_forward` needs `main` to be an ancestor of the branch: **only one writer per gold table** (the DAG,
  `max_active_runs=1`). Streaming bronze tables are not published through WAP.
- Impala cannot address a branch by name: anything reading an unpublished branch from Impala or the app uses the
  snapshot id from `ref.publish_log`.
- Lineage to Atlas is published by our code (OGX pattern, `lakehouse/publish.py` lineage step), not left to the
  Spark Atlas hook, which records only the table creation.
- M1 uses the plain `overwritePartitions` path inside the same `publish()` signature; M2 switches the inside to the
  steps above. Option (b) (WAP-ID cherry-pick) also works and stays the documented fallback; (c) is not needed.
