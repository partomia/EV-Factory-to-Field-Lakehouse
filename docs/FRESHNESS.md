# Freshness per data product

`cde/jobs/freshness.py`, CDE job `rsingh-evx-freshness`, runs after recon and writes one
`rsingh_evx_ref.freshness` row per data product (PLAN.md section 5) and business date. A rerun for the same date
replaces that date's rows. Products, owners and SLAs are in `config/operations.json` under `freshness`.

```bash
spark-submit freshness.py --business-date 2026-11-09 [--live]
```

Business dates are decoupled from the wall clock (PLAN.md section 15), so lag is measured on the business
calendar, never against today's date.

| Check | Last loaded | Lag |
|---|---|---|
| batch | latest `ref.load_audit` COMMITTED date of the table's stage, or `max(date_col)` when the product names one | business date minus that date, in business days |
| stream | latest event time in the table | end of the business date (next midnight UTC) minus that time, in minutes, clamped at 0; `--live` uses the wall clock |

- **Status.** GREEN within the SLA. AMBER within SLA + `amber_extra_days` (batch) or SLA x `amber_factor_minutes`
  (stream). RED beyond that, or when a table of the product has never been loaded.
- The product row carries the worst check. Every check is listed in `details` (JSON).
- `last_commit_at` is the latest Iceberg snapshot of the determining table.
- Products whose tables do not exist yet (for example the twin before M2 is loaded) are skipped, with a
  `FRESHNESS_SKIP` line.
- Column names from config are checked against `^[a-z0-9_]+$` and used only as identifiers. Dates and timestamps
  in SQL come from parsed values. Timestamps are `timestamp_ntz`.

## Local evidence (CI scale 0.05, business date 2026-11-10)

`FRESHNESS_RESULT business_date=2026-11-10 products=6 GREEN=5 RED=1`. The RED is `field`:
`bronze.telematics_daily` was last loaded for 2026-11-05 in that local warehouse, five business days behind.
