# Late EDR dumps

Event-recorder (EDR) dumps land days after their event. `gold.fact_edr_event` is partitioned by the arrival
business date (the landing folder), so nightly gold already holds every dump in its arrival partition. The
aggregate keyed by the event date, `gold.edr_event_daily`, is the table a late dump makes stale.

## Job

`cde/jobs/reprocess_late.py`, CDE job `rsingh-evx-reprocess`, runs after gold for the same business date.

```bash
spark-submit reprocess_late.py --business-date 2026-11-10        # nightly
spark-submit reprocess_late.py --from 2026-02-09 --to 2026-11-05  # initial load
```

- **Touched dates.** It reads the dumps of the arrival dates processed and takes their event dates (the IST date
  of `trigger_ts`). It recomputes `gold.edr_event_daily` for exactly those event dates in one `publish()` commit
  (mode `dates`), and for no other date.
- **Idempotent.** A rerun for the same arrival date recomputes the same event dates from every dump in gold and
  writes the same rows. Replaying an older arrival date never drops a dump that arrived later.
- **Late.** Only the landing date is known, not the upload time. With `late_data.sla_hours` 72 in
  `config/operations.json`, a dump is late when it lands on or after event date + floor(72 / 24) + 1, so on day 4.
  Normal upload delays (`volumes.edr_delay_hours`, up to 72 h from a trigger between 07:00 and 21:00 IST) land
  by day 3 at the latest, so normal traffic is never late. A late dump re-opens a closed event date.
- **Log.** `ref.reprocess_log` has one row per arrival date processed; a rerun replaces it. Columns: event dates
  touched and re-opened, dumps and late dumps, events before and after in `gold.edr_event_daily`, snapshots
  before and after, and status `REPROCESSED` (late dumps), `ON_TIME`, `NO_DATA` or `FAILED`.
- **Recon.** Arrival dates with status `REPROCESSED` give the observed row (gold, `fact_edr_event`,
  `late reprocess`), matched against fault 8 in `config/expected_recon.json`.
- Writes only `rsingh_evx_*` tables and reads no planted-fault config.

## Planted fault

The planted dumps of event date 2026-11-06 must land on day 4 or later to be distinguishable from normal
traffic. A landing on 2026-11-09 (day 3) is within the normal delay range.

## Local evidence (CI scale 0.05)

`REPROCESS_ROW arrival=2026-11-10 status=REPROCESSED touched=2026-11-06 reopened=2026-11-06 late_dumps=1 of 1
events_before=1 events_after=1`; recon for 2026-11-10 classifies (gold, `fact_edr_event`, `late reprocess`)
as EXPLAINED, fault 8.
