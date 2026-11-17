# Station clock skew (planted fault 11)

Station `VA-EOL-02` (vehicle EOL test, line VA2) has its PLC clock 5400 s (90 minutes) ahead on business
date 2026-11-09. Silver finds the offset from the data alone, corrects it, and recon reports it as fault 11.
All data is synthetic.

## Clocks

| column | clock | trusted |
|---|---|---|
| `cycle_start_ts`, `event_ts` | station PLC | no: a station clock can drift or be set wrong |
| `mes_ts` | MES server receive time (cycle end + 0.5..3 s) | yes |
| `cycle_s` | `event_ts - cycle_start_ts` | a constant skew leaves it unchanged |

The business date of a line event is the IST date of `mes_ts`.

## Planter: `generators/clock_skew.py`

`apply(event, cfg)` returns a new event dict. When `station_id` and the business date match
`config/lakehouse.json` `faults.clock_skew` (`station`, `date`, `seconds`), it adds `seconds` to
`cycle_start_ts` and `event_ts`, keeping the `+05:30` ISO format. `mes_ts` and `cycle_s` stay as they are.
Lane E's `generators/twin.py` passes every line event through it before writing the event out.
`truth_rows(cfg)` gives the `ref.generator_truth_fault` row
(`{"fault": "clock_skew", "date": "2026-11-09", "key": "VA-EOL-02", "value": "5400"}`).

## Correction: `cde/jobs/clock_skew.py`

`correct_clock_skew(df, tolerance_s)`, called in the line_event section of `cde/jobs/build_silver.py`:

1. offset = median(`event_ts - mes_ts`) in seconds per (`station_id`, `_business_date`). A healthy station
   has a median of a few seconds below zero (the MES latency).
2. If |offset| > `tolerance_s` (`config/operations.json` `clock_skew.tolerance_s`, 120), subtract the
   offset rounded to the nearest `round_s` (60) from `cycle_start_ts` and `event_ts`. Otherwise do nothing.
3. Add `clock_offset_s` (int, the seconds subtracted, 0 when nothing was corrected) and `clock_corrected`
   (boolean).

The correction never reads `faults.clock_skew`. A 90 s drift is inside the tolerance and is left alone.
Run the correction once, on the typed bronze rows: on an already corrected frame it measures no skew and
resets the two flag columns.

Call site (integrator, lane E's `twin_tables`):

```python
from clock_skew import correct_clock_skew, tolerance as skew_tolerance
df = correct_clock_skew(typed_entity(b, contracts["line_event"]), skew_tolerance())
```

The streaming line-cycle window (lane A) windows on `mes_ts`, so the skewed station does not shift its
windows and needs no correction there.

## Recon

Exception source `(silver, line_event, skew)`: rows of `silver.line_event` with `clock_corrected`, per
`_business_date`, the detail listing `station offset s`. It is skipped while the table does not exist.
`config/expected_recon.json` fault 11 (2026-11-09, EXPLAINED) matches it. A corrected station on any other
date has no expected row, so recon reports it UNEXPECTED and fails. Fault 11 is checked for MISSING once
`M2` is in `built_milestones`.

## Evidence (local, CI scale 0.05)

Lane E's events for 2026-11-08 and 2026-11-09 (8,200 rows, 112 station-dates) with the planter:
`VA-EOL-02` on 2026-11-09 had a median of 5397.5 s before the correction and -2.5 s after it, with
`clock_offset_s` 5400. It was the only station-date corrected; every other station-date had |median| of at
most 3 s. Recon observed `2026-11-09 silver line_event skew 2 'VA-EOL-02 5400 s'` and marked it EXPLAINED,
fault 11. When `VA-EOL-01` was moved by -600 s on 2026-11-08, recon reported it UNEXPECTED.
Unit tests: `tests/test_d_skew.py`.
