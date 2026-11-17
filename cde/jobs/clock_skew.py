"""
Station clock skew in silver.line_event, measured against the MES receive time (mes_ts, the trusted clock).

  offset     per (station_id, _business_date): median(event_ts - mes_ts) in seconds. Normal MES latency
             (mes_ts 0.5..3 s after the cycle end) gives a median of a few seconds below zero.
  correct    when |offset| > tolerance_s (config/operations.json clock_skew.tolerance_s, default 120), the
             offset rounded to the nearest round_s (60) is subtracted from cycle_start_ts and event_ts;
             mes_ts and cycle_s are untouched (a constant skew leaves the cycle time as it was)
  columns    clock_offset_s (int, the seconds subtracted, 0 when not corrected) and clock_corrected (boolean)

The correction never reads the planted fault (config/lakehouse.json faults.clock_skew): it finds a skew from
the data alone. The integrator calls it in the line_event branch of cde/jobs/build_silver.py:

  from clock_skew import correct_clock_skew, tolerance
  df = correct_clock_skew(df, tolerance())
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import evx_common as C  # noqa: E402

KEYS = ("station_id", "_business_date")
SHIFTED = ("cycle_start_ts", "event_ts")
ADDED = ("clock_offset_s", "clock_corrected")
DEFAULT_TOLERANCE_S = 120
DEFAULT_ROUND_S = 60


def settings() -> dict:
    try:
        return C.load_json("config/operations.json").get("clock_skew", {})
    except OSError:
        return {}


def tolerance() -> float:
    return float(settings().get("tolerance_s", DEFAULT_TOLERANCE_S))


def offset_to_subtract(median_s: float | None, tolerance_s: float, round_s: int = DEFAULT_ROUND_S) -> int:
    """The same rule as correct_clock_skew, for one median (tests, docs)."""
    if median_s is None or abs(median_s) <= tolerance_s:
        return 0
    return int(math.floor(abs(median_s) / round_s + 0.5) * round_s) * (1 if median_s > 0 else -1)


def correct_clock_skew(df, tolerance_s: float = DEFAULT_TOLERANCE_S, round_s: int | None = None):
    """df with station_id, _business_date, cycle_start_ts, event_ts, mes_ts (timestamp or timestamp_ntz) ->
    the same rows and column order, the two PLC timestamps corrected where the station's clock was off, plus
    clock_offset_s and clock_corrected."""
    from pyspark.sql import functions as F

    round_s = int(round_s or settings().get("round_s", DEFAULT_ROUND_S))

    def secs(c):
        return F.unix_micros(F.col(c).cast("timestamp")) / F.lit(1e6)

    median = F.median(secs("event_ts") - secs("mes_ts"))
    offsets = df.groupBy(*KEYS).agg(median.alias("_median_s")) \
        .withColumn("clock_corrected", F.coalesce(F.abs("_median_s") > F.lit(float(tolerance_s)), F.lit(False))) \
        .withColumn("clock_offset_s",
                    F.when(F.col("clock_corrected"),
                           (F.signum("_median_s") * F.floor(F.abs("_median_s") / round_s + 0.5) * round_s)
                           .cast("int")).otherwise(F.lit(0))) \
        .drop("_median_s")
    base = [c for c in df.columns if c not in ADDED]
    out = df.select(*base).join(F.broadcast(offsets), list(KEYS), "left") \
        .withColumn("clock_offset_s", F.coalesce("clock_offset_s", F.lit(0))) \
        .withColumn("clock_corrected", F.coalesce("clock_corrected", F.lit(False)))
    for c in SHIFTED:
        out = out.withColumn(c, F.col(c) - F.make_dt_interval(secs=F.col("clock_offset_s").cast("decimal(18,6)")))
    return out.select(*base, *ADDED)
