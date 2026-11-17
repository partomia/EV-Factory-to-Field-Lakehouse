"""Clock skew: the planter (generators/clock_skew.py) and the silver correction (cde/jobs/clock_skew.py)."""
import shutil
from datetime import datetime, timedelta, timezone

import pytest

from generators import clock_skew as planter
import clock_skew as cs

IST = timezone(timedelta(hours=5, minutes=30))
CFG = {"faults": {"clock_skew": {"date": "2026-11-09", "station": "VA-EOL-02", "seconds": 5400}}}


def _iso(dt):
    return dt.isoformat(timespec="seconds")


def event(station, start, cycle_s=60, latency_s=2):
    end = start + timedelta(seconds=cycle_s)
    return {"event_id": f"{station}-{start:%Y%m%d%H%M%S}", "station_id": station, "line_id": "VA2",
            "area": "VEHICLE", "unit_id": "U1", "cycle_start_ts": _iso(start), "event_ts": _iso(end),
            "mes_ts": _iso(end + timedelta(seconds=latency_s)), "cycle_s": float(cycle_s), "status": "OK",
            "shift": "A"}


# ---------------------------------------------------------------- planter


def test_planter_shifts_station_clock_only_on_skew_date():
    e = event("VA-EOL-02", datetime(2026, 11, 9, 10, 0, tzinfo=IST))
    out = planter.apply(e, CFG)
    assert out is not e and e["event_ts"] == "2026-11-09T10:01:00+05:30"
    assert out["cycle_start_ts"] == "2026-11-09T11:30:00+05:30"
    assert out["event_ts"] == "2026-11-09T11:31:00+05:30"
    assert out["mes_ts"] == e["mes_ts"] and out["cycle_s"] == e["cycle_s"]


@pytest.mark.parametrize("station,day", [("VA-EOL-01", 9), ("VA-EOL-02", 8), ("VA-EOL-02", 10)])
def test_planter_leaves_other_stations_and_dates(station, day):
    e = event(station, datetime(2026, 11, day, 10, 0, tzinfo=IST))
    assert planter.apply(e, CFG) == e


def test_planter_business_date_is_ist_date_of_mes_ts():
    # cycle ends 23:59:59 IST on 11-08, MES receives it 00:00:01 IST on 11-09: that event belongs to 11-09
    e = event("VA-EOL-02", datetime(2026, 11, 8, 23, 58, 59, tzinfo=IST), cycle_s=60, latency_s=2)
    assert planter.business_date(e["mes_ts"]) == "2026-11-09"
    assert planter.apply(e, CFG)["event_ts"] == "2026-11-09T01:29:59+05:30"
    assert planter.business_date("2026-11-08T20:00:00Z") == "2026-11-09"


def test_planter_without_fault_is_identity():
    e = event("VA-EOL-02", datetime(2026, 11, 9, 10, 0, tzinfo=IST))
    assert planter.apply(e, {}) == e and planter.truth_rows({}) == []


def test_truth_row_shape():
    assert planter.truth_rows(CFG) == [{"fault": "clock_skew", "date": "2026-11-09", "key": "VA-EOL-02",
                                        "value": "5400"}]


def test_planter_matches_lakehouse_fault():
    import evx_common as C

    s = C.load_json("config/lakehouse.json")["faults"]["clock_skew"]
    assert (s["station"], s["date"], int(s["seconds"])) == ("VA-EOL-02", "2026-11-09", 5400)


# ---------------------------------------------------------------- correction rule (no Spark)


@pytest.mark.parametrize("median,expected", [
    (None, 0), (0.0, 0), (-1.8, 0), (90.0, 0), (-120.0, 0), (120.0, 0),
    (121.0, 120), (5398.2, 5400), (5429.0, 5400), (5431.0, 5460), (-5398.2, -5400), (-140.0, -120), (150.0, 180),
])
def test_offset_rule(median, expected):
    assert cs.offset_to_subtract(median, 120) == expected


def test_tolerance_from_operations_json():
    assert cs.tolerance() == 120.0


def test_correction_never_reads_the_planted_fault():
    with open(cs.__file__) as f:
        code = f.read().split('"""', 2)[2]
    assert "faults" not in code and "lakehouse" not in code


# ---------------------------------------------------------------- correction on Spark


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark")
    if not shutil.which("java"):
        pytest.skip("no Java for local Spark")
    from scripts.run_local import local_spark

    return local_spark(tmp_path_factory.mktemp("warehouse"), driver_memory="2g")


def frame(spark, ntz: bool):
    """Three stations on 11-09 (VA-EOL-02 planted +5400 s, VA-EOL-01 a 90 s drift, PL1-EOL-01 clean), and
    VA-EOL-02 clean on 11-08."""
    rows = []
    day9, day8 = datetime(2026, 11, 9, 8, 0, tzinfo=IST), datetime(2026, 11, 8, 8, 0, tzinfo=IST)
    for i in range(11):
        lat = 0.5 + 0.25 * i
        rows.append(planter.apply(event("VA-EOL-02", day9 + timedelta(minutes=3 * i), latency_s=lat), CFG))
        rows.append(event("VA-EOL-02", day8 + timedelta(minutes=3 * i), latency_s=lat))
        rows.append(event("PL1-EOL-01", day9 + timedelta(minutes=3 * i), latency_s=lat))
        e = event("VA-EOL-01", day9 + timedelta(minutes=3 * i), latency_s=lat)
        for c in ("cycle_start_ts", "event_ts"):
            e[c] = _iso(datetime.fromisoformat(e[c]) + timedelta(seconds=90))
        rows.append(e)
    from pyspark.sql import functions as F

    df = spark.createDataFrame(rows).withColumn("_business_date", F.to_date(F.from_utc_timestamp(F.to_timestamp("mes_ts"), "+05:30")))
    for c in ("cycle_start_ts", "event_ts", "mes_ts"):
        df = df.withColumn(c, F.to_timestamp(c))
        if ntz:
            df = df.withColumn(c, F.col(c).cast("timestamp_ntz"))
    return df


@pytest.mark.parametrize("ntz", [False, True])
def test_correction_on_spark(spark, ntz):
    from pyspark.sql import functions as F

    df = frame(spark, ntz)
    out = cs.correct_clock_skew(df, 120)
    assert out.columns == df.columns + ["clock_offset_s", "clock_corrected"]
    assert out.count() == df.count() == 44
    types = {f.name: f.dataType.simpleString() for f in out.schema.fields}
    assert types["event_ts"] == ("timestamp_ntz" if ntz else "timestamp")
    assert (types["clock_offset_s"], types["clock_corrected"]) == ("int", "boolean")

    def secs(c):
        return F.unix_micros(F.col(c).cast("timestamp")) / F.lit(1e6)

    got = {(r[0], str(r[1])): (r[2], r[3], r[4], r[5]) for r in out.groupBy("station_id", "_business_date").agg(
        F.max("clock_offset_s"), F.min("clock_offset_s"), F.bool_and("clock_corrected"),
        F.median(secs("event_ts") - secs("mes_ts"))).collect()}
    planted = got[("VA-EOL-02", "2026-11-09")]
    assert planted[:3] == (5400, 5400, True)
    assert -3 <= planted[3] <= 0, "corrected residual is MES latency only"
    drift = got[("VA-EOL-01", "2026-11-09")]
    assert drift[:3] == (0, 0, False) and 85 <= drift[3] <= 90, "90 s drift is inside tolerance, left alone"
    for k in (("PL1-EOL-01", "2026-11-09"), ("VA-EOL-02", "2026-11-08")):
        assert got[k][:3] == (0, 0, False)

    # untouched rows are byte-identical; corrected rows keep mes_ts and cycle_s
    key = ["event_id"]
    a = df.alias("a").join(out.alias("b"), key)
    assert a.where("b.clock_corrected = false AND (a.event_ts <> b.event_ts OR a.cycle_start_ts <> b.cycle_start_ts)"
                   ).count() == 0
    assert a.where("a.mes_ts <> b.mes_ts OR a.cycle_s <> b.cycle_s").count() == 0
    assert a.where("b.clock_corrected AND (unix_timestamp(a.event_ts) - unix_timestamp(b.event_ts) <> 5400 OR "
                   "unix_timestamp(a.cycle_start_ts) - unix_timestamp(b.cycle_start_ts) <> 5400)").count() == 0


def test_correction_is_idempotent_on_rerun(spark):
    out = cs.correct_clock_skew(frame(spark, True), 120)
    again = cs.correct_clock_skew(out, 120)
    assert again.columns == out.columns
    assert again.where("clock_corrected").count() == 0, "a corrected frame measures no skew the second time"
