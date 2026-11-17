"""Twin source (generators/twin.py): deterministic, unique ids, parseable by the bronze parsers, sized as planned."""
import hashlib
import importlib
import json
import re
import sys
import types
from collections import Counter
from datetime import date, datetime

import pytest

from generators import twin as T

DAY = date(2026, 11, 9)


@pytest.fixture(scope="module")
def cfg(ci_cfg):
    return ci_cfg[0]


@pytest.fixture(scope="module")
def scale(ci_cfg):
    return ci_cfg[0]["_scale"]


@pytest.fixture(scope="module")
def records(cfg, scale):
    """Twin records for the skew date, a week start (2026-11-02) and the day before line events start."""
    return list(T.generate_twin(cfg, [date(2026, 10, 23), date(2026, 11, 2), DAY], scale))


def _digest(recs) -> str:
    h = hashlib.sha256()
    for r in recs:
        h.update(repr(r[:2]).encode())
        h.update(r[2] if r[0] == "file" else json.dumps(r[1], sort_keys=True).encode())
    return h.hexdigest()


def test_deterministic(cfg, scale, records):
    assert _digest(T.generate_twin(cfg, [date(2026, 10, 23), date(2026, 11, 2), DAY], scale)) == _digest(records)
    assert T.station_cycle_events(cfg, DAY, scale) == T.station_cycle_events(cfg, DAY, scale)


def test_layout_and_window(records):
    files = [r[1] for r in records if r[0] == "file"]
    assert not [f for f in files if f.startswith("line_event/2026-10-23/")], "no line events before line_events_from"
    assert sorted(f for f in files if f.startswith(f"line_event/{DAY}/")) == \
        [f"line_event/{DAY}/{ln}.csv" for ln in ("CL1", "CL2", "PL1", "PL2", "VA1", "VA2")]
    assert sorted(f for f in files if f.startswith("twin_scenario/")) == \
        [f"twin_scenario/{w}/{ln}.ndjson" for w in ("2026-11-02", "2026-11-09")
         for ln in ("CL1", "CL2", "PL1", "PL2", "VA1", "VA2")]
    assert {r[0] for r in records} <= {"file", "truth_twin", "truth_fault"}


def test_event_ids_unique_and_consistent(cfg, scale):
    ev = T.station_cycle_events(cfg, DAY, scale)
    assert len({e["event_id"] for e in ev}) == len(ev)
    for e in ev:
        assert e["event_id"].startswith(f"{e['station_id']}-20261109-")
        assert datetime.fromisoformat(e["mes_ts"]).date() == DAY, "business date = IST date of mes_ts"
        start, end = (datetime.fromisoformat(e[k]) for k in ("cycle_start_ts", "event_ts"))
        assert e["cycle_s"] == (end - start).total_seconds() > 0
        assert e["station_id"].endswith(f"-{int(e['line_id'][-1]):02d}")
    cells = [e["unit_id"] for e in ev if e["area"] == "CELL"]
    assert all(re.fullmatch(r"CBCL[12][0-9]{6}[ABC][0-9]{4}", u) for u in cells), "cell lines run real cell ids"


def test_statuses_shifts_and_bottleneck(cfg, scale):
    ev = T.station_cycle_events(cfg, DAY, scale)
    st = Counter(e["status"] for e in ev)
    assert set(st) == {"OK", "FAULT", "BLOCKED", "STARVED"} and st["OK"] > 0.9 * len(ev)
    assert set(Counter(e["shift"] for e in ev)) == {"A", "B", "C"}
    ok = [e for e in ev if e["status"] == "OK" and e["line_id"] == "CL1"]
    mean = {s: sum(e["cycle_s"] for e in ok if e["station_id"] == s) / max(1, sum(e["station_id"] == s for e in ok))
            for s in {e["station_id"] for e in ok}}
    assert max(mean, key=mean.get) == "CA-EFL-01", "the designed bottleneck is the slowest station"


def test_va_eol_02_reports_on_the_skew_date(cfg, scale):
    assert any(e["station_id"] == "VA-EOL-02" for e in T.station_cycle_events(cfg, DAY, scale))


def test_volume_proportional_to_scale(cfg, scale):
    n_ci = len(T.station_cycle_events(cfg, DAY, scale))
    n_default = len(T.station_cycle_events(cfg, DAY, 1.0))
    assert 2_500 <= n_ci <= 6_000, n_ci
    assert 55_000 <= n_default <= 70_000, n_default   # PLAN.md section 13: line about 60k per date


def test_scenarios_weekly_over_the_calendar(cfg, scale):
    weeks = T.week_starts(cfg, scale)
    assert weeks[0] == date(2026, 2, 9) and weeks[-1] == date(2026, 11, 9) and len(weeks) == 40
    docs = [doc for w in weeks for doc in T.twin_scenarios(cfg, w, scale)]
    assert 350 <= len(docs) <= 450
    assert len({x["scenario_id"] for x in docs}) == len(docs)
    for doc in docs[:60]:
        assert sum(s["bottleneck"] for s in doc["stations"]) == 1
        top = max(doc["stations"], key=lambda s: s["predicted_cycle_s"])
        assert top["bottleneck"] and top["station_id"] == doc["bottleneck_station"]
        assert len(doc["stations"]) == doc["n_stations"]


def test_truth_records(records):
    truth = [r[1] for r in records if r[0] == "truth_twin"]
    assert truth and all(t["kind"] == "scenario" for t in truth)
    docs = {json.loads(line)["scenario_id"]: json.loads(line) for r in records if r[0] == "file"
            and r[1].startswith("twin_scenario/") for line in r[2].decode().splitlines()}
    assert {t["scenario_id"] for t in truth} == set(docs)
    assert all(docs[t["scenario_id"]]["bottleneck_station"] == t["designed_bottleneck"] for t in truth)
    assert not any("tool_change_station" in json.dumps(d) for d in docs.values()), "truth stays out of the source"


def _valid(c: dict, v):
    if v in (None, ""):
        return not c.get("required")
    if c["type"] == "timestamp":
        return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}", v))
    if c["type"] == "date":
        return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", v))
    if c["type"] in ("double", "int"):
        x = float(v)
        if c["type"] == "int" and x != int(x):
            return False
        return x >= c.get("min", float("-inf"))
    if "pattern" in c and not re.fullmatch(c["pattern"].strip("^$"), v):
        return False
    return "allowed" not in c or v in c["allowed"]


def test_bronze_parsers_read_generated_files(records):
    """The generic contract-driven bronze parsers read both sources with no code change; every value passes."""
    import evx_common as C
    from extract.records import delimited_rows, ndjson_rows

    contracts = C.contracts()
    seen = Counter()
    for kind, rel, data in (r for r in records if r[0] == "file"):
        src, day = rel.split("/")[:2]
        c = next(c for c in contracts.values() if c["source"] == src)
        if c["format"] == "csv":
            rows, stats = delimited_rows(f"x/{rel}", data.decode(), c)
            assert stats["header_ok"] and not stats["has_trailer"] and not c.get("trailer")
        else:
            rows = ndjson_rows(f"x/{rel}", data.decode(), c)
            assert all(r[-1] is None for r in rows), "no malformed JSON"
        for r in rows:
            for col, v in zip(c["columns"], r[2:]):
                assert _valid(col, v), (c["entity"], col["name"], v)
        keys = Counter((day,) + tuple(r[2 + [x["name"] for x in c["columns"]].index(k)] for k in c["key"]) for r in rows)
        assert max(keys.values()) == 1
        seen[c["entity"]] += len(rows)
    assert seen["line_event"] > 2_500 and seen["twin_scenario"] >= 6


def test_skew_hook_falls_back_and_is_called(monkeypatch, cfg, scale):
    try:
        monkeypatch.setitem(sys.modules, "generators.clock_skew", None)   # import raises ImportError
        t = importlib.reload(T)
        assert t._skew({"a": 1}, cfg) == {"a": 1}
        plain = t.station_cycle_events(cfg, DAY, scale)

        fake = types.ModuleType("generators.clock_skew")
        fake.apply = lambda e, c: {**e, "event_ts": "SKEWED"} if e["station_id"] == "VA-EOL-02" else e
        fake.truth_rows = lambda c: [{"fault": "clock_skew", "date": "2026-11-09", "key": "VA-EOL-02"}]
        monkeypatch.setitem(sys.modules, "generators.clock_skew", fake)
        t = importlib.reload(T)
        hooked = t.station_cycle_events(cfg, DAY, scale)
        assert [e["event_ts"] == "SKEWED" for e in hooked] == [e["station_id"] == "VA-EOL-02" for e in plain]
        assert [e["event_id"] for e in hooked] == [e["event_id"] for e in plain], "ids assigned before the hook"
        faults = [r for r in t.generate_twin(cfg, [DAY], scale) if r[0] == "truth_fault"]
        assert faults == [("truth_fault", {"fault": "clock_skew", "date": "2026-11-09", "key": "VA-EOL-02"})]
    finally:
        monkeypatch.undo()
        importlib.reload(T)
