"""M2 streams: topic isolation from config/streaming.json, contracts, producer determinism and control totals,
DTC routing fields for the NiFi alert router, the line producer's injected event source, the drain guard."""
import json
import os
import re
import stat
import subprocess
import sys
from datetime import date
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "cde" / "jobs"))

from stream.producer import common as K  # noqa: E402
from stream.producer import dtc as D  # noqa: E402
from stream.producer import line as L  # noqa: E402
from stream.producer import line_fake  # noqa: E402
from stream.producer import qc as Q  # noqa: E402

CFG = json.loads((ROOT / "config" / "streaming.json").read_text())
CATALOG = json.loads((ROOT / "config" / "dtc_catalog.json").read_text())["dtc"]
CONTRACTS = {p.stem: json.loads(p.read_text()) for p in (ROOT / "contracts" / "stream").glob("*.json")}
DAY = date(2026, 11, 6)
FLEET = [{"vin": f"XEVS1K2609010{i:04d}", "delivery_date": "2026-09-05", "daily_km": 30.0, "fast_charge_share": 0.2,
          "contaminated": False, "v_spread_mv": 15.0,
          "event_date": {0: "2026-11-06", 1: "2026-11-04"}.get(i), "event_kind": {0: "thermal", 1: "imbalance"}.get(i),
          "live_subset": True, "bms_fw": "BMS-3.4.2", "geohash5": "te7mu"} for i in range(400)]
QC_CSV = (Q.HEADER + "\n"
          "CBCL1261106A0000|CBCL1261106A|CL1|XRAY|XR-CL1-01|2026-11-06T06:10:00+05:30|PASS||0.0657||\n"
          "CBCL1261106A0001|CBCL1261106A|CL1|XRAY|XR-CL1-01|2026-11-06T06:17:42+05:30|FAIL|XR-OVH|1.2000|100,60,110,70|"
          "qc_xray/2026-11-06/CBCL1261106A0001.png\n"
          "CBCL1261106AS5|CBCL1261106A|CL1|FILM|VF-CL1-01|2026-11-06T11:30:00+05:30|PASS||0.2249||\nTRL|3\n")


def validate(msg: dict, contract: dict) -> list[str]:
    """The bronze jobs' contract rules, in Python."""
    errs = []
    for f in contract["fields"]:
        v = msg.get(f["name"])
        if v is None or v == "":
            if f.get("required"):
                errs.append(f"{f['name']}:REQUIRED")
            continue
        if f["type"] == "timestamp":
            K.parse_ts(v)
        elif f["type"] in ("double", "bigint", "int"):
            float(v)
        if "pattern" in f and not re.search(f["pattern"], str(v)):
            errs.append(f"{f['name']}:PATTERN")
        if "allowed" in f and v not in f["allowed"]:
            errs.append(f"{f['name']}:NOT_ALLOWED")
    return errs


class Files:
    def __init__(self, files):
        self.files = files

    def list(self, uri):
        return sorted((Path(k).name, len(v)) for k, v in self.files.items() if str(Path(k).parent) == uri)

    def read_text(self, uri):
        return self.files[uri]


# ---------------------------------------------------------------- config: topics, jobs, checkpoints


def test_each_bronze_job_subscribes_only_to_its_own_topics():
    import stream_common as S
    import stream_telemetry_bronze as TB

    assert TB.TOPICS == {"telemetry": "evx.vehicle.telemetry", "control": "evx.control"}
    owners = {}
    for key, t in CFG["topics"].items():
        assert t["name"].startswith("evx.")
        if t.get("bronze_job"):
            owners.setdefault(t["bronze_job"], set()).add(key)
            assert t["bronze_job"] in CFG["jobs"]
    assert owners == {"rsingh-evx-stream-telemetry": {"telemetry", "control"}, "rsingh-evx-stream-dtc": {"dtc"},
                      "rsingh-evx-stream-qc": {"qc"}, "rsingh-evx-stream-line": {"line"}}
    for job, keys in owners.items():
        if job != TB.JOB:
            assert set(S.job_topics(job)) == keys


def test_alert_topic_is_derived_and_subscribed_by_nobody():
    import stream_common as S

    alert = CFG["topics"]["alert_priority"]
    assert alert["name"] == "evx.alert.priority" and alert["bronze_job"] is None and alert["derived"]
    for job in CFG["jobs"]:
        assert "alert_priority" not in S.job_topics(job)
    assert CONTRACTS["evx.alert.priority"]["table"] is None


def test_checkpoints_and_quarantines_are_not_shared():
    import stream_common as S

    cps = [j["checkpoint"] for j in CFG["jobs"].values()]
    assert len(cps) == len(set(cps))
    for job in ("rsingh-evx-stream-dtc", "rsingh-evx-stream-qc", "rsingh-evx-stream-line"):
        assert S.checkpoint_name(job) == job.split("stream-")[1] + "_bronze"
    assert CFG["jobs"]["rsingh-evx-stream-line-window"]["checkpoint"] not in S.TELEMETRY_CHECKPOINTS
    quarantines = {k: t.get("quarantine") for k, t in CFG["topics"].items() if t.get("bronze_job")}
    assert quarantines == {"telemetry": "stream_quarantine", "control": None, "dtc": "dtc_quarantine",
                           "qc": "qc_quarantine", "line": "line_quarantine"}


def test_watermarks_and_trigger():
    import stream_common as S

    assert S.seconds(CFG["topics"]["dtc"]["watermark"]) == 1800
    assert all(S.seconds(CFG["topics"][k]["watermark"]) == 600 for k in ("telemetry", "qc", "line"))
    assert f"{CFG['spark']['trigger_seconds']} seconds" == "60 seconds"


def test_window_jobs_come_after_their_bronze_job():
    names = list(CFG["jobs"])
    for name, j in CFG["jobs"].items():
        if j.get("after"):
            assert names.index(j["after"]) < names.index(name)
            reads = j["reads_table"].split(".")[1]
            assert any(t.get("table") == reads and t.get("bronze_job") == j["after"] for t in CFG["topics"].values())


def test_producer_holds_exceed_largest_delay():
    for p in CFG["producers"].values():
        if isinstance(p, dict):
            worst = p["latency_s"][1] + max(p["late_seconds"][1], p["very_late_seconds"][1], 4.0)
            assert p["control_hold_s"] > worst


# ---------------------------------------------------------------- contracts


def test_every_topic_has_a_valid_contract():
    for key, t in CFG["topics"].items():
        c = CONTRACTS[t["name"]]
        assert c["topic"] == t["name"]
        if t.get("bronze_job"):
            assert c["table"] == t["table"]
            assert t["event_time"] in {f["name"] for f in c["fields"] if f["type"] == "timestamp"}
        for f in c["fields"]:
            assert f["type"] in {"string", "double", "bigint", "int", "timestamp"}
            assert isinstance(f["required"], bool)
            if "pattern" in f:
                re.compile(f["pattern"])
    dtc = CONTRACTS["evx.vehicle.dtc"]
    assert next(f for f in dtc["fields"] if f["name"] == "dtc_code")["allowed"] == sorted(CATALOG)


# ---------------------------------------------------------------- DTC


def test_dtc_deterministic_and_valid():
    f = set()
    a = [m for v in FLEET for m in D.vehicle_day(v, DAY, f, "r")]
    b = [m for v in FLEET for m in D.vehicle_day(v, DAY, f, "r")]
    assert a == b and a
    ids = [m["event_id"] for _, m in a]
    assert len(ids) == len(set(ids))
    for _, m in a:
        assert validate(m, CONTRACTS["evx.vehicle.dtc"]) == []


def test_dtc_battery_codes_follow_the_field_event():
    first = [m for _, m in D.vehicle_day(FLEET[0], DAY, set(), "r")]
    assert any(m["dtc_code"] == "BMS-T01" and m["severity"] == "CRITICAL" for m in first)
    # imbalance event on 11-04: repeats every second day (11-06 is j=1 when the vehicle has >= 2 repeats)
    later = [m for d in (date(2026, 11, 7), date(2026, 11, 9)) for _, m in D.vehicle_day(FLEET[1], d, set(), "r")]
    assert not any(m["dtc_code"] == "BMS-I02" for m in later)


def test_dtc_routing_fields_for_nifi():
    """Every message carries what the router needs; some route to P1 (THERMAL kind or CRITICAL severity)."""
    sink = K.ListSink()
    D.backfill(sink, FLEET, [DAY, date(2026, 11, 7)], "r", log=lambda s: None)
    dtcs = [m for t, m, _ in sink.msgs if t == "evx.vehicle.dtc"]
    for m in dtcs:
        assert m["vin"] and m["event_id"] and m["event_ts"] and m["dtc_code"] in CATALOG
        assert m["severity"] in ("LOW", "MEDIUM", "HIGH", "CRITICAL")
    routed = [m for m in dtcs if CATALOG[m["dtc_code"]]["kind"] == "THERMAL" or m["severity"] == "CRITICAL"]
    assert routed and len(routed) < len(dtcs)


def test_history_dates_are_refused():
    with pytest.raises(ValueError):
        D.backfill(K.ListSink(), FLEET, [date(2026, 11, 5)], "r", log=lambda s: None)
    with pytest.raises(ValueError):
        L.backfill(K.ListSink(), {}, [date(2026, 11, 5)], 0.05, "r", events=line_fake.station_cycle_events,
                   log=lambda s: None)


# ---------------------------------------------------------------- control totals


def control_ok(msgs: list, topic: str, n: int) -> None:
    data = [m for t, m, _ in msgs if t == topic]
    ctl = [m for t, m, _ in msgs if t == "evx.control"]
    assert len(data) == n > 0
    assert {c["topic"] for c in ctl} == {topic}
    assert sum(c["total"] for c in ctl) == n
    assert len({c["minute"] for c in ctl}) == len(ctl)
    per_minute = {}
    for m in data:
        k = K.minute_utc(m["event_ts"] if topic != "evx.factory.line" else m["mes_ts"])
        per_minute[k] = per_minute.get(k, 0) + 1
    assert {c["minute"]: c["total"] for c in ctl} == per_minute
    sends = [s for t, _, s in msgs if t == topic]
    assert sends == sorted(sends)


def test_dtc_control_totals():
    sink = K.ListSink()
    n = D.backfill(sink, FLEET, [DAY, date(2026, 11, 7)], "r", log=lambda s: None)
    control_ok(sink.msgs, "evx.vehicle.dtc", n)


def test_qc_one_message_per_verdict_row_and_control_totals():
    files = Files({"/g/cell_qc/2026-11-06/CL1_qc_A.csv": QC_CSV, "/g/cell_qc/2026-11-06/_manifest.json": "{}"})
    sink = K.ListSink()
    n = Q.backfill(sink, files, "/g", [DAY], "r", log=lambda s: None)
    assert n == 3
    control_ok(sink.msgs, "evx.factory.qc", n)
    msgs = [m for t, m, _ in sink.msgs if t == "evx.factory.qc"]
    assert {m["event_id"] for m in msgs} == {"QC-CBCL1261106A0000-XRAY", "QC-CBCL1261106A0001-XRAY",
                                            "QC-CBCL1261106AS5-FILM"}
    for m in msgs:
        assert validate(m, CONTRACTS["evx.factory.qc"]) == [] and m["business_date"] == "2026-11-06"
    again = K.ListSink()
    Q.backfill(again, files, "/g", [DAY], "r", log=lambda s: None)
    assert again.msgs == sink.msgs


def test_qc_without_exports_fails():
    with pytest.raises(FileNotFoundError):
        Q.backfill(K.ListSink(), Files({}), "/g", [DAY], "r", log=lambda s: None)


# ---------------------------------------------------------------- line


def test_line_producer_does_not_import_the_twin_at_module_load():
    code = ("import sys; sys.path.insert(0, %r); import stream.producer.line; "
            "assert 'generators.twin' not in sys.modules" % str(ROOT))
    subprocess.run([sys.executable, "-c", code], check=True)


def test_line_events_from_an_injected_source():
    calls = []

    def stub(cfg, day, scale):
        calls.append((day, scale))
        return line_fake.station_cycle_events(cfg, day, scale)

    sink = K.ListSink()
    n = L.backfill(sink, {"seed": 1}, [DAY], 0.05, "r", events=stub, log=lambda s: None)
    assert calls == [(DAY, 0.05)]
    control_ok(sink.msgs, "evx.factory.line", n)
    msgs = [m for t, m, _ in sink.msgs if t == "evx.factory.line"]
    for m in msgs[:200]:
        assert validate(m, CONTRACTS["evx.factory.line"]) == []
    again = K.ListSink()
    L.backfill(again, {"seed": 1}, [DAY], 0.05, "r", events=stub, log=lambda s: None)
    assert again.msgs == sink.msgs


def test_line_event_missing_a_field_is_refused():
    with pytest.raises(KeyError):
        L.message({"event_id": "x", "station_id": "VA-EOL-02"}, "r")


def test_message_keys():
    assert K.record_key({"vin": "V", "topic": "t"}) == "V"
    assert K.record_key({"station_id": "VA-EOL-02", "unit_id": "U"}) == "VA-EOL-02"
    assert K.record_key({"unit_id": "C1"}) == "C1"
    assert K.record_key({"topic": "evx.factory.qc"}) == "evx.factory.qc"


# ---------------------------------------------------------------- producer job and deploy script


def test_produce_topic_filter_defaults_to_telemetry():
    import stream_produce as SP

    assert SP.parse_topics("telemetry") == ["telemetry"]
    assert SP.parse_topics("line,dtc,qc") == ["dtc", "qc", "line"]
    with pytest.raises(SystemExit):
        SP.parse_topics("alert_priority")
    src = (ROOT / "cde" / "jobs" / "stream_produce.py").read_text()
    assert 'add_argument("--topics", default="telemetry"' in src
    assert "alterConfigs" not in src and "createPartitions" not in src and "incrementalAlterConfigs" not in src


def fake_cde(tmp_path: Path, running_job: str | None) -> tuple[dict, Path]:
    log = tmp_path / "cde.log"
    script = tmp_path / "bin" / "cde"
    script.parent.mkdir()
    script.write_text(f"""#!/usr/bin/env bash
echo "$*" >> {log}
case "$1 $2" in
  "run list") if [[ "$*" == *"job[eq]{running_job}"* ]]; then echo '[{{"id": 7, "status": "running"}}]'; else echo '[]'; fi ;;
  "job run") echo '{{"id": 42}}' ;;
  "run describe") echo '{{"status": "succeeded"}}' ;;
esac
""")
    script.chmod(script.stat().st_mode | stat.S_IEXEC)
    env = {k: v for k, v in os.environ.items() if not k.startswith("EVX_")}
    env["PATH"] = f"{script.parent}:{env['PATH']}"
    return env, log


def test_drain_refuses_while_a_continuous_run_is_active(tmp_path):
    env, log = fake_cde(tmp_path, "rsingh-evx-stream-line-window")
    r = subprocess.run([str(ROOT / "cde" / "scripts" / "deploy_streaming.sh"), "drain-m2"], env=env,
                       capture_output=True, text=True)
    assert r.returncode == 1 and "refusing" in r.stdout
    assert "job run" not in log.read_text()


def test_drain_m2_runs_in_dependency_order_and_leaves_telemetry_alone(tmp_path):
    env, log = fake_cde(tmp_path, "rsingh-evx-stream-telemetry")
    r = subprocess.run([str(ROOT / "cde" / "scripts" / "deploy_streaming.sh"), "drain-m2"], env=env,
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stdout + r.stderr
    runs = [ln.split("--name ")[1].split()[0] for ln in log.read_text().splitlines() if ln.startswith("job run")]
    assert runs == ["rsingh-evx-stream-dtc", "rsingh-evx-stream-qc", "rsingh-evx-stream-line",
                    "rsingh-evx-stream-line-window"]
    assert all("--arg=available-now" in ln for ln in log.read_text().splitlines() if ln.startswith("job run"))
