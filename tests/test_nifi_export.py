"""The exported NiFi flow definition (nifi/flows/evx_alert_router.json) and the offline render of
flow_spec(): no secrets, no listener, no key-based object store, the root truststore referenced by
lookup, the right topics, and the routing rule of the stream contract."""
import json
import os
import re
import sqlite3
from pathlib import Path

import pytest

from nifi import deploy_flow as D

ROOT = Path(__file__).resolve().parents[1]
CATALOG = json.loads((ROOT / "config" / "dtc_catalog.json").read_text())["dtc"]
THERMAL = sorted(c for c, d in CATALOG.items() if d["kind"] == "THERMAL")
SECRET_NAME = re.compile(r"pass(word)?|secret|token|access.?key|private.?key|credential", re.I)
SECRET_TEXT = [re.compile(p) for p in (
    r"(AKIA|ASIA)[0-9A-Z]{16}", r"-----BEGIN [A-Z ]*(PRIVATE KEY|CERTIFICATE)-----",
    r"(?i)aws_secret_access_key", r"(?i)\"[^\"]*password[^\"]*\"\s*:\s*\"(?!\$\{)[^\"]+\"",
    r"(?i)\"[^\"]*(secret|token)[^\"]*\"\s*:\s*\"(?!\$\{)[^\"]+\"")]
LIVE_SECRET_VARS = ("EVX_WORKLOAD_PASSWORD", "EVX_KAFKA_PASSWORD", "EVX_IMPALA_PASSWORD", "EVX_CAI_API_KEY",
                    "EVX_VIZ_API_KEY", "EVX_HF_TOKEN")


def _flows():
    return {"committed": json.loads(D.EXPORT.read_text()), "offline": D.render_offline(D.flow_spec())}


@pytest.fixture(params=["committed", "offline"])
def flow(request):
    return _flows()[request.param]


def components(flow):
    return list(D._walk_components(flow["flowContents"]))


def processors(flow, short):
    return [p for p in flow["flowContents"]["processors"] if p["type"].split(".")[-1] == short]


def test_export_file_exists_and_passes_export_checks(flow):
    assert D.EXPORT.name == "evx_alert_router.json"
    assert D.check_export(flow) == []


def test_every_sensitive_property_is_null(flow):
    sensitive = 0
    for comp in components(flow):
        for name, d in comp.get("propertyDescriptors", {}).items():
            if d.get("sensitive"):
                sensitive += 1
                assert comp["properties"].get(name) is None, f"{comp['name']}: {name}"
    assert sensitive >= 1  # the Kafka SASL password is declared sensitive


def test_no_secret_values_anywhere(flow):
    for comp in components(flow):
        for name, value in comp.get("properties", {}).items():
            if SECRET_NAME.search(name):
                assert value is None or re.fullmatch(r"\$\{[A-Z0-9_]+\}", value), f"{comp['name']}: {name}"
    text = json.dumps(flow)
    hits = [p.pattern for p in SECRET_TEXT if p.search(text)]
    assert not hits
    leaked = [v for v in LIVE_SECRET_VARS if len(os.environ.get(v, "")) >= 4 and os.environ[v] in text]
    assert not leaked  # names only: never compare-print a secret value


def test_environment_values_are_placeholders(flow):
    kafka = next(c for c in flow["flowContents"]["controllerServices"] if c["type"].endswith("Kafka3ConnectionService"))
    p = kafka["properties"]
    assert p["bootstrap.servers"] == "${EVX_KAFKA_BOOTSTRAP}"
    assert p["sasl.username"] == "${EVX_WORKLOAD_USER}"
    assert p["sasl.password"] is None
    assert (p["security.protocol"], p["sasl.mechanism"]) == ("SASL_SSL", "PLAIN")


def test_no_forbidden_components(flow):
    text = json.dumps(flow)
    for forbidden in ("CdpCredentialsProviderControllerService", "ListenHTTP", "HandleHttpRequest",
                      "PutCDPObjectStore", "AWSCredentialsProviderControllerService"):
        assert forbidden not in text
    fc = flow["flowContents"]
    assert fc["inputPorts"] == [] and fc["outputPorts"] == [] and fc["remoteProcessGroups"] == []
    assert all(c["type"].split(".")[-1] not in D.FORBIDDEN_TYPES for c in components(flow))


def test_ssl_context_is_the_existing_root_service_by_reference(flow):
    services = flow["flowContents"]["controllerServices"]
    assert not [s for s in services if "SSLContext" in s["type"]], "no SSL context service inside the group"
    external = flow["externalControllerServices"]
    kafka = next(s for s in services if s["type"].endswith("Kafka3ConnectionService"))
    ref = kafka["properties"]["SSL Context Service"]
    assert ref in external
    assert external[ref]["name"] == D.NIFI["ssl_context_service"] == "rsingh Truststore-only NiFi SSL Context Service"
    text = json.dumps(flow).lower()
    for embedded in ("truststore filename", "keystore filename", "truststore password", "keystore password"):
        assert embedded not in text


def test_consumes_only_dtc_and_publishes_only_alert_priority(flow):
    consume, publish = processors(flow, "ConsumeKafka"), processors(flow, "PublishKafka")
    assert [p["properties"]["Topics"] for p in consume] == [D.NIFI["topics"]["in"]["name"]] == ["evx.vehicle.dtc"]
    assert [p["properties"]["Topic Name"] for p in publish] == [D.NIFI["topics"]["out"]["name"]] == ["evx.alert.priority"]
    assert consume[0]["properties"]["Group ID"] == D.NIFI["consumer_group"]
    assert consume[0]["properties"]["Topic Format"] == "names"


def test_topic_names_match_streaming_json_when_lane_a_keys_exist():
    topics = json.loads((ROOT / "config" / "streaming.json").read_text()).get("topics", {})
    missing = [k for k in ("dtc", "alert_priority") if k not in topics]
    if missing:
        pytest.skip(f"config/streaming.json has no topics.{', topics.'.join(missing)} yet (lane A adds them)")
    assert topics["dtc"]["name"] == D.NIFI["topics"]["in"]["name"] == D.topic("in")
    assert topics["alert_priority"]["name"] == D.NIFI["topics"]["out"]["name"] == D.topic("out")


def _queries(flow):
    route = processors(flow, "QueryRecord")
    assert len(route) == 1
    p = route[0]
    dynamic = {k: v for k, v in p["properties"].items() if p["propertyDescriptors"][k].get("dynamic")}
    return p, dynamic


def test_routing_expression_has_exactly_the_thermal_codes_and_critical(flow):
    _, q = _queries(flow)
    assert set(q) == {"thermal", "critical"}
    assert THERMAL == ["BMS-T01", "MCU-T10"]
    for name in ("thermal", "critical"):
        listed = re.search(r"IN \(([^)]*)\)", q[name]).group(1)
        assert sorted(re.findall(r"'([^']+)'", listed)) == THERMAL
    assert "severity = 'CRITICAL'" in q["critical"]
    assert "'THERMAL' AS reason" in q["thermal"] and "'CRITICAL' AS reason" in q["critical"]


def test_routing_follows_the_catalog_not_a_hard_coded_list(monkeypatch):
    monkeypatch.setattr(D, "thermal_codes", lambda: ["BMS-T01", "MCU-T10", "OBC-T99"])
    q = {k: v for k, v in D.flow_spec().by_key("route").props.items() if k in ("thermal", "critical")}
    assert "'OBC-T99'" in q["thermal"] and "'OBC-T99'" in q["critical"]


def test_routing_sql_behaviour(flow):
    """Run the two QueryRecord statements on sample DTCs (sqlite stands in for Calcite)."""
    _, q = _queries(flow)
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE FLOWFILE (event_id, vin, event_ts, dtc_code, dtc_desc, severity, odo_km)")
    rows = [("e1", "V1", "2026-11-09T10:00:00Z", "BMS-T01", "cell over-temp", "HIGH", 100.0),
            ("e2", "V2", "2026-11-09T10:01:00Z", "MCU-T10", "inverter hot", "CRITICAL", 5.0),
            ("e3", "V3", "2026-11-09T10:02:00Z", "ABS-W30", "wheel sensor", "CRITICAL", 9.0),
            ("e4", "V4", "2026-11-09T10:03:00Z", "BMS-I02", "imbalance", "HIGH", 1.0),
            ("e5", "V5", "2026-11-09T10:04:00Z", "CHG-C05", "charger comms", "LOW", 2.0),
            ("e6", "V6", "2026-11-09T10:05:00Z", None, None, "CRITICAL", 3.0)]
    db.executemany("INSERT INTO FLOWFILE VALUES (?,?,?,?,?,?,?)", rows)
    out = {}
    for rel in ("thermal", "critical"):
        cur = db.execute(q[rel])
        cols = [c[0] for c in cur.description]
        assert tuple(cols) == D.ALERT_FIELDS
        for r in cur.fetchall():
            rec = dict(zip(cols, r))
            assert rec["alert_id"] not in out, "a DTC must leave on one relationship only"
            out[rec["alert_id"]] = rec
    assert {k: v["reason"] for k, v in out.items()} == {"e1": "THERMAL", "e2": "THERMAL", "e3": "CRITICAL",
                                                        "e6": "CRITICAL"}
    assert all(v["priority"] == "P1" and v["source_topic"] == "evx.vehicle.dtc" and v["routed_at"] is None
               for v in out.values())
    assert out["e2"]["severity"] == "CRITICAL" and out["e1"]["vin"] == "V1"


def test_routed_records_are_stamped_then_published_and_failures_hold(flow):
    fc = flow["flowContents"]
    names = {p["identifier"]: p["name"] for p in fc["processors"]}
    edges = {(names[c["source"]["id"]], names[c["destination"]["id"]], r)
             for c in fc["connections"] for r in c["selectedRelationships"]}
    route, stamp = "Route thermal and critical DTCs", "Stamp routed_at"
    assert {(route, stamp, "thermal"), (route, stamp, "critical"), (stamp, "Publish evx.alert.priority", "success"),
            ("Consume evx.vehicle.dtc", route, "success")} <= edges
    hold = [p for p in fc["processors"] if p["name"].startswith("Hold")]
    assert len(hold) == 1
    to_hold = {(s, r) for s, d, r in edges if d == hold[0]["name"]}
    assert to_hold == {("Consume evx.vehicle.dtc", "parse failure"), (route, "failure"), (stamp, "failure"),
                       ("Publish evx.alert.priority", "failure")}
    assert not [e for e in edges if e[0] == hold[0]["name"]]
    query = next(p for p in fc["processors"] if p["name"] == route)
    assert query["autoTerminatedRelationships"] == ["original"]  # unmatched DTCs are dropped, not errors
    stamp_p = next(p for p in fc["processors"] if p["name"] == stamp)
    assert stamp_p["properties"]["/routed_at"].startswith("${now():format(")


def test_alert_schema_matches_the_contract_fields():
    schema = json.loads(D.ALERT_SCHEMA)
    assert [f["name"] for f in schema["fields"]] == list(D.ALERT_FIELDS)
    assert set(D.ALERT_FIELDS) == {"alert_id", "vin", "event_ts", "dtc_code", "severity", "reason", "priority",
                                   "source_topic", "routed_at"}
