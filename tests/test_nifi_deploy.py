"""nifi/deploy_flow.py against a fake NiFi REST API (no network): the platform_ctl interface,
GET-only retries, deploy refusing an existing group and leaving processors stopped, start keeping
Hold* stopped, dry runs writing nothing, validate catching bad properties, export sanitising, and no
credential ever printed."""
import importlib.util
import json
from pathlib import Path

import pytest
import requests

from nifi import deploy_flow as D

ROOT = Path(__file__).resolve().parents[1]
USER, PASSWORD, BOOTSTRAP = "user-SENTINEL-7f3", "pw-SENTINEL-9q2x", "broker-SENTINEL-1:9093"
PG_NAME = D.NIFI["process_group"]
RELS = {"ConsumeKafka": ["success"], "QueryRecord": ["failure", "original"], "UpdateRecord": ["failure", "success"],
        "PublishKafka": ["failure", "success"], "UpdateAttribute": ["success"]}


class Resp:
    def __init__(self, status, body):
        self.status_code = status
        self.text = body if isinstance(body, str) else json.dumps(body)

    def json(self):
        return json.loads(self.text)


class FakeNifi:
    """Just enough of the NiFi 2 REST API for deploy_flow.py; records every request."""

    def __init__(self, groups=(), procs=None, fail=None):
        self.calls, self.auth, self.n = [], None, 0
        self.groups = [self._group(g, i) for i, g in enumerate(groups)]
        self.procs = procs or []
        self.fail = fail or {}            # (method, path prefix) -> list of responses / exceptions
        self.spec = D.flow_spec()

    @staticmethod
    def _group(name, i):
        return {"id": f"pg-{i}", "component": {"name": name}, "runningCount": 3, "stoppedCount": 1, "invalidCount": 0}

    def _definition(self, typ):
        same = [c for c in self.spec.components() if c.type == typ]
        comp = same[0]
        apis = {"ssl": "org.apache.nifi.ssl.SSLContextService",
                **{s.key: "x." + s.apis[0] for s in self.spec.services}}
        descs = {}
        for c in same:
            for k, v in c.props.items():
                if k in c.dynamic:
                    continue
                d = {"name": k, "displayName": k, "required": False, "sensitive": k in c.sensitive}
                if isinstance(v, D.Ref):
                    d["typeProvidedByValue"] = {"type": apis[v.key]}
                descs[k] = d
        return {"propertyDescriptors": descs, "supportsDynamicProperties": bool(comp.dynamic),
                "supportedRelationships": [{"name": r} for r in RELS.get(comp.short, [])]}

    def request(self, method, url, timeout=None, **kw):
        path = url.split("/nifi-api", 1)[-1].split("/nifi-registry-api", 1)[-1]
        self.calls.append((method, path, kw.get("json")))
        for (m, prefix), queue in self.fail.items():
            if m == method and path.startswith(prefix) and queue:
                item = queue.pop(0)
                if isinstance(item, Exception):
                    raise item
                return item
        return self.route(method, path, kw.get("json"))

    def route(self, method, path, body):
        if method == "GET":
            if path == "/flow/about":
                return Resp(200, {"about": {"version": "2.6.0-fake"}})
            if path == "/flow/process-groups/root":
                return Resp(200, {"processGroupFlow": {"id": "root-id"}})
            if path == "/flow/process-groups/root-id":
                return Resp(200, {"processGroupFlow": {"id": "root-id", "flow": {"processGroups": self.groups}}})
            if path == "/flow/process-groups/root-id/controller-services":
                return Resp(200, {"controllerServices": [{"id": "ssl-id", "component": {
                    "name": D.NIFI["ssl_context_service"], "parentGroupId": "root-id", "state": "ENABLED",
                    "controllerServiceApis": [{"type": "org.apache.nifi.ssl.SSLContextService"}]}}]})
            if path == "/flow/registries":
                return Resp(200, {"registries": [{"id": "reg-1", "component": {"name": D.NIFI["registry_client"]}}]})
            if path in ("/flow/processor-types", "/flow/controller-service-types"):
                kind = "processor" if "processor" in path else "controller-service"
                key = "processorTypes" if kind == "processor" else "controllerServiceTypes"
                return Resp(200, {key: [{"type": c.type, "bundle": {"group": "org.apache.nifi", "artifact": c.artifact,
                                                                    "version": "2.6.0"}}
                                        for c in self.spec.components() if c.kind == kind]})
            if "-definition/" in path:
                return Resp(200, self._definition(path.rsplit("/", 1)[-1]))
            if path.endswith("/controller-services"):
                pg = path.split("/")[3]
                return Resp(200, {"controllerServices": [
                    {"component": {"name": s.name, "parentGroupId": pg, "state": "ENABLED"}} for s in self.spec.services]})
            if path.endswith("/processors"):
                return Resp(200, {"processors": self.procs})
            if path.startswith("/process-groups/pg-"):
                return Resp(200, {"revision": {"version": 1}, "component": {"id": path.split("/")[2]}})
            if path == "/buckets":
                return Resp(200, [])
            return Resp(404, "not found")
        self.n += 1
        if method == "POST" and path == "/process-groups/root-id/process-groups":
            self.groups.append({"id": "pg-new", "component": {"name": body["component"]["name"]},
                                "runningCount": 0, "stoppedCount": 0, "invalidCount": 0})
            return Resp(201, {"id": "pg-new", "revision": {"version": 0}})
        if method == "POST":
            return Resp(201, {"id": f"id-{self.n}", "identifier": f"id-{self.n}", "revision": {"version": 0},
                              "versionControlInformation": {"version": 1}})
        if method == "PUT" and path.endswith("/run-status"):
            pid = path.split("/")[2]
            for p in self.procs:
                if p["id"] == pid:
                    p["component"]["state"] = body["state"]
            return Resp(200, {})
        if method == "PUT" and path.startswith("/flow/process-groups/") and path.count("/") == 3:
            for p in self.procs:
                if p["component"]["state"] != "DISABLED":
                    p["component"]["state"] = body["state"]
            return Resp(200, {})
        return Resp(200, {})

    def writes(self):
        return [(m, p) for m, p, _ in self.calls if m != "GET"]


def _proc(pid, name, state="STOPPED"):
    return {"id": pid, "revision": {"version": 1},
            "component": {"name": name, "state": state, "validationStatus": "VALID"}}


@pytest.fixture
def fake(monkeypatch):
    for var, value in ((D.ENV["user"], USER), (D.ENV["password"], PASSWORD), (D.ENV["kafka_bootstrap"], BOOTSTRAP)):
        monkeypatch.setenv(var, value)
    monkeypatch.setattr(D.time, "sleep", lambda s: None)
    box = {"fake": FakeNifi()}
    monkeypatch.setattr(D.requests, "Session", lambda: box["fake"])
    return box


def test_platform_ctl_interface(fake):
    fake["fake"] = FakeNifi(groups=["Other demo", PG_NAME])
    n = D.Nifi()
    g = n.find_group(PG_NAME)
    assert g["id"] == "pg-1" and {"runningCount", "stoppedCount", "invalidCount"} <= set(g)
    assert n.find_group("missing") is None
    n.call("PUT", f"/flow/process-groups/{g['id']}", json={"id": g["id"], "state": "STOPPED"})
    assert fake["fake"].calls[-1] == ("PUT", "/flow/process-groups/pg-1", {"id": "pg-1", "state": "STOPPED"})
    assert fake["fake"].auth == (USER, PASSWORD)


def test_credentials_come_only_from_the_environment(monkeypatch):
    monkeypatch.delenv(D.ENV["user"], raising=False)
    monkeypatch.delenv(D.ENV["password"], raising=False)
    with pytest.raises(SystemExit, match="EVX_WORKLOAD_USER"):
        D.Nifi()


def test_get_is_retried_and_writes_never_are(fake):
    f = FakeNifi(fail={("GET", "/flow/about"): [requests.ConnectionError("x"), Resp(503, "busy")],
                       ("POST", "/process-groups/"): [requests.ConnectionError("x")],
                       ("PUT", "/processors/"): [Resp(503, "busy")]})
    fake["fake"] = f
    n = D.Nifi()
    assert n.call("GET", "/flow/about")["about"]["version"] == "2.6.0-fake"
    assert [c[1] for c in f.calls].count("/flow/about") == 3
    with pytest.raises(RuntimeError, match="ConnectionError"):
        n.call("POST", "/process-groups/x/processors", json={})
    with pytest.raises(RuntimeError, match="HTTP 503"):
        n.call("PUT", "/processors/p1", json={})
    assert len(f.writes()) == 2


def test_deploy_refuses_an_existing_group(fake):
    fake["fake"] = FakeNifi(groups=[PG_NAME])
    with pytest.raises(SystemExit, match="already exists"):
        D.build(D.Nifi(), D.flow_spec())
    assert fake["fake"].writes() == []


def test_deploy_builds_everything_stopped_and_references_the_root_ssl_service(fake):
    f = fake["fake"]
    D.build(D.Nifi(), D.flow_spec())
    posts = [(p, b) for m, p, b in f.calls if m == "POST"]
    assert posts[0][0] == "/process-groups/root-id/process-groups" and posts[0][1]["component"]["name"] == PG_NAME
    created = [b["component"]["name"] for p, b in posts if p.endswith("/processors")]
    assert created == [p.name for p in D.flow_spec().processors]
    assert sum(p.endswith("/connections") for p, _ in posts) == 7
    services = [b["component"]["type"] for p, b in posts if p.endswith("/controller-services")]
    assert not [t for t in services if "SSLContext" in t or "Credentials" in t]
    # no run-status call at all: every processor stays STOPPED after deploy
    assert not [c for c in f.calls if c[1].endswith("/run-status")]
    # nothing outside the new group is written (root services, the truststore, other groups)
    assert all(p.startswith(("/process-groups/pg-new", "/controller-services/id-", "/processors/id-",
                             "/flow/process-groups/pg-new", "/process-groups/root-id/process-groups"))
               for m, p in f.writes())
    kafka_put = next(b for m, p, b in f.calls if m == "PUT" and p.startswith("/controller-services/")
                     and "SSL Context Service" in b["component"]["properties"])
    props = kafka_put["component"]["properties"]
    assert props["SSL Context Service"] == "ssl-id"
    assert props["bootstrap.servers"] == BOOTSTRAP and props["sasl.password"] == PASSWORD


def test_validate_passes_and_catches_bad_properties(fake, monkeypatch):
    n = D.Nifi(dry=True)
    assert D.validate(n, D.flow_spec()) == []
    spec = D.flow_spec()
    spec.by_key("consume").props["Topic Formatt"] = "names"
    spec.by_key("kafka").sensitive = frozenset()
    spec.connections.remove(("stamp", "hold", ["failure"]))
    problems = D.validate(D.Nifi(dry=True), spec)
    assert any("unknown property 'Topic Formatt'" in p for p in problems)
    assert any("'sasl.password' sensitive=True" in p for p in problems)
    assert any("Stamp routed_at: relationships neither connected nor auto-terminated ['failure']" in p for p in problems)
    assert fake["fake"].writes() == []


def test_validate_rejects_forbidden_components():
    spec = D.flow_spec()
    spec.processors.append(D.Component("listen", "processor", "ListenHTTP",
                                       "org.apache.nifi.processors.standard.ListenHTTP", "nifi-standard-nar",
                                       "Listen", {}))
    assert any("ListenHTTP" in p for p in D.check_spec_static(spec))


def test_dry_run_deploy_writes_nothing(fake):
    D.build(D.Nifi(dry=True), D.flow_spec())
    assert fake["fake"].writes() == []


def test_start_keeps_hold_stopped_and_starts_the_rest(fake):
    f = FakeNifi(groups=[PG_NAME], procs=[_proc("a", "Consume evx.vehicle.dtc"), _proc("b", "Publish evx.alert.priority"),
                                          _proc("h", "Hold failures (inspect, then replay)", "RUNNING")])
    fake["fake"] = f
    D.start(D.Nifi())
    states = {p["component"]["name"][:4]: p["component"]["state"] for p in f.procs}
    assert states == {"Cons": "RUNNING", "Publ": "RUNNING", "Hold": "STOPPED"}
    assert not [c for c in f.calls if c[1] == "/flow/process-groups/pg-0" and c[0] == "PUT"]  # no group-level start
    D.stop(D.Nifi())
    assert f.calls[-1] == ("PUT", "/flow/process-groups/pg-0", {"id": "pg-0", "state": "STOPPED"})


def test_platform_ctl_start_keeps_hold_stopped(fake, monkeypatch):
    f = FakeNifi(groups=[PG_NAME], procs=[_proc("a", "Consume evx.vehicle.dtc"), _proc("h", "Hold failures (x)")])
    fake["fake"] = f
    spec = importlib.util.spec_from_file_location("evx_platform_ctl_nifi", ROOT / "scripts" / "platform_ctl.py")
    P = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(P)
    P.nifi_project("start")
    assert {p["component"]["name"][:4]: p["component"]["state"] for p in f.procs} == {"Cons": "RUNNING", "Hold": "STOPPED"}
    P.nifi_project("status")


def test_version_creates_the_bucket_and_commits(fake):
    fake["fake"] = FakeNifi(groups=[PG_NAME])
    assert D.version(D.Nifi(), "first") == 1
    posts = [(p, b) for m, p, b in fake["fake"].calls if m == "POST"]
    assert posts[0] == ("/buckets", {"name": "evx-flows", "description": posts[0][1]["description"]})
    body = posts[1][1]["versionedFlow"]
    assert posts[1][0] == "/versions/process-groups/pg-0"
    assert (body["flowName"], body["registryId"], body["action"]) == ("evx_alert_router", "reg-1", "COMMIT")


def _live_like_snapshot():
    """An offline render with what a live download would add: real values and an embedded truststore."""
    flow = D.render_offline(D.flow_spec())
    kafka = next(c for c in flow["flowContents"]["controllerServices"] if c["type"].endswith("Kafka3ConnectionService"))
    kafka["properties"].update({"bootstrap.servers": BOOTSTRAP, "sasl.username": USER, "sasl.password": PASSWORD})
    flow["snapshotMetadata"] = {"author": USER, "comments": "x"}
    return flow


def test_sanitize_removes_secrets_and_environment_values():
    env = {D.ENV["user"]: USER, D.ENV["password"]: PASSWORD, D.ENV["kafka_bootstrap"]: BOOTSTRAP}
    clean = D.sanitize(_live_like_snapshot(), env)
    text = json.dumps(clean)
    assert USER not in text and PASSWORD not in text and BOOTSTRAP not in text
    assert "snapshotMetadata" not in clean
    assert D.check_export(clean) == []
    # only whole values are replaced: a workload user that is part of the SSL service name keeps the name intact
    short_user = D.sanitize(D.render_offline(D.flow_spec()), {D.ENV["user"]: "rsingh"})
    assert D.check_export(short_user) == []
    leaky = _live_like_snapshot()
    leaky["flowContents"]["processors"][0]["comments"] = PASSWORD
    with pytest.raises(SystemExit, match="export refused"):
        D.sanitize(leaky, env)


def test_check_export_flags_an_embedded_truststore():
    flow = D.render_offline(D.flow_spec())
    flow["flowContents"]["controllerServices"].append({
        "componentType": "CONTROLLER_SERVICE", "name": "copied truststore", "properties": {},
        "type": "org.apache.nifi.ssl.StandardRestrictedSSLContextService"})
    flow["externalControllerServices"] = {}
    problems = D.check_export(flow)
    assert any("embedded SSL context service" in p for p in problems)
    assert any("not referenced as an external controller service" in p for p in problems)


def test_live_export_writes_a_sanitised_file(fake, monkeypatch, tmp_path):
    f = FakeNifi(groups=[PG_NAME])
    snapshot = _live_like_snapshot()
    orig = f.route
    f.route = lambda m, p, b: (Resp(200, snapshot) if p.startswith("/process-groups/pg-0/download") else orig(m, p, b))
    fake["fake"] = f
    monkeypatch.setattr(D, "EXPORT", tmp_path / "nifi" / "flows" / "evx_alert_router.json")
    monkeypatch.setattr(D, "ROOT", tmp_path)
    D.export(D.Nifi())
    text = D.EXPORT.read_text()
    assert PASSWORD not in text and USER not in text and BOOTSTRAP not in text
    assert any("includeReferencedServices=false" in c[1] for c in f.calls)
    assert f.writes() == []


def test_no_credentials_printed_in_any_command(fake, capsys):
    fake["fake"] = FakeNifi()
    D.main(["validate"])
    D.main(["deploy"])
    fake["fake"] = FakeNifi(groups=[PG_NAME], procs=[_proc("a", "Consume evx.vehicle.dtc"), _proc("h", "Hold failures")])
    for argv in (["start"], ["stop"], ["version", "--dry-run"], ["export", "--dry-run"]):
        try:
            D.main(argv)
        except SystemExit:
            pass
    # an error response that echoes values must come back redacted
    fake["fake"] = FakeNifi(fail={("PUT", "/processors/"): [Resp(400, f"bad value {BOOTSTRAP} for {USER}/{PASSWORD}")]})
    with pytest.raises(RuntimeError) as err:
        D.Nifi().call("PUT", "/processors/p1", json={})
    out = capsys.readouterr()
    text = out.out + out.err + str(err.value)
    leaked = [name for name, v in (("user", USER), ("password", PASSWORD), ("bootstrap", BOOTSTRAP)) if v in text]
    assert not leaked
    assert "<EVX_WORKLOAD_PASSWORD>" in str(err.value)
