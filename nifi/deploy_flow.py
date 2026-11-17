"""
NiFi flow "EVX - Alert Router" as code, on the federal-nifi Data Hub (NiFi 2, REST via Knox).

  set -a; source .env; set +a
  python nifi/deploy_flow.py status                # group counts, per-processor state, registry version
  python nifi/deploy_flow.py validate              # every property against the live definitions; read-only
  python nifi/deploy_flow.py deploy [--dry-run]    # create the group, services enabled, processors STOPPED
  python nifi/deploy_flow.py version -m "..."      # commit to NiFi Registry bucket evx-flows / evx_alert_router
  python nifi/deploy_flow.py export                # registry snapshot (or live group) -> nifi/flows/evx_alert_router.json
  python nifi/deploy_flow.py export --offline      # the same file rendered from flow_spec(), no NiFi call
  python nifi/deploy_flow.py start | stop          # "Hold*" processors always stay stopped

Every write command takes --dry-run: GETs run, every POST / PUT is printed and skipped.

The flow
  ConsumeKafka evx.vehicle.dtc (Kafka3ConnectionService, SASL_SSL PLAIN, workload user; the existing
  root truststore SSL context service, looked up by name and referenced, never copied or changed)
    -> QueryRecord: "thermal"  = dtc_code of catalog kind THERMAL (config/dtc_catalog.json, read at deploy)
                    "critical" = severity CRITICAL and not thermal (THERMAL wins when both)
                    every other DTC is dropped (not an error)
    -> UpdateRecord: routed_at = NiFi time, ISO UTC
    -> PublishKafka evx.alert.priority (alert shape of the stream contract, key = vin)
  Parse, query, update and publish failures queue in front of the stopped "Hold failures" processor.

Only the EVX process group, its own controller services and the evx-flows bucket are created. No
object-store writer, no CDP access keys, no listener and no new port. Credentials come from
EVX_WORKLOAD_USER / EVX_WORKLOAD_PASSWORD (sensitive NiFi property, never printed or logged).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

import requests


def _repo_root() -> Path:
    try:
        return Path(__file__).resolve().parents[1]
    except NameError:  # CAI job kernels run scripts without __file__; cwd is the project
        return Path.cwd()


ROOT = _repo_root()
NIFI = json.loads((ROOT / "nifi" / "config.json").read_text())
EXPORT = ROOT / "nifi" / "flows" / f"{NIFI['flow_name']}.json"
ENV = NIFI["env"]
GET_TRIES = 4
HOLD_PREFIX = "Hold"
FORBIDDEN_TYPES = ("ListenHTTP", "HandleHttpRequest", "HandleHttpResponse", "ListenTCP", "ListenUDP",
                   "ListenSyslog", "CdpCredentialsProviderControllerService", "PutCDPObjectStore",
                   "AWSCredentialsProviderControllerService")


def say(msg: str) -> None:
    print(msg, flush=True)


def topic(role: str) -> str:
    """Topic name for role in / out: config/streaming.json when it has the key, else nifi/config.json."""
    t = NIFI["topics"][role]
    streaming = json.loads((ROOT / "config" / "streaming.json").read_text()).get("topics", {})
    return (streaming.get(t["streaming_key"]) or {}).get("name") or t["name"]


def thermal_codes() -> list[str]:
    catalog = json.loads((ROOT / "config" / "dtc_catalog.json").read_text())["dtc"]
    codes = sorted(c for c, d in catalog.items() if d.get("kind") == "THERMAL")
    for c in codes:
        if not re.fullmatch(r"[A-Z0-9-]+", c):
            raise ValueError(f"DTC code {c!r} is not safe to put in a SQL literal")
    return codes


# ------------------------------------------------------------------ flow definition (data)
@dataclass(frozen=True)
class Ref:
    """A controller service: a key of this flow's services, or "ssl" for the existing root service."""
    key: str


@dataclass(frozen=True)
class Env:
    """A value read from the environment at deploy time; exported as ${VAR}, or null when sensitive."""
    var: str


@dataclass
class Component:
    key: str
    kind: str                     # processor | controller-service
    short: str
    type: str
    artifact: str
    name: str
    props: dict
    sensitive: frozenset = frozenset()
    dynamic: frozenset = frozenset()      # dynamic properties (QueryRecord / UpdateRecord)
    apis: tuple = ()                      # controller-service APIs this service provides
    pos: tuple = (0, 0)
    auto_terminate: tuple = ()
    extra_rels: frozenset = frozenset()   # relationships present only for some property values

    @property
    def rels(self) -> set:
        return set(self.dynamic) if self.short == "QueryRecord" else set()


@dataclass
class FlowSpec:
    services: list
    processors: list
    connections: list = field(default_factory=list)   # (source key, destination key, [relationships])

    def components(self) -> list:
        return self.services + self.processors

    def by_key(self, key: str) -> Component:
        return next(c for c in self.components() if c.key == key)


def _avro(name: str, fields: list[tuple[str, bool]]) -> str:
    return json.dumps({"type": "record", "name": name, "namespace": "evx.stream", "fields": [
        {"name": f, "type": "string"} if required else {"name": f, "type": ["null", "string"], "default": None}
        for f, required in fields]}, separators=(",", ":"))


# only the fields the router reads; extra DTC fields are ignored by the reader
DTC_SCHEMA = _avro("evx_vehicle_dtc_route", [(f, False) for f in ("event_id", "vin", "event_ts", "dtc_code", "severity")])
ALERT_FIELDS = ("alert_id", "vin", "event_ts", "dtc_code", "severity", "reason", "priority", "source_topic", "routed_at")
ALERT_SCHEMA = _avro("evx_alert_priority", [(f, f in ("alert_id", "reason", "priority", "source_topic"))
                                            for f in ALERT_FIELDS])
ROUTED_AT = "${now():format(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\", \"GMT\")}"


def route_sql(reason: str, codes: list[str], in_topic: str) -> str:
    if reason not in ("THERMAL", "CRITICAL"):
        raise ValueError(f"route reason {reason!r}")
    for v in (in_topic, NIFI["alert_priority"], *codes):
        if not re.fullmatch(r"[A-Za-z0-9._-]+", v):
            raise ValueError(f"{v!r} is not safe to put in a SQL literal")
    listed = ", ".join(f"'{c}'" for c in codes)
    cols = (f"SELECT event_id AS alert_id, vin, event_ts, dtc_code, severity, '{reason}' AS reason, "
            f"'{NIFI['alert_priority']}' AS priority, '{in_topic}' AS source_topic, "
            "CAST(NULL AS VARCHAR) AS routed_at FROM FLOWFILE WHERE ")
    if reason == "THERMAL":
        return cols + f"dtc_code IN ({listed})"
    return cols + f"severity = 'CRITICAL' AND (dtc_code IS NULL OR dtc_code NOT IN ({listed}))"


def flow_spec() -> FlowSpec:
    in_topic, out_topic, codes = topic("in"), topic("out"), thermal_codes()
    rs = "nifi-record-serialization-services-nar"
    reader_api, writer_api = ("RecordReaderFactory",), ("RecordSetWriterFactory",)
    services = [
        Component("kafka", "controller-service", "Kafka3ConnectionService",
                  "org.apache.nifi.kafka.service.Kafka3ConnectionService", "nifi-kafka-3-service-nar",
                  "EVX Kafka (SASL_SSL PLAIN)", {
                      "bootstrap.servers": Env(ENV["kafka_bootstrap"]), "security.protocol": "SASL_SSL",
                      "sasl.mechanism": "PLAIN", "sasl.username": Env(ENV["user"]),
                      "sasl.password": Env(ENV["password"]), "SSL Context Service": Ref("ssl")},
                  sensitive=frozenset({"sasl.password"}), apis=("KafkaConnectionService",)),
        Component("dtc_reader", "controller-service", "JsonTreeReader", "org.apache.nifi.json.JsonTreeReader", rs,
                  "EVX DTC reader (JSON)", {"schema-access-strategy": "schema-text-property",
                                            "schema-text": DTC_SCHEMA}, apis=reader_api),
        Component("dtc_writer", "controller-service", "JsonRecordSetWriter", "org.apache.nifi.json.JsonRecordSetWriter",
                  rs, "EVX DTC writer (JSON lines)", {"Schema Write Strategy": "no-schema",
                                                     "schema-access-strategy": "inherit-record-schema",
                                                     "output-grouping": "output-oneline"}, apis=writer_api),
        Component("alert_reader", "controller-service", "JsonTreeReader", "org.apache.nifi.json.JsonTreeReader", rs,
                  "EVX alert reader (JSON)", {"schema-access-strategy": "schema-text-property",
                                              "schema-text": ALERT_SCHEMA}, apis=reader_api),
        Component("alert_writer", "controller-service", "JsonRecordSetWriter",
                  "org.apache.nifi.json.JsonRecordSetWriter", rs, "EVX alert writer (JSON lines)", {
                      "Schema Write Strategy": "no-schema", "schema-access-strategy": "schema-text-property",
                      "schema-text": ALERT_SCHEMA, "output-grouping": "output-oneline",
                      "suppress-nulls": "never-suppress"}, apis=writer_api),
    ]
    processors = [
        Component("consume", "processor", "ConsumeKafka", "org.apache.nifi.kafka.processors.ConsumeKafka",
                  "nifi-kafka-nar", f"Consume {in_topic}", {
                      "Kafka Connection Service": Ref("kafka"), "Group ID": NIFI["consumer_group"],
                      "Topic Format": "names", "Topics": in_topic, "auto.offset.reset": "earliest",
                      "Processing Strategy": "RECORD", "Record Reader": Ref("dtc_reader"),
                      "Record Writer": Ref("dtc_writer")},
                  pos=(0, 0), extra_rels=frozenset({"parse failure"})),
        Component("route", "processor", "QueryRecord", "org.apache.nifi.processors.standard.QueryRecord",
                  "nifi-standard-nar", "Route thermal and critical DTCs", {
                      "record-reader": Ref("dtc_reader"), "record-writer": Ref("alert_writer"),
                      "include-zero-record-flowfiles": "false",
                      "thermal": route_sql("THERMAL", codes, in_topic),
                      "critical": route_sql("CRITICAL", codes, in_topic)},
                  dynamic=frozenset({"thermal", "critical"}), pos=(0, 250), auto_terminate=("original",)),
        Component("stamp", "processor", "UpdateRecord", "org.apache.nifi.processors.standard.UpdateRecord",
                  "nifi-standard-nar", "Stamp routed_at", {
                      "Record Reader": Ref("alert_reader"), "Record Writer": Ref("alert_writer"),
                      "Replacement Value Strategy": "literal-value", "/routed_at": ROUTED_AT},
                  dynamic=frozenset({"/routed_at"}), pos=(0, 500)),
        Component("publish", "processor", "PublishKafka", "org.apache.nifi.kafka.processors.PublishKafka",
                  "nifi-kafka-nar", f"Publish {out_topic}", {
                      "Kafka Connection Service": Ref("kafka"), "Topic Name": out_topic,
                      "Record Reader": Ref("alert_reader"), "Record Writer": Ref("alert_writer"),
                      "Message Key Field": "vin"},
                  pos=(0, 750), auto_terminate=("success",)),
        Component("hold", "processor", "UpdateAttribute", "org.apache.nifi.processors.attributes.UpdateAttribute",
                  "nifi-update-attribute-nar", "Hold failures (inspect, then replay)", {},
                  pos=(550, 375), auto_terminate=("success",)),
    ]
    connections = [
        ("consume", "route", ["success"]),
        ("consume", "hold", ["parse failure"]),
        ("route", "stamp", ["thermal", "critical"]),
        ("route", "hold", ["failure"]),
        ("stamp", "publish", ["success"]),
        ("stamp", "hold", ["failure"]),
        ("publish", "hold", ["failure"]),
    ]
    return FlowSpec(services, processors, connections)


# ------------------------------------------------------------------ REST client
class Nifi:
    """NiFi REST through the Data Hub Knox gateway with workload-user basic auth.

    Interface used by scripts/platform_ctl.py: Nifi(), find_group(name) -> entity dict (id,
    runningCount, stoppedCount, invalidCount) or None, call(method, path, json=...).
    """

    def __init__(self, dry: bool = False):
        user, password = os.environ.get(ENV["user"]), os.environ.get(ENV["password"])
        if not user or not password:
            raise SystemExit(f"set {ENV['user']} and {ENV['password']} (set -a; source .env; set +a)")
        self.s = requests.Session()
        self.s.auth = (user, password)
        self.api = NIFI["api"].rstrip("/")
        self.reg = NIFI["registry_api"].rstrip("/")
        self.dry = dry
        self._types: dict = {}
        self._defs: dict = {}
        self._root: str | None = None
        self._n = 0

    @staticmethod
    def _redact(text: str) -> str:
        for var in ENV.values():
            value = os.environ.get(var)
            if value and len(value) >= 3:
                text = text.replace(value, f"<{var}>")
        return text

    def _fake(self, body) -> dict:
        self._n += 1
        fid = f"dry-{self._n}"
        comp = dict((body or {}).get("component") or {})
        comp.setdefault("id", fid)
        return {"id": fid, "identifier": fid, "revision": {"version": 0}, "component": comp}

    def call(self, method: str, path: str, base: str | None = None, **kw):
        """GETs retry on connection errors and 5xx; writes are sent once and never retried."""
        if method != "GET" and self.dry:
            say(f"  would {method} {path}")
            return self._fake(kw.get("json"))
        tries = GET_TRIES if method == "GET" else 1
        for attempt in range(1, tries + 1):
            try:
                r = self.s.request(method, (base or self.api) + path, timeout=120, **kw)
            except (requests.ConnectionError, requests.Timeout) as e:
                if attempt == tries:
                    raise RuntimeError(f"{method} {path}: {type(e).__name__}") from None
                time.sleep(2 * attempt)
                continue
            if r.status_code >= 500 and attempt < tries:
                time.sleep(2 * attempt)
                continue
            if r.status_code >= 400:
                raise RuntimeError(f"{method} {path}: HTTP {r.status_code} {self._redact(r.text[:400])}")
            return r.json() if r.text.strip().startswith(("{", "[")) else r.text
        raise RuntimeError(f"{method} {path}: no response")

    # ---- lookups (read-only)
    def root_id(self) -> str:
        if self._root is None:
            self._root = self.call("GET", "/flow/process-groups/root")["processGroupFlow"]["id"]
        return self._root

    def find_group(self, name: str) -> dict | None:
        groups = self.call("GET", f"/flow/process-groups/{self.root_id()}")["processGroupFlow"]["flow"]["processGroups"]
        return next((g for g in groups if g["component"]["name"] == name), None)

    def root_service(self, name: str) -> dict:
        """The existing root-level controller service called name (looked up, never modified)."""
        cs = self.call("GET", f"/flow/process-groups/{self.root_id()}/controller-services")["controllerServices"]
        hits = [c for c in cs if c["component"]["name"] == name and c["component"].get("parentGroupId") == self.root_id()]
        if not hits:
            raise SystemExit(f"controller service {name!r} not found on the root canvas")
        return hits[0]

    def bundle(self, kind: str, typ: str, artifact: str) -> dict:
        if kind not in self._types:
            key = "processorTypes" if kind == "processor" else "controllerServiceTypes"
            self._types[kind] = self.call("GET", f"/flow/{kind}-types")[key]
        hits = [t for t in self._types[kind] if t["type"] == typ]
        if not hits:
            raise RuntimeError(f"{kind} type {typ} not installed")
        return next((t for t in hits if t["bundle"]["artifact"] == artifact), hits[0])["bundle"]

    def definition(self, kind: str, typ: str, artifact: str) -> dict:
        if typ not in self._defs:
            b = self.bundle(kind, typ, artifact)
            self._defs[typ] = self.call("GET", f"/flow/{kind}-definition/{b['group']}/{b['artifact']}/{b['version']}/{typ}")
        return self._defs[typ]

    def registry_client_id(self) -> str:
        regs = self.call("GET", "/flow/registries")["registries"]
        hit = next((r["id"] for r in regs if r["component"]["name"] == NIFI["registry_client"]), None)
        if hit is None:
            raise SystemExit(f"NiFi registry client {NIFI['registry_client']!r} not found")
        return hit

    def processors(self, pg: str) -> list[dict]:
        return self.call("GET", f"/process-groups/{pg}/processors")["processors"]


# ------------------------------------------------------------------ validate (read-only)
def _deps_ok(descriptor: dict, values: dict, descriptors: dict) -> bool:
    for dep in descriptor.get("dependencies") or []:
        other = dep.get("propertyName")
        value = values.get(other, (descriptors.get(other) or {}).get("defaultValue"))
        wanted = dep.get("dependentValues")
        if value is None or (wanted and value not in wanted):
            return False
    return True


def check_component(comp: Component, defn: dict, service_apis: dict) -> list[str]:
    """Problems with one component's properties and relationships against its live definition."""
    problems, descs = [], defn.get("propertyDescriptors") or {}
    values = {k: (v if isinstance(v, str) else "<set>") for k, v in comp.props.items()}
    for name, value in comp.props.items():
        d = descs.get(name)
        if d is None:
            if name in comp.dynamic and defn.get("supportsDynamicProperties"):
                continue
            problems.append(f"{comp.name}: unknown property {name!r}")
            continue
        if bool(d.get("sensitive")) != (name in comp.sensitive):
            problems.append(f"{comp.name}: {name!r} sensitive={d.get('sensitive')} on NiFi, not as in flow_spec")
        allowed = [a.get("allowableValue", a).get("value") for a in d.get("allowableValues") or []]
        if allowed and isinstance(value, str) and value not in allowed:
            problems.append(f"{comp.name}: {name!r} must be one of {allowed}")
        api = ((d.get("typeProvidedByValue") or {}).get("type") or "").split(".")[-1]
        if api and not isinstance(value, Ref):
            problems.append(f"{comp.name}: {name!r} needs a controller service ({api})")
        if isinstance(value, Ref) and api and api not in service_apis.get(value.key, ()):
            problems.append(f"{comp.name}: {name!r} wants {api}; {value.key} provides {service_apis.get(value.key)}")
        if not _deps_ok(d, values, descs):
            problems.append(f"{comp.name}: {name!r} is set but its dependencies are not met (NiFi would ignore it)")
    for name, d in descs.items():
        if d.get("required") and d.get("defaultValue") is None and name not in comp.props and _deps_ok(d, values, descs):
            problems.append(f"{comp.name}: required property {name!r} not set")
    if comp.kind == "processor":
        rels = {r["name"] for r in defn.get("supportedRelationships") or []} | comp.rels | set(comp.extra_rels)
        unknown = set(comp.auto_terminate) - rels
        if unknown:
            problems.append(f"{comp.name}: unknown auto-terminated relationships {sorted(unknown)}")
    return problems


def check_wiring(spec: FlowSpec, live_rels: dict) -> list[str]:
    problems, used = [], {p.key: set(p.auto_terminate) for p in spec.processors}
    for src, dst, rels in spec.connections:
        comp = spec.by_key(src)
        unknown = set(rels) - live_rels[src]
        if unknown:
            problems.append(f"{comp.name}: unknown relationships {sorted(unknown)}")
        used[src] |= set(rels)
        spec.by_key(dst)
    for p in spec.processors:
        missing = live_rels[p.key] - used[p.key]
        if missing:
            problems.append(f"{p.name}: relationships neither connected nor auto-terminated {sorted(missing)}")
    return problems


def check_spec_static(spec: FlowSpec) -> list[str]:
    """Checks that need no NiFi: forbidden components, topics, names."""
    problems = []
    for c in spec.components():
        if c.short in FORBIDDEN_TYPES or c.type.split(".")[-1] in FORBIDDEN_TYPES:
            problems.append(f"{c.name}: component type {c.short} is not allowed in this flow")
    consumes = [c.props["Topics"] for c in spec.processors if c.short == "ConsumeKafka"]
    publishes = [c.props["Topic Name"] for c in spec.processors if c.short == "PublishKafka"]
    if consumes != [topic("in")] or publishes != [topic("out")] or topic("in") == topic("out"):
        problems.append(f"topics: consumes {consumes}, publishes {publishes}")
    if not any(c.name.startswith(HOLD_PREFIX) for c in spec.processors):
        problems.append(f"no {HOLD_PREFIX}* processor for failures")
    return problems


def validate(n: Nifi, spec: FlowSpec) -> list[str]:
    problems = check_spec_static(spec)
    about = n.call("GET", "/flow/about")["about"]
    say(f"NiFi {about.get('version')} at {NIFI['datahub']}")
    g = n.find_group(NIFI["process_group"])
    say(f"  process group {NIFI['process_group']}: {'exists (deploy refuses)' if g else 'not deployed'}")
    ssl = n.root_service(NIFI["ssl_context_service"])["component"]
    ssl_apis = tuple(a["type"].split(".")[-1] for a in ssl.get("controllerServiceApis") or [])
    say(f"  root SSL context service {ssl['name']!r}: {ssl.get('state')} (referenced, not modified)")
    if "SSLContextService" not in ssl_apis:
        problems.append(f"{ssl['name']!r} is not an SSLContextService")
    if ssl.get("state") != "ENABLED":
        problems.append(f"{ssl['name']!r} is {ssl.get('state')}: the Kafka connection service cannot enable")
    n.registry_client_id()
    say(f"  registry client {NIFI['registry_client']!r}: found")
    for var in ENV.values():
        if not os.environ.get(var):
            problems.append(f"environment variable {var} is not set")
    service_apis = {"ssl": ssl_apis, **{s.key: s.apis for s in spec.services}}
    live_rels = {}
    for comp in spec.components():
        defn = n.definition(comp.kind, comp.type, comp.artifact)
        found = check_component(comp, defn, service_apis)
        problems += found
        if comp.kind == "processor":
            live_rels[comp.key] = ({r["name"] for r in defn.get("supportedRelationships") or []}
                                   | comp.rels | set(comp.extra_rels))
        say(f"  {'ok ' if not found else 'BAD'} {comp.kind} {comp.name} ({comp.short}): {len(comp.props)} properties")
    problems += check_wiring(spec, live_rels)
    for p in problems:
        say(f"  problem: {p}")
    say("validate passed: every property, value and relationship resolves" if not problems
        else f"validate failed: {len(problems)} problem(s)")
    return problems


# ------------------------------------------------------------------ deploy (processors STOPPED)
def _resolve(value, ids: dict, dry: bool):
    if isinstance(value, Ref):
        return ids[value.key]
    if isinstance(value, Env):
        if dry:
            return f"<{value.var}>"
        if not os.environ.get(value.var):
            raise SystemExit(f"environment variable {value.var} is not set")
        return os.environ[value.var]
    return value


def build(n: Nifi, spec: FlowSpec) -> str:
    if n.find_group(NIFI["process_group"]):
        raise SystemExit(f"process group {NIFI['process_group']!r} already exists: use status / version / export "
                         "(replacing it is a delete and needs approval)")
    x, y = NIFI["position"]
    pg = n.call("POST", f"/process-groups/{n.root_id()}/process-groups", json={
        "revision": {"version": 0},
        "component": {"name": NIFI["process_group"], "position": {"x": x, "y": y},
                      "comments": f"Kafka {topic('in')} -> thermal / critical DTCs -> {topic('out')}. "
                                  "Built by nifi/deploy_flow.py (EV-Factory-to-Field-Lakehouse)."}})["id"]
    say(f"process group {NIFI['process_group']} {pg}")
    ids = {"ssl": n.root_service(NIFI["ssl_context_service"])["id"]}
    for s in spec.services:
        b = n.bundle(s.kind, s.type, s.artifact)
        ent = n.call("POST", f"/process-groups/{pg}/controller-services", json={
            "revision": {"version": 0}, "component": {"type": s.type, "bundle": b, "name": s.name}})
        n.call("PUT", f"/controller-services/{ent['id']}", json={
            "revision": ent["revision"], "component": {
                "id": ent["id"], "properties": {k: _resolve(v, ids, n.dry) for k, v in s.props.items()}}})
        ids[s.key] = ent["id"]
        say(f"  service {s.name} ({s.short}): {sorted(s.props)}")
    enable_services(n, pg)
    for p in spec.processors:
        b = n.bundle(p.kind, p.type, p.artifact)
        ent = n.call("POST", f"/process-groups/{pg}/processors", json={
            "revision": {"version": 0}, "component": {
                "type": p.type, "bundle": b, "name": p.name, "position": {"x": p.pos[0], "y": p.pos[1]}}})
        n.call("PUT", f"/processors/{ent['id']}", json={
            "revision": ent["revision"], "component": {"id": ent["id"], "config": {
                "properties": {k: _resolve(v, ids, n.dry) for k, v in p.props.items()},
                "autoTerminatedRelationships": list(p.auto_terminate)}}})
        ids[p.key] = ent["id"]
        say(f"  processor {p.name} ({p.short}), stopped")
    for src, dst, rels in spec.connections:
        n.call("POST", f"/process-groups/{pg}/connections", json={
            "revision": {"version": 0},
            "component": {"source": {"id": ids[src], "groupId": pg, "type": "PROCESSOR"},
                          "destination": {"id": ids[dst], "groupId": pg, "type": "PROCESSOR"},
                          "selectedRelationships": rels,
                          "backPressureObjectThreshold": 10000, "backPressureDataSizeThreshold": "1 GB"}})
    if not n.dry:
        for p in n.processors(pg):
            c = p["component"]
            errors = c.get("validationErrors") or []
            say(f"  {c['state']:8} {c.get('validationStatus', '')}  {c['name']}"
                + (f"  {Nifi._redact('; '.join(errors))}" if errors else ""))
    if n.dry:
        say(f"dry run: nothing created; deploy would build {NIFI['process_group']} with every processor STOPPED")
    else:
        say(f"deployed {NIFI['process_group']}: services enabled, every processor STOPPED; "
            "next: version, export, then start")
    return pg


def enable_services(n: Nifi, pg: str) -> None:
    n.call("PUT", f"/flow/process-groups/{pg}/controller-services", json={"id": pg, "state": "ENABLED"})
    if n.dry:
        return
    own = []
    for _ in range(30):
        cs = n.call("GET", f"/flow/process-groups/{pg}/controller-services")["controllerServices"]
        own = [c for c in cs if c["component"].get("parentGroupId") == pg]
        if own and all(c["component"]["state"] == "ENABLED" for c in own):
            say(f"  services enabled: {len(own)}")
            return
        time.sleep(2)
    bad = {c["component"]["name"]: Nifi._redact("; ".join(c["component"].get("validationErrors") or []))
           for c in own if c["component"]["state"] != "ENABLED"}
    raise RuntimeError(f"services not enabled: {bad}")


# ------------------------------------------------------------------ start / stop / status
def _group(n: Nifi) -> dict:
    g = n.find_group(NIFI["process_group"])
    if not g:
        raise SystemExit(f"process group {NIFI['process_group']!r} not deployed")
    return g


def start(n: Nifi) -> None:
    """Start every processor except Hold*, one by one (a group-level start would run Hold* briefly
    and drop the queued failures)."""
    g = _group(n)
    n.call("PUT", f"/flow/process-groups/{g['id']}/controller-services", json={"id": g["id"], "state": "ENABLED"})
    for p in n.processors(g["id"]):
        name, state = p["component"]["name"], p["component"]["state"]
        want = "STOPPED" if name.startswith(HOLD_PREFIX) else "RUNNING"
        if want == "RUNNING" and p["component"].get("validationStatus") == "INVALID":
            say(f"  not started (invalid): {name}")
            continue
        if state != want and state != "DISABLED":
            n.call("PUT", f"/processors/{p['id']}/run-status", json={"revision": p["revision"], "state": want})
            say(f"  {want.lower()} {name}")
    say(f"started {NIFI['process_group']} ({HOLD_PREFIX}* stays stopped)")


def stop(n: Nifi) -> None:
    g = _group(n)
    n.call("PUT", f"/flow/process-groups/{g['id']}", json={"id": g["id"], "state": "STOPPED"})
    say(f"stopped {NIFI['process_group']} (services stay enabled; queues and Kafka offsets are kept)")


def status(n: Nifi) -> list[dict]:
    g = n.find_group(NIFI["process_group"])
    if not g:
        say(f"{NIFI['process_group']}: not deployed")
        return []
    snap = n.call("GET", f"/flow/process-groups/{g['id']}/status?recursive=false")
    rows = []
    for p in snap["processGroupStatus"]["aggregateSnapshot"]["processorStatusSnapshots"]:
        s = p["processorStatusSnapshot"]
        rows.append({"processor": s["name"], "state": s["runStatus"], "in_5m": s["flowFilesIn"],
                     "out_5m": s["flowFilesOut"], "queued": s.get("queued", "")})
    vci = n.call("GET", f"/process-groups/{g['id']}")["component"].get("versionControlInformation") or {}
    say(f"{NIFI['process_group']}: {g.get('runningCount', 0)} running, {g.get('stoppedCount', 0)} stopped, "
        f"{g.get('invalidCount', 0)} invalid; registry {vci.get('bucketName', '-')}/{vci.get('flowName', '-')} "
        f"version {vci.get('version', '-')} ({vci.get('state', 'not versioned')})")
    for r in sorted(rows, key=lambda r: r["processor"]):
        say(f"  {r['state']:8} in {r['in_5m']:>6} out {r['out_5m']:>6} queued {r['queued']:>8}  {r['processor']}")
    return rows


# ------------------------------------------------------------------ NiFi Registry
def bucket_id(n: Nifi) -> str:
    buckets = n.call("GET", "/buckets", base=n.reg)
    hit = next((b for b in buckets if b["name"] == NIFI["registry_bucket"]), None)
    if hit is None:
        hit = n.call("POST", "/buckets", base=n.reg, json={
            "name": NIFI["registry_bucket"], "description": "EVX NiFi flows (flow as code, nifi/deploy_flow.py)"})
        say(f"  registry bucket {NIFI['registry_bucket']} created")
    return hit["identifier"]


def version(n: Nifi, comment: str) -> int | None:
    g = _group(n)
    ent = n.call("GET", f"/process-groups/{g['id']}")
    vci = ent["component"].get("versionControlInformation")
    body = {"processGroupRevision": ent["revision"], "versionedFlow": {
        "registryId": n.registry_client_id(), "bucketId": bucket_id(n), "flowName": NIFI["flow_name"],
        "description": "EVX alert router (nifi/deploy_flow.py)", "comments": comment, "action": "COMMIT"}}
    if vci:
        body["versionedFlow"]["flowId"] = vci["flowId"]
    out = n.call("POST", f"/versions/process-groups/{g['id']}", json=body)
    v = (out.get("versionControlInformation") or {}).get("version")
    say(f"  NiFi Registry: {NIFI['registry_bucket']}/{NIFI['flow_name']} version {v}")
    return v


# ------------------------------------------------------------------ export (sensitive values removed)
SNAPSHOT_KEYS = ("externalControllerServices", "flowContents", "flowEncodingVersion", "parameterContexts",
                 "parameterProviders", "latest")
PLACEHOLDER_PROPS = {"bootstrap.servers": ENV["kafka_bootstrap"], "sasl.username": ENV["user"]}


def _walk_components(group: dict):
    for kind in ("controllerServices", "processors"):
        yield from group.get(kind) or []
    for child in group.get("processGroups") or []:
        yield from _walk_components(child)


def sanitize(snapshot: dict, env: dict | None = None) -> dict:
    """Keep the flow-definition keys; null every sensitive property and parameter; replace
    environment-sourced values by ${VAR}. Refuses when a password value is still present."""
    env = dict(os.environ if env is None else env)
    flow = json.loads(json.dumps({k: snapshot[k] for k in SNAPSHOT_KEYS if k in snapshot}))
    for comp in _walk_components(flow.get("flowContents") or {}):
        props, descs = comp.setdefault("properties", {}), comp.get("propertyDescriptors") or {}
        for name, d in descs.items():
            if d.get("sensitive"):
                props[name] = None
        for name, var in PLACEHOLDER_PROPS.items():
            if props.get(name) is not None:
                props[name] = "${" + var + "}"
    for ctx in (flow.get("parameterContexts") or {}).values():
        for p in ctx.get("parameters") or []:
            if p.get("sensitive"):
                p["value"] = None
    whole = {env[v]: "${" + v + "}" for v in (ENV["kafka_bootstrap"], ENV["user"]) if env.get(v)}

    def swap(node):
        if isinstance(node, dict):
            return {k: swap(v) for k, v in node.items()}
        if isinstance(node, list):
            return [swap(v) for v in node]
        return whole.get(node, node) if isinstance(node, str) else node

    flow = swap(flow)
    secret = env.get(ENV["password"])
    if secret and json.dumps(secret)[1:-1] in json.dumps(flow):
        raise SystemExit(f"export refused: the value of {ENV['password']} is still in the flow definition")
    return flow


def check_export(flow: dict) -> list[str]:
    """Security checks on an exported definition (also run by tests/test_nifi_export.py)."""
    problems = []
    fc = flow.get("flowContents") or {}
    for comp in _walk_components(fc):
        short = comp.get("type", "").split(".")[-1]
        if short in FORBIDDEN_TYPES:
            problems.append(f"forbidden component {short}")
        if comp.get("componentType") == "CONTROLLER_SERVICE" and "SSLContext" in short:
            problems.append(f"embedded SSL context service {comp.get('name')!r}: reference the root one instead")
        for name, d in (comp.get("propertyDescriptors") or {}).items():
            if d.get("sensitive") and (comp.get("properties") or {}).get(name) is not None:
                problems.append(f"{comp.get('name')}: sensitive property {name!r} has a value")
    external = {k: v.get("name") for k, v in (flow.get("externalControllerServices") or {}).items()}
    if NIFI["ssl_context_service"] not in external.values():
        problems.append(f"{NIFI['ssl_context_service']!r} is not referenced as an external controller service")
    return problems


def render_offline(spec: FlowSpec) -> dict:
    """The flow definition in the shape of a NiFi export, from flow_spec(); ids are deterministic."""
    ns = uuid.uuid5(uuid.NAMESPACE_URL, f"evx/{NIFI['flow_name']}")
    ident = lambda key: str(uuid.uuid5(ns, key))  # noqa: E731
    pg = ident("process-group")
    ids = {"ssl": ident("external/ssl-context-service"), **{c.key: ident(c.key) for c in spec.components()}}
    bundle = lambda art: {"group": "org.apache.nifi", "artifact": art, "version": NIFI["nifi_version"]}  # noqa: E731

    def props(c: Component) -> tuple[dict, dict]:
        values, descs = {}, {}
        for k, v in c.props.items():
            if isinstance(v, Ref):
                values[k] = ids[v.key]
            elif isinstance(v, Env):
                values[k] = None if k in c.sensitive else "${" + v.var + "}"
            else:
                values[k] = v
            descs[k] = {"name": k, "displayName": k, "dynamic": k in c.dynamic,
                        "identifiesControllerService": isinstance(v, Ref), "sensitive": k in c.sensitive}
        return values, descs

    services = []
    for s in spec.services:
        values, descs = props(s)
        services.append({
            "bundle": bundle(s.artifact), "comments": "", "componentType": "CONTROLLER_SERVICE",
            "controllerServiceApis": [{"type": a} for a in s.apis], "groupIdentifier": pg,
            "identifier": ids[s.key], "name": s.name, "properties": values, "propertyDescriptors": descs,
            "scheduledState": "DISABLED", "type": s.type})
    processors = []
    for p in spec.processors:
        values, descs = props(p)
        processors.append({
            "autoTerminatedRelationships": sorted(p.auto_terminate), "bundle": bundle(p.artifact), "comments": "",
            "componentType": "PROCESSOR", "concurrentlySchedulableTaskCount": 1, "executionNode": "ALL",
            "groupIdentifier": pg, "identifier": ids[p.key], "name": p.name,
            "position": {"x": float(p.pos[0]), "y": float(p.pos[1])}, "properties": values,
            "propertyDescriptors": descs, "scheduledState": "ENABLED", "schedulingPeriod": "0 sec",
            "schedulingStrategy": "TIMER_DRIVEN", "type": p.type})
    connections = []
    for src, dst, rels in spec.connections:
        end = lambda key: {"groupId": pg, "id": ids[key], "name": spec.by_key(key).name, "type": "PROCESSOR"}  # noqa: E731
        connections.append({
            "backPressureDataSizeThreshold": "1 GB", "backPressureObjectThreshold": 10000,
            "componentType": "CONNECTION", "destination": end(dst), "groupIdentifier": pg,
            "identifier": ident(f"connection/{src}/{dst}/{'+'.join(rels)}"), "selectedRelationships": sorted(rels),
            "source": end(src)})
    x, y = NIFI["position"]
    return {
        "externalControllerServices": {ids["ssl"]: {"identifier": ids["ssl"], "name": NIFI["ssl_context_service"]}},
        "flowContents": {
            "comments": "Rendered offline by nifi/deploy_flow.py export --offline; replace with the registry "
                        "export after the live deploy.",
            "componentType": "PROCESS_GROUP", "connections": connections, "controllerServices": services,
            "funnels": [], "identifier": pg, "inputPorts": [], "labels": [], "name": NIFI["process_group"],
            "outputPorts": [], "position": {"x": float(x), "y": float(y)}, "processGroups": [],
            "processors": processors, "remoteProcessGroups": []},
        "flowEncodingVersion": "1.0", "parameterContexts": {}, "parameterProviders": {}, "latest": False}


def export(n: Nifi | None, offline: bool = False, dry: bool = False) -> Path:
    if offline:
        flow, source = render_offline(flow_spec()), "flow_spec() (offline)"
    else:
        g = _group(n)
        vci = n.call("GET", f"/process-groups/{g['id']}")["component"].get("versionControlInformation")
        if vci:
            raw = n.call("GET", f"/buckets/{vci['bucketId']}/flows/{vci['flowId']}/versions/{vci['version']}",
                         base=n.reg)
            source = f"registry {NIFI['registry_bucket']}/{NIFI['flow_name']} version {vci['version']}"
        else:
            raw = n.call("GET", f"/process-groups/{g['id']}/download?includeReferencedServices=false")
            source = "live group (not versioned yet)"
        flow = sanitize(raw)
    problems = check_export(flow)
    if problems:
        raise SystemExit("export refused: " + "; ".join(problems))
    if dry:
        say(f"would write {EXPORT.relative_to(ROOT)} from {source}")
        return EXPORT
    EXPORT.parent.mkdir(parents=True, exist_ok=True)
    EXPORT.write_text(json.dumps(flow, indent=2, sort_keys=True) + "\n")
    say(f"exported {source} -> {EXPORT.relative_to(ROOT)} (sensitive values removed)")
    return EXPORT


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("action", choices=["status", "validate", "deploy", "version", "export", "start", "stop"])
    ap.add_argument("--dry-run", action="store_true", help="run the GETs, print every write and skip it")
    ap.add_argument("--offline", action="store_true", help="export: render from flow_spec(), no NiFi call")
    ap.add_argument("-m", "--message", default="")
    a, _ = ap.parse_known_args(argv)  # a CAI job kernel adds -f <file>
    spec = flow_spec()
    if a.action == "export" and a.offline:
        export(None, offline=True, dry=a.dry_run)
        return 0
    n = Nifi(dry=a.dry_run)
    if a.action == "status":
        status(n)
    elif a.action == "validate":
        return 1 if validate(n, spec) else 0
    elif a.action == "deploy":
        if validate(n, spec):
            raise SystemExit("deploy refused: fix the validate problems first")
        build(n, spec)
    elif a.action == "version":
        version(n, a.message or "evx alert router")
    elif a.action == "export":
        export(n, dry=a.dry_run)
    elif a.action == "start":
        start(n)
    else:
        stop(n)
    return 0


if __name__ == "__main__":
    rc = main()
    if rc:
        sys.exit(rc)
