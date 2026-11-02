#!/usr/bin/env python3
"""
The three M1 EVX dashboards in Cloudera Data Visualization, as code (machinery from OGX):

  EVX Factory Quality & Yield       cell and pack FPY from the semantic KPI views
  EVX Traceability & Containment    powder lots, genealogy and the champion worklist
  EVX Fleet Reliability             FIR90 by build week and supplier batch

  set -a; source .env; set +a
  python dataviz/build_dashboard.py                          # write dataviz/evx_dashboards.json
  python dataviz/build_dashboard.py --check                  # every visual on Impala
  python dataviz/build_dashboard.py --import --connection federal-impala-1
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

OUT = ROOT / "dataviz" / "evx_dashboards.json"
DB = "rsingh_evx_semantic"
NS = uuid.UUID("a1c3e5f7-9b2d-4e06-8c1a-3f5d7e9b2c40")
DASHBOARD_PK0 = 15000
DATASET_PK0 = 15100
VISUAL_PK0 = 15200
DEFAULT_VERSION = {"Arcviz Version": "8.1.4.1000", "Description": "8.1.4.1000-4"}

DATASETS = {
    "fpy_cell": ("EVX - Cell FPY", "kpi_fpy_cell", set()),
    "fpy_pack": ("EVX - Pack FPY", "kpi_fpy_pack", set()),
    "fir90_week": ("EVX - FIR90 by build week", "kpi_fir90_build_week", set()),
    "fir90_batch": ("EVX - FIR90 by supplier batch", "kpi_fir90_supplier_batch", set()),
    "pack_risk": ("EVX - Pack risk decisions", "dash_pack_risk", {"genealogy_open", "safety_floor", "ood_flag"}),
    "supplier_batch": ("EVX - Supplier batch containment", "dash_supplier_batch", set()),
}

pct = lambda e: f"round(100 * {e}, 1)"  # noqa: E731

FACTORY_SHEETS = [
    ("Factory yield", [
        dict(type="kpi", ds="fpy_cell", title="Cells graded", measures=[("sum([cells_graded])", "Cells")],
             pos=(1, 1, 16, 10)),
        dict(type="kpi", ds="fpy_cell", title="Cell first-pass yield",
             measures=[(pct("sum([cells_first_pass]) / sum([cells_graded])"), "FPY %")], pos=(17, 1, 16, 10)),
        dict(type="kpi", ds="fpy_pack", title="Packs tested", measures=[("sum([packs_tested])", "Packs")],
             pos=(33, 1, 16, 10)),
        dict(type="kpi", ds="fpy_pack", title="Pack first-pass yield",
             measures=[(pct("sum([packs_first_pass]) / sum([packs_tested])"), "FPY %")], pos=(49, 1, 16, 10)),
        dict(type="trellis-lines", ds="fpy_cell", title="Cell FPY by production date",
             x=[("production_date", "Date")], measures=[("avg([fpy])", "FPY")], pos=(1, 11, 32, 22)),
        dict(type="trellis-lines", ds="fpy_pack", title="Pack FPY by production date",
             x=[("production_date", "Date")], measures=[("avg([fpy])", "FPY")], pos=(33, 11, 32, 22)),
        dict(type="table", ds="fpy_cell", title="Cell FPY by line and shift (latest week)",
             dims=[("production_date", "Date"), ("line", "Line"), ("shift", "Shift"),
                   ("formation_station", "Station")],
             measures=[("sum([cells_graded])", "Graded"), ("sum([cells_first_pass])", "First pass"),
                       ("avg([fpy])", "FPY")],
             sort_dim="production_date", sort_desc=True, limit=500, pos=(1, 33, 64, 28)),
    ]),
]

TRACE_SHEETS = [
    ("Containment", [
        dict(type="kpi", ds="fpy_pack", title="Packs with open genealogy",
             measures=[("sum([packs_genealogy_open])", "Open")], pos=(1, 1, 16, 10)),
        dict(type="kpi", ds="pack_risk", title="Worklist vehicles",
             measures=[("sum(CASE WHEN [decision] = 'WORKLIST' THEN 1 ELSE 0 END)", "Worklist")], pos=(17, 1, 16, 10)),
        dict(type="kpi", ds="pack_risk", title="Safety-floor vehicles",
             measures=[("sum([safety_floor])", "Safety floor")], pos=(33, 1, 16, 10)),
        dict(type="kpi", ds="pack_risk", title="Abstained (band or OOD)",
             measures=[("sum(CASE WHEN [decision] = 'ABSTAIN' THEN 1 ELSE 0 END)", "Abstain")], pos=(49, 1, 16, 10)),
        dict(type="trellis-bars", ds="supplier_batch", title="Vehicles by powder lot (top 25 by worklist)",
             x=[("supplier_batch", "Powder lot")],
             measures=[("sum([vehicles])", "Vehicles"), ("sum([worklist_vehicles])", "Worklist"),
                       ("sum([safety_floor_vehicles])", "Safety floor")],
             sort_dim="supplier_batch", limit=25, pos=(1, 11, 64, 24)),
        dict(type="table", ds="pack_risk", title="Highest-risk vehicles (worklist and safety floor)",
             dims=[("vin", "VIN"), ("pack_serial", "Pack"), ("decision", "Decision"), ("reason", "Reason"),
                   ("priority", "Priority")],
             measures=[("max([score])", "Score")],
             filters=["[decision] IN ('WORKLIST', 'SAFETY_FLOOR')"],
             sort_desc=True, limit=200, pos=(1, 35, 64, 28)),
    ]),
]

FLEET_SHEETS = [
    ("Field reliability", [
        dict(type="kpi", ds="fir90_week", title="Matured vehicles",
             measures=[("sum([vehicles_matured])", "Matured")], pos=(1, 1, 16, 10)),
        dict(type="kpi", ds="fir90_week", title="Battery claims in 90 days",
             measures=[("sum([vehicles_failed])", "Failed")], pos=(17, 1, 16, 10)),
        dict(type="kpi", ds="fir90_week", title="FIR90 per 1,000",
             measures=[("round(1000 * sum([vehicles_failed]) / sum([vehicles_matured]), 2)", "FIR90")],
             pos=(33, 1, 16, 10)),
        dict(type="trellis-bars", ds="fir90_week", title="FIR90 by build week",
             x=[("build_week", "Build week")], measures=[("avg([fir90_per_1000])", "FIR90 per 1k")],
             pos=(1, 11, 32, 22)),
        dict(type="trellis-bars", ds="fir90_batch", title="FIR90 by supplier batch (top 20 lots)",
             x=[("supplier_batch", "Powder lot")], measures=[("avg([fir90_per_1000])", "FIR90 per 1k")],
             sort_dim="supplier_batch", limit=20, pos=(33, 11, 32, 22)),
        dict(type="table", ds="fir90_batch", title="Supplier batches ranked by FIR90",
             dims=[("supplier_batch", "Powder lot"), ("batch_status", "Status"), ("supplier_id", "Supplier")],
             measures=[("sum([vehicles_matured])", "Matured"), ("sum([vehicles_failed])", "Failed"),
                       ("avg([fir90_per_1000])", "FIR90 per 1k")],
             sort_desc=True, limit=40, pos=(1, 33, 64, 28)),
    ]),
]

DASHBOARDS = [
    dict(title="EVX Factory Quality & Yield", pk=DASHBOARD_PK0, key="factory-quality", sheets=FACTORY_SHEETS,
         main_ds="fpy_cell", subtitle="Cell and pack first-pass yield from factory grading and EOL"),
    dict(title="EVX Traceability & Containment", pk=DASHBOARD_PK0 + 100, key="traceability", sheets=TRACE_SHEETS,
         main_ds="pack_risk", subtitle="Powder lots, genealogy gaps and the champion worklist"),
    dict(title="EVX Fleet Reliability", pk=DASHBOARD_PK0 + 200, key="fleet-reliability", sheets=FLEET_SHEETS,
         main_ds="fir90_week", subtitle="Field incident rate within 90 days in service"),
]

SHELVES = {
    "kpi": [("dimensions_shelf", 1, 1), ("aggregates_shelf", 1, 2), ("compare_shelf", 1, 2), ("label_shelf", 1, 2),
            ("tooltip_shelf", 1, 2), ("x_shelf", 1, 1), ("y_shelf", 1, 1), ("filters_shelf", 2, 3)],
    "table": [("dimensions_shelf", 1, 1), ("aggregates_shelf", 1, 2), ("filters_shelf", 2, 3)],
    "trellis-bars": [("x_shelf", 1, 3), ("y_shelf", 1, 3), ("color_shelf", 1, 3), ("tooltip_shelf", 1, 2),
                     ("drill_shelf", 1, 1), ("label_shelf", 1, 2), ("filters_shelf", 2, 3)],
    "trellis-lines": [("x_shelf", 1, 3), ("y_shelf", 1, 3), ("color_shelf", 1, 3), ("tooltip_shelf", 1, 2),
                      ("filters_shelf", 2, 3)],
}


def visuals_of(d: dict):
    for sheet, items in d["sheets"]:
        for v in items:
            yield sheet, v


def uid(*parts: str) -> str:
    return str(uuid.uuid5(NS, "/".join(parts)))


def impala():
    import run_semantic as S

    cfg = json.loads((ROOT / "config" / "lakehouse.json").read_text())
    eng = S.Impala(cfg["db_prefix"])

    class Engine:
        def query(self, sql: str):
            return eng._query(eng.conn, sql)

    return Engine()


def column_types(engine, ds_key: str) -> dict[str, str]:
    cols, rows = engine.query(f"DESCRIBE {DB}.{DATASETS[ds_key][1]}")
    return {r[cols.index("name")]: str(r[cols.index("type")]).upper() for r in rows}


def is_dim(ds_key: str, col: str, typ: str) -> bool:
    return col in DATASETS[ds_key][2] or not any(t in typ for t in ("INT", "DOUBLE", "FLOAT", "DECIMAL", "BIGINT"))


def dataset_record(key: str, pk: int, types: dict[str, str], conn_id: int, dashboards: list[int]) -> dict:
    name, view, _ = DATASETS[key]
    table = f"{DB}.{view}"
    cols = [{"alias": c, "type": t, "name": c, "isdim": is_dim(key, c, t)} for c, t in types.items()]
    return {"model": "datasets.dataset", "pk": pk, "fields": {
        "dataconnection": conn_id, "dataset_name": name, "dataset_type": "singletable", "dataset_detail": table,
        "dataset_description": f"{table} (sql/semantic)",
        "dataset_info": json.dumps([{"tablename": table, "columns": cols}]),
        "dataset_tablenames": json.dumps([table]), "uuid": uid("dataset", key), "imported_uuid": None,
        "cache_sequence": 0, "dataset_settings": "{}", "search_enabled": False, "dashboards": dashboards,
        "version_id": pk, "version_group_id": pk, "is_active_version": True,
        "version_name": "evx-factory-to-field", "is_named_version": False}}


def dim_item(col: str, alias: str, typ: str) -> dict:
    return {"dataset_colname": col, "dataset_coltype": typ, "expression_for_trigger": f"[{col}]", "col_alias": alias}


def measure_item(expr: str, alias: str) -> dict:
    return {"custom_expr": expr, "expression_for_trigger": expr, "expr_hasagg": True, "col_alias": alias,
            "dataset_colname": alias, "dataset_coltype": "DOUBLE"}


def filter_item(expr: str) -> dict:
    return {"custom_expr": expr, "expression_for_trigger": expr, "filter_input": {}, "filter_data": [],
            "dataset_colname": "", "dataset_coltype": "STRING", "filter_column": ""}


def visual_record(v: dict, pk: int, sheet: str, types: dict[str, str], dataset_pk: int, dash: dict) -> dict:
    kind = v["type"]
    shelves = {name: [] for name, _, _ in SHELVES[kind]}
    sources = {}

    def add_dims(shelf, pairs):
        for col, alias in pairs:
            shelves[shelf].append(dim_item(col, alias, types[col]))
            sources[f"[{col}] as 'sub:{alias}'"] = shelf

    def add_measures(shelf, pairs):
        for expr, alias in pairs:
            shelves[shelf].append(measure_item(expr, alias))
            sources[f"{expr} as 'sub:{alias}'"] = shelf

    if kind in ("kpi", "table"):
        add_dims("dimensions_shelf", v.get("dims", []))
        add_measures("aggregates_shelf", v["measures"])
    else:
        add_dims("x_shelf", v["x"])
        add_measures("y_shelf", v["measures"])
        add_dims("color_shelf", v.get("color", []))
    for expr in v.get("filters", []):
        shelves["filters_shelf"].append(filter_item(expr))
        sources[expr] = "filters_shelf"
    if v.get("sort_desc"):
        shelf = "aggregates_shelf" if kind == "table" else "y_shelf"
        if shelves[shelf]:
            shelves[shelf][0]["order"] = {"priority": 1, "ascending": False}
    if v.get("sort_dim"):
        shelf = "dimensions_shelf" if kind == "table" else "x_shelf"
        item = next(i for i in shelves[shelf] if i["dataset_colname"] == v["sort_dim"])
        item["order"] = {"priority": 1, "ascending": v.get("sort_asc", True)}
    report = {
        "report_title": v["title"], "report_subtitle": "", "dashboard_id": dash["pk"],
        "limit": v.get("limit", 1000), "sample_pct": "Off", "selected_segments": [], "report_derived_data": [],
        "click_behaviors": {}, "sort_orders_asc": {}, "user_settings": {}, **shelves,
        "core": {"viz_type": kind, "saved_shelf_sources": sources,
                 "shelves": [{"name": n, "shelf_type": s, "column_type": c} for n, s, c in SHELVES[kind]]},
    }
    return {"model": "reports.report", "pk": pk, "fields": {
        "report_name": "", "report_description": f"{dash['title']} / {sheet}", "dataset": dataset_pk,
        "workspace": 1, "report_type": kind, "report_mode": "", "dashboard_url_name": "",
        "report_data": json.dumps({"report_data": report, "report_type": kind}), "shared_visual_dashboards": None,
        "parent_report": None, "uuid": uid("visual", dash["key"], sheet, v["title"]), "imported_uuid": None,
        "has_css_styles": False, "report_search_text": ""}}


def dashboard_record(d: dict, pk0: int, visuals: list[dict], ds_pk: dict[str, int], types: dict) -> dict:
    sheets, pk = [], pk0
    for order, (sheet, items) in enumerate(d["sheets"], 1):
        placed = []
        for v in items:
            pk += 1
            visuals.append(visual_record(v, pk, sheet, types[v["ds"]], ds_pk[v["ds"]], d))
            placed.append((pk, v["pos"]))
        sheets.append({"sheet_id": order, "order": order, "sheet_handle_title": sheet, "behaviors": {},
                       "visual_widgets": [{"col": c, "row": r, "size_x": w, "size_y": h, "id": f"uri-{i}-widget-{p}"}
                                          for i, (p, (c, r, w, h)) in enumerate(placed, 1)],
                       "control_widgets": []})
    dash = {"report_title": d["title"], "numColumns": 64, "report_subtitle": d["subtitle"],
            "dashboard_widgets": sheets[0]["visual_widgets"], "dashboard_sheets": sheets,
            "user_settings": {"dashboard_width": "1280", "display_filters": "true",
                              "permit_csv_download_dashboard": "true"},
            "global_control_widgets": [], "control_widgets": [], "click_behavior": {}}
    return {"model": "reports.report", "pk": d["pk"], "fields": {
        "report_name": d["title"], "report_description": "README_CAI.md",
        "dataset": ds_pk[d["main_ds"]], "workspace": 1, "report_type": "dashboard", "report_mode": None,
        "dashboard_url_name": "", "report_data": json.dumps(dash), "shared_visual_dashboards": "[]",
        "parent_report": None, "uuid": uid("dashboard", d["key"]), "imported_uuid": None, "has_css_styles": False,
        "report_search_text": None}}


def missing_columns(types: dict[str, dict[str, str]]) -> list[str]:
    out = []
    for d in DASHBOARDS:
        for sheet, v in visuals_of(d):
            exprs = [e for e, _ in v["measures"]] + v.get("filters", [])
            cols = {c for c, _ in v.get("dims", []) + v.get("x", []) + v.get("color", [])}
            cols |= {c for e in exprs for c in re.findall(r"\[(\w+)\]", e)}
            out += [f"{d['title']} / {sheet} / {v['title']}: {c}" for c in sorted(cols - set(types[v["ds"]]))]
    return out


def build(types: dict[str, dict[str, str]], conn_id: int, version: dict) -> dict:
    missing = missing_columns(types)
    if missing:
        raise SystemExit("columns not in the dataset's view:\n  " + "\n  ".join(missing))
    ds_pk = {k: DATASET_PK0 + i for i, k in enumerate(DATASETS)}
    visuals, dashboards, pk0 = [], [], VISUAL_PK0
    for d in DASHBOARDS:
        dashboards.append(dashboard_record(d, pk0, visuals, ds_pk, types))
        pk0 += 100
    used_by = {k: [d["pk"] for d in DASHBOARDS if any(v["ds"] == k for _, v in visuals_of(d))] for k in DATASETS}
    return {"segments": [], "staticasset": [], "dashboards": dashboards, "appgroupmembership": [],
            "reportannotation": [], "events": [], "customcss": [], "reportimage": [], "dateranges": [],
            "visuals": visuals, "colorpalette": [], "appgroups": [],
            "datasets": [dataset_record(k, ds_pk[k], types[k], conn_id, used_by[k]) for k in DATASETS],
            "version": version}


def _strip(e: str) -> str:
    return re.sub(r"\[(\w+)\]", r"\1", e)


def tile_sql(v: dict) -> str:
    where = " AND ".join(_strip(f) for f in v.get("filters", [])) or "TRUE"
    return f"SELECT {_strip(v['measures'][0][0])} FROM {DB}.{DATASETS[v['ds']][1]} WHERE {where}"


def visual_sql(v: dict) -> str:
    dims = [c for c, _ in v.get("dims", []) + v.get("x", []) + v.get("color", [])]
    where = " AND ".join(_strip(f) for f in v.get("filters", [])) or "TRUE"
    group = f" GROUP BY {', '.join(dims)}" if dims else ""
    return (f"SELECT {', '.join(dims + [_strip(e) for e, _ in v['measures']])} FROM {DB}.{DATASETS[v['ds']][1]} "
            f"WHERE {where}{group} LIMIT {v.get('limit', 1000)}")


def check(engine) -> int:
    failed = 0
    for d in DASHBOARDS:
        for sheet, v in visuals_of(d):
            try:
                rows = engine.query(visual_sql(v))[1]
                ok, note = bool(rows) or v.get("may_be_empty", False), f"{len(rows)} rows"
                if rows and v["type"] == "kpi":
                    note = f"{rows[0][-1]}"
            except Exception as e:  # noqa: BLE001
                ok, note = False, str(e).strip().splitlines()[-1][:200]
            failed += not ok
            print(f"{'ok  ' if ok else 'FAIL'} {d['title']} / {sheet} / {v['title']}: {note}", flush=True)
    return failed


class DataViz:
    def __init__(self):
        import requests

        key = os.environ.get("EVX_VIZ_API_KEY")
        if not key:
            raise SystemExit("set EVX_VIZ_API_KEY (Data Visualization: Site Administration -> Manage API Keys), "
                             "or import dataviz/evx_dashboards.json in the UI")
        self.url = json.loads((ROOT / "config" / "lakehouse.json").read_text())["dataviz_url"].rstrip("/")
        self.s = requests.Session()
        self.s.headers["Authorization"] = f"apikey {key}"

    def get(self, path: str, **params):
        r = self.s.get(self.url + path, params=params, timeout=60)
        if r.status_code != 200:
            raise SystemExit(f"GET {path}: HTTP {r.status_code} {r.text[:200]}")
        return r.json()

    def connections(self) -> list[dict]:
        return self.get("/arc/adminapi/v1/connections")

    def connection_id(self, name: str) -> int:
        found = next((c for c in self.connections() if c["name"] == name), None)
        if found is None:
            raise SystemExit(f"no connection {name!r}; --list-connections shows them")
        return found["id"]

    def version(self) -> dict:
        return self.get("/arc/migration/api/export/", dashboards="[]", filename="version", dry_run="False")["version"]

    def import_file(self, path: Path, connection: str) -> None:
        with path.open("rb") as f:
            r = self.s.post(self.url + "/arc/migration/api/import/", files={"import_file": f},
                            data={"dry_run": "False", "dataconnection_name": connection}, timeout=300)
        print(f"import: HTTP {r.status_code} {r.text[:500]}")
        if r.status_code != 200:
            raise SystemExit(1)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--import", dest="do_import", action="store_true", help="import the file (needs EVX_VIZ_API_KEY)")
    p.add_argument("--connection", help="Data Visualization connection to Impala (with --import)")
    p.add_argument("--check", action="store_true", help="run every visual's query directly on Impala")
    p.add_argument("--list-connections", action="store_true")
    args = p.parse_args()
    if args.list_connections:
        for c in DataViz().connections():
            print(c["id"], c["name"], c.get("type"))
        return 0
    engine = impala()
    if args.check:
        return 1 if check(engine) else 0
    viz = DataViz() if args.do_import else None
    if viz and not args.connection:
        p.error("--import needs --connection (see --list-connections)")
    types = {k: column_types(engine, k) for k in DATASETS}
    conn_id = viz.connection_id(args.connection) if viz else 1
    doc = build(types, conn_id, viz.version() if viz else DEFAULT_VERSION)
    OUT.write_text(json.dumps(doc, indent=1) + "\n")
    print(f"wrote {OUT.relative_to(ROOT)}: {len(doc['dashboards'])} dashboards, {len(doc['datasets'])} datasets, "
          f"{len(doc['visuals'])} visuals, {sum(len(d['sheets']) for d in DASHBOARDS)} sheets")
    if viz:
        viz.import_file(OUT, args.connection)
        print(f"open {viz.url}/arc/apps/ -> Dashboards -> " + " / ".join(d["title"] for d in DASHBOARDS))
    return 0


if __name__ == "__main__":
    sys.exit(main())
