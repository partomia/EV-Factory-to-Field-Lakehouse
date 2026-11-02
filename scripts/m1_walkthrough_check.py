#!/usr/bin/env python3
"""
M1 browser-walkthrough substitute: run the same Impala SQL, dashboard checks and HTTP probes
the presenter would hit in the workbenches, vision app and CDV. No secrets printed.

  set -a; source .env; set +a
  python3 scripts/m1_walkthrough_check.py
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from common import lakehouse_config, load_env  # noqa: E402
from usecases.uc01_pack_risk.app_page import batches_sql, vehicle_sql, worklist_sql  # noqa: E402
from usecases.uc01_pack_risk.media_page import edr_sql, film_sql, xray_index_sql  # noqa: E402

load_env()
CFG = lakehouse_config()
PREFIX = CFG.get("db_prefix", "rsingh_evx")
GOLD, SEMANTIC, BRONZE = f"{PREFIX}_gold", f"{PREFIX}_semantic", f"{PREFIX}_bronze"
FAIL = 0


def ok(label: str, detail: str = "") -> None:
    print(f"PASS  {label}" + (f" — {detail}" if detail else ""), flush=True)


def bad(label: str, detail: str = "") -> None:
    global FAIL
    FAIL += 1
    print(f"FAIL  {label}" + (f" — {detail}" if detail else ""), flush=True)


def query(sql: str):
    from impala.dbapi import connect

    imp = CFG["impala"]
    conn = connect(host=os.environ.get("EVX_IMPALA_HOST", imp["host"]), port=int(imp["port"]),
                   use_ssl=True, use_http_transport=True, http_path=imp["http_path"],
                   auth_mechanism=imp["auth_mechanism"],
                   user=os.environ["EVX_IMPALA_USER"], password=os.environ["EVX_IMPALA_PASSWORD"],
                   timeout=180)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        return cols, rows
    finally:
        conn.close()


def http_code(url: str) -> int:
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return e.code


def check_impala() -> None:
    print("\n=== Quality workbench SQL ===", flush=True)
    cols, rows = query(worklist_sql(GOLD, SEMANTIC, "risk", "WORKLIST", 5))
    if rows:
        ok("Vehicle risk worklist", f"{len(rows)} sample rows; top VIN {rows[0][cols.index('vin')]}")
    else:
        bad("Vehicle risk worklist", "no WORKLIST rows")

    cols, rows = query(worklist_sql(GOLD, SEMANTIC, "risk", "SAFETY_FLOOR", 3))
    if rows and any(r[cols.index("safety_floor")] for r in rows):
        ok("Safety floor vehicles", f"{len(rows)} rows")
    else:
        bad("Safety floor vehicles", "expected thermal-floor rows")

    cols, rows = query(batches_sql(GOLD, SEMANTIC, 5))
    if rows:
        ok("Batch containment", f"{len(rows)} powder lots")
    else:
        bad("Batch containment", "no batch rows")

    cols, wl = query(worklist_sql(GOLD, SEMANTIC, "risk", "WORKLIST", 1))
    if wl:
        vin = wl[0][cols.index("vin")]
        _, pack = query(vehicle_sql(GOLD, vin))
        if pack and len(pack) >= 1:
            ok("Pack 360", f"VIN {vin}: vehicle row present")
        else:
            bad("Pack 360", f"no vehicle row for {vin}")
    else:
        bad("Pack 360", "no worklist VIN to drill")

    print("\n=== Vision workbench SQL ===", flush=True)
    _, xray = query(xray_index_sql(SEMANTIC, "FAIL", 5))
    if xray:
        ok("Cell X-ray viewer", f"{len(xray)} FAIL rows")
    else:
        bad("Cell X-ray viewer", "no FAIL x-rays")

    _, film = query(film_sql(GOLD, f"{PREFIX}_silver", 5))
    if film:
        ok("Film defect gallery", f"{len(film)} rows")
    else:
        bad("Film defect gallery", "no film rows")

    _, edr = query(edr_sql(GOLD, f"{PREFIX}_silver", 5))
    if edr:
        ok("EDR event replay", f"{len(edr)} events")
    else:
        bad("EDR event replay", "no EDR rows")

    print("\n=== Decision counts (notebook / demo) ===", flush=True)
    _, counts = query(f"""
SELECT `decision`, count(*) AS n
FROM {GOLD}.uc01_pack_risk_score
WHERE _business_date = (SELECT max(_business_date) FROM {GOLD}.uc01_pack_risk_score)
GROUP BY 1 ORDER BY 2 DESC
""")
    total = sum(r[1] for r in counts)
    summary = ", ".join(f"{r[0]}={r[1]}" for r in counts)
    if total >= 17000:
        ok("Score population", f"{total} vehicles ({summary})")
    else:
        bad("Score population", f"only {total} scored")


def check_dashboards() -> None:
    print("\n=== Data Visualization (--check) ===", flush=True)
    proc = subprocess.run([sys.executable, str(ROOT / "dataviz" / "build_dashboard.py"), "--check"],
                          capture_output=True, text=True, cwd=ROOT)
    if proc.returncode == 0:
        last = [ln for ln in proc.stdout.splitlines() if ln.strip()][-3:]
        ok("Dashboard visuals on Impala", "; ".join(last) if last else "all visuals OK")
    else:
        bad("Dashboard visuals", proc.stderr.strip()[:200] or proc.stdout.strip()[:200])


def check_http() -> None:
    print("\n=== HTTP (SSO redirect is healthy) ===", flush=True)
    for name, url in (
        ("Quality workbench", "https://evx-workbench.federal-cml.federal.dp5i-5vkq.cloudera.site/"),
        ("Vision workbench", "https://evx-vision.federal-cml.federal.dp5i-5vkq.cloudera.site/"),
    ):
        code = http_code(url)
        if code in (200, 302, 303):
            ok(name, f"HTTP {code}")
        else:
            bad(name, f"HTTP {code}")


def check_cai_notebook_artifacts() -> None:
    print("\n=== CAI notebook artifacts ===", flush=True)
    metrics = ROOT / "outputs" / "uc01" / "pack_risk_eol" / "metrics.json"
    baseline = ROOT / "usecases" / "uc01_pack_risk" / "baseline.json"
    nb = ROOT / "notebooks" / "cai_demo.ipynb"
    if metrics.is_file() and baseline.is_file():
        m = json.loads(metrics.read_text())
        b = json.loads(baseline.read_text())
        if m.get("winner") == b.get("winner") == "logistic":
            ok("Champion metrics + baseline", f"winner logistic, schema {b.get('schema_hash', '')[:12]}")
        else:
            bad("Champion alignment", "winner mismatch")
    else:
        bad("Champion files", "metrics.json or baseline.json missing locally")
    if nb.is_file() and nb.stat().st_size > 5000:
        ok("cai_demo.ipynb", f"{nb.stat().st_size // 1024} KB")
    else:
        bad("cai_demo.ipynb", "missing or too small")


def main() -> int:
    missing = [k for k in ("EVX_IMPALA_USER", "EVX_IMPALA_PASSWORD") if not os.environ.get(k)]
    if missing:
        print(f"set {missing} in .env first", file=sys.stderr)
        return 1
    check_http()
    check_impala()
    check_dashboards()
    check_cai_notebook_artifacts()
    print(f"\n{'All checks passed.' if not FAIL else f'{FAIL} check(s) failed.'}", flush=True)
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
