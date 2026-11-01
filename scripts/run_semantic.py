"""
Semantic layer: the views in sql/semantic/ (NN_<name>.sql, a SELECT with ${gold} / ${semantic} placeholders)
and the KPI consistency check.

  .venv/bin/python scripts/run_semantic.py --steps views,kpi_check                  # CDW Impala
  .venv/bin/python scripts/run_semantic.py --steps views,kpi_check --engine spark   # local Spark (CI)

On Impala every view is a view in <prefix>_semantic (dropped and created), after a table-scoped
INVALIDATE METADATA of each gold table it reads. On local Spark they are temporary views.
The check compares each KPI's totals with an independent count on gold, the two FIR90 cuts with each other,
and keeps every rate within 0..1.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
SQL_DIR = ROOT / "sql" / "semantic"
GOLD_REF = re.compile(r"\$\{gold\}\.(\w+)")


def view_files() -> list[tuple[str, str]]:
    out = []
    for f in sorted(SQL_DIR.glob("[0-9][0-9]_*.sql")):
        body = "\n".join(line for line in f.read_text().splitlines() if not line.lstrip().startswith("--"))
        out.append((f.stem.split("_", 1)[1], body.strip()))
    return out


class Impala:
    def __init__(self, prefix: str):
        from lakehouse.impala import connect, query

        self.conn, self._query = connect(), query
        self.gold, self.semantic = f"{prefix}_gold", f"{prefix}_semantic"

    def rows(self, sql: str) -> list[tuple]:
        return self._query(self.conn, sql)[1]

    def create(self, name: str, body: str) -> None:
        self.rows(f"DROP VIEW IF EXISTS {self.semantic}.{name}")
        self.rows(f"CREATE VIEW {self.semantic}.{name} AS {body}")

    def prepare(self, gold_tables: set[str]) -> None:
        if not self.rows(f"SHOW DATABASES LIKE '{self.semantic}'"):
            raise SystemExit(f"{self.semantic} does not exist (created by the CDE jobs)")
        for t in sorted(gold_tables):
            self.rows(f"INVALIDATE METADATA {self.gold}.{t}")

    def sub(self, sql: str) -> str:
        return sql.replace("${gold}", self.gold).replace("${semantic}", self.semantic)


class LocalSpark(Impala):
    def __init__(self, prefix: str):
        from run_local import DATA, local_spark

        self.spark = local_spark(DATA / "warehouse")
        self.gold, self.semantic = f"{prefix}_gold", ""

    def rows(self, sql: str) -> list[tuple]:
        return [tuple(r) for r in self.spark.sql(sql).collect()]

    def create(self, name: str, body: str) -> None:
        self.spark.sql(body).createOrReplaceTempView(name)

    def prepare(self, gold_tables: set[str]) -> None:
        pass

    def sub(self, sql: str) -> str:
        return sql.replace("${gold}", self.gold).replace("${semantic}.", "")


CHECKS = [  # label, KPI side, independent side (None: the KPI side must be 0)
    ("cells graded", "SELECT sum(cells_graded) FROM ${semantic}.kpi_fpy_cell",
     "SELECT count(*) FROM ${gold}.fact_cell_grading WHERE production_date IS NOT NULL"),
    ("cells first pass", "SELECT sum(cells_first_pass) FROM ${semantic}.kpi_fpy_cell",
     "SELECT count(*) FROM ${gold}.fact_cell_grading g WHERE g.production_date IS NOT NULL "
     "AND g.grade <> 'REJECT' AND NOT EXISTS "
     "(SELECT 1 FROM ${gold}.fact_qc_inspection x WHERE x.cell_id = g.cell_id AND x.inspection = 'XRAY' "
     "AND x.verdict = 'FAIL')"),
    ("packs tested", "SELECT sum(packs_tested) FROM ${semantic}.kpi_fpy_pack",
     "SELECT count(*) FROM ${gold}.fact_pack_eol"),
    ("packs first pass", "SELECT sum(packs_first_pass) FROM ${semantic}.kpi_fpy_pack",
     "SELECT count(*) FROM ${gold}.fact_pack_eol WHERE eol_result = 'PASS'"),
    ("vehicles", "SELECT count(*) FROM ${semantic}.v_vehicle_fir90",
     "SELECT count(DISTINCT vin) FROM ${gold}.dim_vehicle"),
    ("FIR90 matured, week = batch cut", "SELECT sum(vehicles_matured) FROM ${semantic}.kpi_fir90_build_week",
     "SELECT sum(vehicles_matured) FROM ${semantic}.kpi_fir90_supplier_batch"),
    ("FIR90 failed, week = batch cut", "SELECT sum(vehicles_failed) FROM ${semantic}.kpi_fir90_build_week",
     "SELECT sum(vehicles_failed) FROM ${semantic}.kpi_fir90_supplier_batch"),
    ("FIR90 failed = battery claims in 90 days on matured vehicles",
     "SELECT sum(vehicles_failed) FROM ${semantic}.kpi_fir90_build_week",
     "SELECT count(DISTINCT w.resolved_vin) FROM ${gold}.fact_warranty_claim w "
     "JOIN ${gold}.dim_vehicle v ON v.vin = w.resolved_vin AND v.is_current "
     "CROSS JOIN (SELECT max(_business_date) AS d FROM ${gold}.fact_vehicle_daily) a "
     "WHERE w.battery_related "
     "AND datediff(w.failure_date, coalesce(v.delivery_date, CAST(v.eol_ts AS DATE))) BETWEEN 0 AND 90 "
     "AND datediff(a.d, coalesce(v.delivery_date, CAST(v.eol_ts AS DATE))) >= 90"),
    ("FPY outside 0..1", "SELECT count(*) FROM ${semantic}.kpi_fpy_cell WHERE fpy < 0 OR fpy > 1 OR shift IS NULL", None),
    ("pack FPY outside 0..1", "SELECT count(*) FROM ${semantic}.kpi_fpy_pack WHERE fpy < 0 OR fpy > 1", None),
    ("FIR90 above 1,000", "SELECT count(*) FROM ${semantic}.kpi_fir90_build_week WHERE fir90_per_1000 > 1000", None),
]


def kpi_check(eng) -> list[str]:
    errors = []
    for label, kpi, ref in CHECKS:
        a = eng.rows(eng.sub(kpi))[0][0] or 0
        b = (eng.rows(eng.sub(ref))[0][0] or 0) if ref else 0
        ok = a == b
        print(f"KPI_CHECK {'OK' if ok else 'FAIL'} {label}: {a}" + (f" vs {b}" if ref else ""), flush=True)
        if not ok:
            errors.append(label)
    for name in ("kpi_fpy_cell", "kpi_fpy_pack"):
        if not eng.rows(eng.sub(f"SELECT count(*) FROM ${{semantic}}.{name}"))[0][0]:
            errors.append(f"{name} is empty")
    head = eng.rows(eng.sub(
        "SELECT round(sum(cells_first_pass) / sum(cells_graded), 4) FROM ${semantic}.kpi_fpy_cell UNION ALL "
        "SELECT round(sum(packs_first_pass) / sum(packs_tested), 4) FROM ${semantic}.kpi_fpy_pack UNION ALL "
        "SELECT round(1000 * sum(vehicles_failed) / sum(vehicles_matured), 2) FROM ${semantic}.kpi_fir90_build_week"))
    print(f"KPI_HEADLINE cell_fpy={head[0][0]} pack_fpy={head[1][0]} fir90_per_1000={head[2][0]}", flush=True)
    return errors


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--steps", default="views,kpi_check")
    p.add_argument("--engine", choices=("impala", "spark"), default="impala")
    p.add_argument("--db-prefix", default="rsingh_evx")
    args = p.parse_args(argv)
    eng = (Impala if args.engine == "impala" else LocalSpark)(args.db_prefix)
    steps = args.steps.split(",")
    views = view_files()
    if "views" in steps or "kpi_check" in steps:
        if args.engine == "spark" or "views" in steps:
            eng.prepare({t for _, body in views for t in GOLD_REF.findall(body)})
            for name, body in views:
                eng.create(name, eng.sub(body))
                print(f"SEMANTIC_VIEW {name} {eng.rows(eng.sub(f'SELECT count(*) FROM ${{semantic}}.{name}'))[0][0]}",
                      flush=True)
    errors = kpi_check(eng) if "kpi_check" in steps else []
    for e in errors:
        print(f"FAIL {e}")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
