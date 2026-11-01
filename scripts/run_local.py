"""
The pipeline on a laptop or in CI: the same CDE job files on local Spark + Iceberg (SQLite JDBC catalog),
CI-scale data (config/ci.yaml). Nothing here talks to Cloudera.

  .venv/bin/python scripts/run_local.py --stages generate,land,bronze --from 2026-09-01 --to 2026-09-20
  .venv/bin/python scripts/run_local.py --stages bronze --business-date 2026-11-07

Folders: data/generated, data/landing, data/raw, data/warehouse (all git-ignored).
EVX_ICEBERG_PACKAGE=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2 with pyspark 3.5.4 and
EVX_LOCAL_CATALOG=hadoop reproduce CDE's Spark and Iceberg versions.
"""
from __future__ import annotations

import argparse
import importlib
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
JOBS_DIR = ROOT / "cde" / "jobs"
sys.path.insert(0, str(JOBS_DIR))
sys.path.insert(0, str(ROOT))
os.environ.setdefault("EVX_CONFIG_OVERLAY", "config/ci.yaml")
import evx_common as C  # noqa: E402

ICEBERG_PACKAGE = os.environ.get("EVX_ICEBERG_PACKAGE", "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0")
SQLITE_PACKAGE = "org.xerial:sqlite-jdbc:3.46.1.3"
DATA = ROOT / "data"


def local_spark(warehouse: Path, driver_memory: str = "4g"):
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ["PYTHONPATH"] = os.pathsep.join([str(JOBS_DIR), str(ROOT), os.environ.get("PYTHONPATH", "")])
    warehouse.mkdir(parents=True, exist_ok=True)
    b = (SparkSession.builder.appName("evx-local").master("local[4]")
         .config("spark.driver.host", "127.0.0.1").config("spark.driver.bindAddress", "127.0.0.1")
         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.local.warehouse", str(warehouse)))
    if os.environ.get("EVX_LOCAL_CATALOG") == "hadoop":
        b = b.config("spark.jars.packages", ICEBERG_PACKAGE).config("spark.sql.catalog.local.type", "hadoop")
    else:
        b = (b.config("spark.jars.packages", f"{ICEBERG_PACKAGE},{SQLITE_PACKAGE}")
             .config("spark.sql.catalog.local.type", "jdbc")
             .config("spark.sql.catalog.local.uri",
                     f"jdbc:sqlite:{warehouse.resolve() / 'catalog.db'}?journal_mode=WAL&busy_timeout=60000")
             .config("spark.sql.catalog.local.clients", "1")
             .config("spark.sql.catalog.local.jdbc.schema-version", "V1"))
    spark = (b.config("spark.sql.defaultCatalog", "local")
             .config("spark.driver.memory", driver_memory)
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.default.parallelism", "4")
             .config("spark.ui.enabled", "false")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    C.configure(spark)
    return spark


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--stages", default="generate,land,bronze")
    p.add_argument("--business-date", default=None)
    p.add_argument("--from", dest="date_from", default=None)
    p.add_argument("--to", dest="date_to", default=None)
    p.add_argument("--scale", default="0.05")
    p.add_argument("--warehouse", default=str(DATA / "warehouse"))
    args = p.parse_args(argv)
    d0 = args.business_date or args.date_from
    d1 = args.business_date or args.date_to or d0
    rng = ["--from", d0, "--to", d1] if d0 else []
    paths = ["--landing", str(DATA / "landing")]
    stages = args.stages.split(",")
    spark = None
    for stage in stages:
        t0 = time.time()
        print(f"== {stage}", flush=True)
        if stage == "generate":
            from generators.run import prepare, run_local

            cfg, keys = prepare(C.config(), float(args.scale))
            run_local(DATA / "generated", cfg, keys)
        elif stage == "land":
            importlib.import_module("land_sources").main(rng + paths + ["--generated", str(DATA / "generated")])
        else:
            spark = spark or local_spark(Path(args.warehouse))
            job = {"bronze": "ingest_bronze", "extract": "extract_unstructured"}[stage]
            extra = ["--raw", str(DATA / "raw"), "--threads", "8"] if stage == "bronze" else []
            importlib.import_module(job).run(spark, rng + paths + extra)
        print(f"== {stage} done in {time.time() - t0:.0f}s", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
