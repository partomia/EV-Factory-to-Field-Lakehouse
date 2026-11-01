"""
Shared helpers for the EVX CDE Spark jobs (driver side, plus the few functions executors call).
Standard library and PySpark only, so the jobs need no python-env resource on CDE.
Copied from OGX cde/jobs/ogx_common.py (itself from GDL) and adapted.

  names        <prefix>_<layer>.<table>; layers bronze, silver, master, gold, features, semantic, ref
  spark        session settings every job relies on (ANSI off: a bad value casts to NULL and is reported)
  iceberg      one atomic commit per write; snapshot ids for lineage; gold goes through cde/jobs/publish.py
  audit        ref.load_audit and ref.transform_log
  fs           landing zone and raw objects through Hadoop (s3a:// or file://)

On CDE the repository (a Git repository resource) is mounted at /app/mount, so config/, contracts/,
generators/ and extract/ are importable there and on a laptop alike.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

LAYERS = ("bronze", "silver", "master", "gold", "features", "semantic", "ref")

# Impala writes some ref tables too and cannot write an Iceberg timestamptz column, so their times are
# timestamp_ntz holding UTC (the session time zone).
LOAD_AUDIT_SCHEMA = (
    "run_id string, pipeline_run string, batch_id string, business_date date, stage string, "
    "entity string, status string, rows_in bigint, rows_out bigint, rows_rejected bigint, "
    "snapshot_before bigint, snapshot_after bigint, started_at timestamp_ntz, ended_at timestamp_ntz, "
    "message string")
TRANSFORM_LOG_SCHEMA = (
    "run_id string, pipeline_run string, batch_id string, business_date date, job string, "
    "step string, transform_type string, source_tables string, target_table string, "
    "rows_in bigint, rows_out bigint, snapshot_id bigint, logged_at timestamp_ntz, details string")
TRANSFORM_TYPES = ("ingest", "validate", "cleanse", "standardise", "deduplicate", "match", "survive",
                   "enrich", "normalise", "aggregate", "historise", "extract", "consume", "generate")


def repo_root() -> Path:
    here = Path(__file__).resolve().parents[2] if len(Path(__file__).resolve().parents) > 2 else Path("/")
    if (here / "config" / "lakehouse.json").exists():
        return here
    for p in (os.environ.get("EVX_REPO"), "/app/mount", os.getcwd()):
        if p and (Path(p) / "config" / "lakehouse.json").exists():
            return Path(p)
    return here


def _import_path() -> None:
    root = str(repo_root())
    if root not in sys.path:
        sys.path.insert(0, root)


_import_path()


def load_json(rel: str):
    return json.loads((repo_root() / rel).read_text())


def config(overlay_path: str | None = None) -> dict:
    """config/lakehouse.json, merged with an overlay (EVX_CONFIG_OVERLAY or --overlay: config/ci.yaml's
    `lakehouse:` block as JSON-compatible YAML subset)."""
    from generators.core import deep_merge, load_config

    cfg = load_config()
    path = overlay_path or os.environ.get("EVX_CONFIG_OVERLAY")
    if path:
        ov = read_overlay(repo_root() / path if not Path(path).is_absolute() else Path(path))
        cfg = deep_merge(cfg, ov.get("lakehouse", {}))
    return cfg


def read_overlay(path: Path) -> dict:
    text = path.read_text()
    if path.suffix == ".json":
        return json.loads(text)
    try:
        import yaml  # laptop / CI

        return yaml.safe_load(text) or {}
    except ImportError:  # CDE: no PyYAML; the overlay also ships as JSON next to it
        return json.loads(path.with_suffix(".json").read_text())


def contracts() -> dict:
    """contracts/sources/*.json by entity, in file-name order."""
    return {json.loads(p.read_text())["entity"]: json.loads(p.read_text())
            for p in sorted((repo_root() / "contracts" / "sources").glob("*.json"))}


def object_contracts() -> dict:
    return {p.stem: json.loads(p.read_text()) for p in sorted((repo_root() / "contracts" / "objects").glob("*.json"))}


DATE_IN_PATH = re.compile(r"/(\d{4}-\d{2}-\d{2})/[^/]+$")


def date_of_path(path: str) -> str | None:
    m = DATE_IN_PATH.search(path)
    return m.group(1) if m else None


try:
    CFG = load_json("config/lakehouse.json")
except OSError:
    CFG = {"db_prefix": "rsingh_evx", "landing": ""}
DEFAULT_PREFIX = CFG["db_prefix"]
DEFAULT_LANDING = CFG.get("landing", "")


def batch_id(business_date: date) -> str:
    return f"B{business_date:%Y%m%d}"


def parse_date(s: str) -> date:
    return date.fromisoformat(s)


def base_parser(description: str, dated: bool = True) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--db-prefix", default=DEFAULT_PREFIX)
    if dated:
        p.add_argument("--business-date", type=parse_date, default=None,
                       help="one business date (DAG); or --from/--to for the initial load / backfill")
        p.add_argument("--from", dest="date_from", type=parse_date, default=None)
        p.add_argument("--to", dest="date_to", type=parse_date, default=None)
    p.add_argument("--landing", default=DEFAULT_LANDING)
    p.add_argument("--overlay", default=None, help="config overlay (config/ci.yaml)")
    p.add_argument("--pipeline-run", default=None, help="groups the stages of one DAG run (Airflow run_id)")
    return p


def parse(parser: argparse.ArgumentParser, argv=None) -> argparse.Namespace:
    args, _ = parser.parse_known_args(argv)
    if hasattr(args, "business_date"):
        if args.business_date and not args.date_from:
            args.date_from = args.date_to = args.business_date
        if args.date_from and not args.date_to:
            args.date_to = args.date_from
    return args


def date_range(a: date, b: date) -> list[date]:
    return [a + timedelta(days=i) for i in range((b - a).days + 1)]


class Names:
    def __init__(self, prefix: str = DEFAULT_PREFIX):
        self.prefix = prefix

    def db(self, layer: str) -> str:
        assert layer in LAYERS, layer
        return f"{self.prefix}_{layer}"

    def t(self, layer: str, name: str) -> str:
        return f"{self.db(layer)}.{name}"


# ---------------------------------------------------------------- spark


def configure(spark) -> None:
    spark.conf.set("spark.sql.ansi.enabled", "false")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    try:
        spark.sparkContext.addPyFile(str(Path(__file__).resolve()))
    except Exception:
        pass


def get_spark(app: str):
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(app).getOrCreate()
    configure(spark)
    return spark


def ship_packages(spark) -> None:
    """Executors need generators/ and extract/ (CDE mounts the repo on the driver only)."""
    import shutil
    import tempfile

    root = repo_root()
    tmp = Path(tempfile.mkdtemp(prefix="evx-pkgs-"))
    for pkg in ("generators", "extract"):
        if (root / pkg).is_dir():
            archive = shutil.make_archive(str(tmp / pkg), "zip", root_dir=str(root), base_dir=pkg)
            spark.sparkContext.addPyFile(archive)


def ensure_databases(spark, names: Names, layers=LAYERS) -> None:
    for layer in layers:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {names.db(layer)}")


# ---------------------------------------------------------------- iceberg


def table_exists(spark, table: str) -> bool:
    return spark.catalog.tableExists(table)


def snapshot_id(spark, table: str) -> int | None:
    if not table_exists(spark, table):
        return None
    rows = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()
    return int(rows[0][0]) if rows else None


def _create(df, table: str, partition_cols=(), props: dict | None = None):
    from pyspark.sql import functions as F

    w = df.writeTo(table).using("iceberg").tableProperty("format-version", "2")
    for k, v in (props or {}).items():
        w = w.tableProperty(k, str(v))
    if partition_cols:
        w = w.partitionedBy(*[F.col(c) for c in partition_cols])
    return w


def write_partitions(df, table: str, partition_cols, props: dict | None = None) -> None:
    """Replace exactly the partitions present in df, in one commit (a re-run of a batch)."""
    if table_exists(df.sparkSession, table):
        df.writeTo(table).overwritePartitions()
    else:
        _create(df, table, partition_cols, props).create()


def replace_table(df, table: str, partition_cols=(), props: dict | None = None) -> None:
    _create(df, table, partition_cols, props).createOrReplace()


def append(df, table: str, partition_cols=()) -> None:
    if table_exists(df.sparkSession, table):
        df.writeTo(table).append()
    else:
        _create(df, table, partition_cols).create()


def ensure_table(spark, table: str, schema: str, partition_cols=()) -> None:
    if not table_exists(spark, table):
        _create(spark.createDataFrame([], schema), table, partition_cols).create()


# ---------------------------------------------------------------- audit


def now_utc() -> datetime:
    """Aware UTC: PySpark reads a naive datetime as the driver's local time, not the session's."""
    return datetime.now(timezone.utc)


class Audit:
    """ref.load_audit rows as they happen (a failed run keeps what it logged); ref.transform_log buffered."""

    def __init__(self, spark, names: Names, job: str, business_date: date, pipeline_run: str | None):
        self.spark, self.names, self.job = spark, names, job
        self.business_date, self.batch = business_date, batch_id(business_date)
        self.run_id = f"{job}-{uuid.uuid4().hex[:12]}"
        self.pipeline_run = pipeline_run or os.environ.get("EVX_PIPELINE_RUN") or self.run_id
        self._steps: list[tuple] = []
        ensure_table(spark, names.t("ref", "load_audit"), LOAD_AUDIT_SCHEMA)
        ensure_table(spark, names.t("ref", "transform_log"), TRANSFORM_LOG_SCHEMA)

    def load(self, stage: str, entity: str, status: str, *, rows_in=None, rows_out=None, rows_rejected=None,
             snapshot_before=None, snapshot_after=None, started_at=None, message: str = "") -> None:
        row = (self.run_id, self.pipeline_run, self.batch, self.business_date, stage, entity, status,
               rows_in, rows_out, rows_rejected, snapshot_before, snapshot_after,
               (started_at or now_utc()).replace(tzinfo=None), now_utc().replace(tzinfo=None), message[:2000])
        self.spark.createDataFrame([row], LOAD_AUDIT_SCHEMA).writeTo(self.names.t("ref", "load_audit")).append()

    def transform(self, step: str, transform_type: str, sources, target: str, rows_in=None, rows_out=None,
                  details: str = "") -> None:
        assert transform_type in TRANSFORM_TYPES, transform_type
        sources = sources if isinstance(sources, str) else ",".join(sources)
        snap = snapshot_id(self.spark, target) if "." in target and table_exists(self.spark, target) else None
        self._steps.append((self.run_id, self.pipeline_run, self.batch, self.business_date, self.job, step,
                            transform_type, sources, target, rows_in, rows_out, snap,
                            now_utc().replace(tzinfo=None), details[:2000]))

    def flush(self) -> None:
        if self._steps:
            self.spark.createDataFrame(self._steps, TRANSFORM_LOG_SCHEMA) \
                .writeTo(self.names.t("ref", "transform_log")).append()
            self._steps = []


# ---------------------------------------------------------------- fs


def sibling(landing: str, name: str) -> str:
    return landing.rstrip("/").rsplit("/", 1)[0] + "/" + name


class HadoopFS:
    """Files through the Hadoop FileSystem of the Spark session (s3a:// on CDE, file:// locally)."""

    def __init__(self, spark):
        self.jvm = spark._jvm
        self.conf = spark._jsc.hadoopConfiguration()

    def _fs(self, uri: str):
        path = self.jvm.org.apache.hadoop.fs.Path(uri)
        return path.getFileSystem(self.conf), path

    def write_bytes(self, uri: str, data: bytes) -> None:
        fs, path = self._fs(uri)
        out = fs.create(path, True)
        try:
            out.write(bytearray(data))
        finally:
            out.close()

    def write_text(self, uri: str, text: str) -> None:
        self.write_bytes(uri, text.encode("utf-8"))

    def exists(self, uri: str) -> bool:
        fs, path = self._fs(uri)
        return bool(fs.exists(path))

    def delete(self, uri: str) -> None:
        fs, path = self._fs(uri)
        if fs.exists(path):
            fs.delete(path, True)

    def read_bytes(self, uri: str) -> bytes:
        fs, path = self._fs(uri)
        stream = fs.open(path)
        try:
            return bytes(self.jvm.org.apache.commons.io.IOUtils.toByteArray(stream))
        finally:
            stream.close()

    def read_text(self, uri: str) -> str:
        return self.read_bytes(uri).decode("utf-8")

    def list(self, uri: str) -> list[tuple[str, int]]:
        fs, path = self._fs(uri)
        if not fs.exists(path):
            return []
        return sorted((s.getPath().getName(), int(s.getLen())) for s in fs.listStatus(path) if s.isFile())

    def list_dirs(self, uri: str) -> list[str]:
        fs, path = self._fs(uri)
        if not fs.exists(path):
            return []
        return sorted(s.getPath().getName() for s in fs.listStatus(path) if s.isDirectory())

    def copy(self, src: str, dst: str) -> None:
        sfs, sp = self._fs(src)
        dfs, dp = self._fs(dst)
        self.jvm.org.apache.hadoop.fs.FileUtil.copy(sfs, sp, dfs, dp, False, True, self.conf)


class LocalFS:
    """The same interface over a local folder (file:// URIs or plain paths), for a laptop or CI."""

    @staticmethod
    def _path(uri: str) -> Path:
        return Path(re.sub(r"^file:/+", "/", uri))

    def write_bytes(self, uri: str, data: bytes) -> None:
        p = self._path(uri)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)

    def write_text(self, uri: str, text: str) -> None:
        self.write_bytes(uri, text.encode("utf-8"))

    def exists(self, uri: str) -> bool:
        return self._path(uri).exists()

    def delete(self, uri: str) -> None:
        import shutil

        p = self._path(uri)
        if p.is_dir():
            shutil.rmtree(p)
        elif p.exists():
            p.unlink()

    def read_bytes(self, uri: str) -> bytes:
        return self._path(uri).read_bytes()

    def read_text(self, uri: str) -> str:
        return self._path(uri).read_text("utf-8")

    def list(self, uri: str) -> list[tuple[str, int]]:
        p = self._path(uri)
        return sorted((f.name, f.stat().st_size) for f in p.iterdir() if f.is_file()) if p.is_dir() else []

    def list_dirs(self, uri: str) -> list[str]:
        p = self._path(uri)
        return sorted(f.name for f in p.iterdir() if f.is_dir()) if p.is_dir() else []

    def copy(self, src: str, dst: str) -> None:
        import shutil

        s, t = self._path(src), self._path(dst)
        t.parent.mkdir(parents=True, exist_ok=True)
        if s.is_dir():
            shutil.copytree(s, t, dirs_exist_ok=True)
        else:
            shutil.copy2(s, t)


def filesystem(spark, uri: str):
    return HadoopFS(spark) if spark is not None and not uri.startswith(("file:", "/")) else LocalFS()


def as_uri(path_or_uri: str) -> str:
    if "://" in path_or_uri:
        return path_or_uri.rstrip("/")
    return "file://" + str(Path(path_or_uri).resolve())


# ---------------------------------------------------------------- typing (Spark columns)


def typed(value, coldef: dict):
    """A bronze string as the contract's type; blank -> NULL."""
    from pyspark.sql import functions as F

    v = F.trim(value)
    v = F.when(v != "", v)
    kind = coldef["type"]
    fmts = coldef.get("formats") or ([coldef["format"]] if coldef.get("format") else [])
    if kind in ("int", "bigint", "double", "boolean"):
        return v.cast(kind)
    if kind == "decimal":
        return v.cast("decimal(18,2)")
    if kind == "date":
        return F.coalesce(*[F.to_date(v, f) for f in fmts]) if fmts else v.cast("date")
    if kind == "timestamp":
        return F.coalesce(*[F.to_timestamp(v, f) for f in fmts]) if fmts else v.cast("timestamp")
    return v


def error_summary(e: BaseException, limit: int = 500) -> str:
    text = str(e)
    errors = [ln.strip() for ln in text.splitlines() if re.match(r"\s*\w+(Error|Exception):", ln)]
    return (errors[-1] if errors else f"{type(e).__name__}: {text.strip().splitlines()[0] if text.strip() else ''}")[:limit]


def fail_on_partition(partition_id: int, target: int = 1) -> int:
    """For the failed-batch drill: one task of the write fails, so Spark aborts before the commit."""
    if partition_id == target:
        raise RuntimeError(f"simulated failure in task for partition {partition_id}")
    return partition_id
