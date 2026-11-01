"""
Stage -1 - Generate: runs the synthetic source systems for the whole calendar into <root>/generated/,
which land_sources.py then releases into landing one business date at a time.

  objects   <generated>/<source>/<business_date>/<object>      written from the driver (thread pool)
  exports   <generated>/<table>/business_date=<d>/part-*.csv    Spark CSV, header, all strings
  truth     <prefix>_ref.generator_truth_pack / _truth_batch / generator_fleet   (Iceberg)

Partition keys are (cell line, pack-assembly month) plus SUPPLY; executors generate, records are persisted
once, and the same (seed, scale) gives the same bytes as `python -m generators.run`.

Usage:
  spark-submit generate.py [--scale 1.0] [--overlay config/ci.yaml] [--only CL1:2026-09] [--no-truth]
  python cde/jobs/generate.py --out data/generated --scale 0.05            # laptop, local Spark
"""
from __future__ import annotations

import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import evx_common as C  # noqa: E402

from generators.run import TABLES, as_strings, partition_records, prepare, row_date, write_manifests  # noqa: E402

TRUTH_TABLES = {"truth_pack": "generator_truth_pack", "truth_batch": "generator_truth_batch",
                "fleet": "generator_fleet", "truth_fault": "generator_truth_fault"}


def _records(cfg: dict):
    def run(keys):
        for key in keys:
            for rec in partition_records(cfg, tuple(key)):
                kind = rec[0]
                if kind == "file":
                    yield ("file", rec[1], bytes(rec[2]))
                elif kind == "row":
                    yield ("row", rec[1], as_strings(rec[2]))
                else:
                    yield (kind, "", json.dumps(rec[1], default=str, sort_keys=True))
    return run


def write_objects(fs, out: str, files_rdd, threads: int) -> int:
    """One partition at a time to the driver (tens of MB each at default scale), written by a thread pool."""
    sc, n = files_rdd.context, 0
    with ThreadPoolExecutor(max_workers=threads) as pool:
        for i in range(files_rdd.getNumPartitions()):
            part = sc.runJob(files_rdd, lambda it: list(it), [i])
            for f in [pool.submit(fs.write_bytes, f"{out}/{rel}", data) for rel, data in part]:
                f.result()
            n += len(part)
    return n


def write_exports(spark, out: str, rows_rdd) -> dict:
    from pyspark.sql import Row  # noqa: F401

    counts = {}
    for table, (cols, _) in TABLES.items():
        sub = rows_rdd.filter(lambda r, t=table: r[0] == t).map(lambda r, t=table: (*r[1], row_date(t, r[1])))
        df = spark.createDataFrame(sub, ", ".join(f"`{c}` string" for c in cols) + ", business_date string")
        df.repartition("business_date").write.mode("overwrite").partitionBy("business_date") \
            .option("header", True).csv(f"{out}/{table}")
        for day, n in df.groupBy("business_date").count().collect():
            counts[(table, day)] = [1, int(n)]
    return counts


def main(argv=None, spark=None) -> int:
    p = C.base_parser(__doc__.split("\n\n")[0], dated=False)
    p.add_argument("--out", default=None, help="default: <landing>/../generated")
    p.add_argument("--scale", type=float, default=None, help="default: lakehouse.generator.scale or 1.0")
    p.add_argument("--only", default=None, help="comma list of partition keys line:month (debug)")
    p.add_argument("--threads", type=int, default=32)
    p.add_argument("--no-truth", action="store_true")
    args = C.parse(p, argv)
    cfg = C.config(args.overlay)
    scale = args.scale if args.scale is not None else float(cfg.get("generator", {}).get("scale", 1.0))
    out = C.as_uri(args.out or C.sibling(args.landing, "generated"))
    spark = spark or C.get_spark("evx-generate")
    C.ship_packages(spark)
    fs = C.filesystem(spark, out)
    names = C.Names(args.db_prefix)

    t0 = time.time()
    cfg, keys = prepare(cfg, scale)
    if args.only:
        want = {tuple(x.split(":")) for x in args.only.split(",")}
        keys = [k for k in keys if k in want]
    print(f"generate scale={scale} partitions={len(keys)} -> {out}", flush=True)
    fs.delete(out)

    from pyspark import StorageLevel

    recs = spark.sparkContext.parallelize([list(k) for k in keys], len(keys)) \
        .mapPartitions(_records(cfg)).persist(StorageLevel.MEMORY_AND_DISK)
    files = recs.filter(lambda r: r[0] == "file").map(lambda r: (r[1], r[2]))
    counts = {k: list(v) for k, v in files.map(lambda f: ((f[0].split("/")[0], f[0].split("/")[1]),
                                                          (1, len(f[1]))))
              .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).collect()}
    print(f"  generated in {time.time() - t0:.0f}s", flush=True)

    n = write_objects(fs, out, files, args.threads)
    print(f"  objects  {n} written in {time.time() - t0:.0f}s", flush=True)
    counts.update(write_exports(spark, out, recs.filter(lambda r: r[0] == "row").map(lambda r: (r[1], r[2]))))
    with ThreadPoolExecutor(max_workers=args.threads) as pool:
        jobs = []
        write_manifests(lambda rel, text: jobs.append(pool.submit(fs.write_text, f"{out}/{rel}", text)), counts)
        for f in jobs:
            f.result()
    print(f"  exports and {len(counts)} manifests in {time.time() - t0:.0f}s", flush=True)

    truth_stats = {}
    if not args.no_truth:
        C.ensure_databases(spark, names, ("ref",))
        for kind, table in TRUTH_TABLES.items():
            js = recs.filter(lambda r, k=kind: r[0] == k).map(lambda r: r[2])
            if js.isEmpty():
                continue
            df = spark.read.json(js)
            C.replace_table(df, names.t("ref", table))
            truth_stats[table] = df.count()
        from generators.run import label_stats

        packs = [json.loads(s) for s in recs.filter(lambda r: r[0] == "truth_pack").map(lambda r: r[2]).collect()]
        truth_stats["label"] = label_stats(packs, cfg["calendar"]["history_end"], cfg["calendar"]["label_days"])
    recs.unpersist()

    by_source: dict = {}
    for (src, _), (n_obj, size) in counts.items():
        a = by_source.setdefault(src, [0, 0])
        a[0] += n_obj
        a[1] += size
    # one short line each: CDE log lines are cut at about 1 KB
    print(f"GENERATE_RESULT scale={scale} seconds={round(time.time() - t0)}", flush=True)
    for src, (n_obj, size) in sorted(by_source.items()):
        print(f"GENERATE_SOURCE {src} {n_obj} {size}", flush=True)
    for k, v in truth_stats.items():
        print(f"GENERATE_TRUTH {k} {json.dumps(v, default=str)}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
