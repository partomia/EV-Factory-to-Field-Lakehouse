"""
Stage 1 - Bronze: ingest landed files as received, for one business date (DAG) or a range (initial load).

Records (contracts/sources/*.json):
  parse      pipe / comma files by header name (trailer TRL|<n> read), NDJSON documents (fields by path, document
             kept as payload), SAP OData v2 pages (one row per record, payload as received)
  validate   every record against its contract (required, type, timestamp formats, pattern, allowed, minimum);
             rejects -> bronze.quarantine with reason codes; warnings kept on the row
  files      a trailer that does not match the data rows quarantines the whole file (ref.file_control MISMATCH);
             an OData page whose delta token was already ingested is a re-send: dropped and counted
             (DUPLICATE_DELTA); a field not in the contract is recorded once in ref.schema_change and kept in
             the payload (additive schema evolution)
  metadata   _batch_id, _business_date (the landing folder's date), _source_system, _source_file, _source_row,
             _ingested_at, _record_hash, _warnings

Objects (contracts/objects/*.json; X-ray / film PNG, job-card / CoA / 8D PDF, JCAMP-DX, EDR):
  fingerprint and validate on executors (binaryFile); compare with the manifest's sha256; store new valid
  objects once in <raw>/<format>/<sha256>.<ext> (immutable, deduplicated); one bronze.doc_object row per file
  (VALID / DUPLICATE / QUARANTINED); failures also -> bronze.quarantine. Bytes never go into Iceberg.

Each table gets one Iceberg commit replacing the partitions (business dates) present, so a re-run never
duplicates rows. ref.load_audit: STARTED, COMMITTED per entity, COMPLETED or FAILED.

Usage:
  spark-submit ingest_bronze.py --business-date 2026-11-06
  spark-submit ingest_bronze.py --from 2026-02-09 --to 2026-11-05
"""
from __future__ import annotations

import hashlib
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import evx_common as C  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

STAGE = "bronze"
FILE_CONTROL_SCHEMA = ("batch_id string, business_date date, source string, entity string, source_file string, "
                       "header_ok boolean, data_records bigint, trailer_records bigint, status string, "
                       "detail string, logged_at timestamp_ntz")
SCHEMA_CHANGE_SCHEMA = ("entity string, entity_set string, field string, first_seen_date date, change_type string, "
                        "action string, source_file string, logged_at timestamp_ntz")
QUARANTINE_SCHEMA = ("_entity string, _batch_id string, _business_date date, _source_system string, "
                     "_source_file string, _source_row int, _reject_reasons array<string>, _warnings array<string>, "
                     "_record string, _record_hash string, _ingested_at timestamp_ntz")
QUARANTINE_COLS = [c.split(" ")[0] for c in QUARANTINE_SCHEMA.split(", ")]
DOC_OBJECT_SCHEMA = (
    "doc_id string, source string, source_path string, file_name string, format string, mime_type string, "
    "size_bytes bigint, sha256 string, manifest_sha256 string, raw_path string, ingest_status string, "
    "reject_reason string, duplicate_of string, facts string, _batch_id string, _business_date date, "
    "_ingested_at timestamp_ntz")


def parser():
    p = C.base_parser(__doc__.split("\n\n")[0])
    p.add_argument("--raw", default=C.CFG.get("raw", ""))
    p.add_argument("--entities", default="", help="comma subset of contract entities and/or doc_object")
    p.add_argument("--threads", type=int, default=32)
    return p


# ---------------------------------------------------------------- validation (from OGX, adapted)


def _checks(columns: list) -> tuple[list, list]:
    rejects, warns = [], []
    for c in columns:
        v = F.trim(F.col(c["name"]))
        blank = v.isNull() | (v == "")
        present = ~blank
        name = c["name"]

        def add(cond, code, severity):
            (rejects if severity == "reject" else warns).append(F.when(cond, F.lit(f"{name}:{code}")))

        if c.get("required"):
            add(blank, "REQUIRED", "reject")
        kind = c["type"]
        if kind in ("int", "bigint"):
            add(present & v.cast("bigint").isNull(), "NOT_INTEGER", "reject")
        elif kind in ("decimal", "double"):
            add(present & v.cast("double").isNull(), "NOT_NUMBER", "reject")
        elif kind == "date":
            add(present & F.coalesce(*[F.to_date(v, f) for f in c["formats"]]).isNull(), "INVALID_DATE", "reject")
        elif kind == "timestamp":
            add(present & F.coalesce(*[F.to_timestamp(v, f) for f in c["formats"]]).isNull(),
                "INVALID_TIMESTAMP", "reject")
        severity = c.get("severity", "warn")
        if "pattern" in c:
            add(present & ~v.rlike(c["pattern"]), "PATTERN", severity)
        if "allowed" in c:
            add(present & ~v.isin(c["allowed"]), "NOT_ALLOWED", severity)
        if "min" in c:
            add(present & (v.cast("double") < c["min"]), "BELOW_MIN", c.get("severity", "reject"))
    return rejects, warns


def _compact(exprs: list):
    if not exprs:
        return F.array().cast("array<string>")
    return F.filter(F.array(*exprs), lambda x: x.isNotNull())


# ---------------------------------------------------------------- writes


# ---------------------------------------------------------------- records


def folders(fs, landing: str, source: str, dates: list[str]) -> list[str]:
    have = set(fs.list_dirs(f"{landing}/{source}"))
    return [f"{landing}/{source}/{d}" for d in dates if d in have]


def ingest_records(spark, names, audit, contract: dict, landing: str, dates: list[str], fs, state: dict) -> dict:
    from extract.records import delimited_rows, ndjson_rows, odata_page

    entity, src, fmt = contract["entity"], contract["source"], contract["format"]
    started = C.now_utc()
    target = names.t("bronze", entity)
    before = C.snapshot_id(spark, target)
    paths = folders(fs, landing, src, dates)
    if not paths:
        return {"rows_in": 0, "accepted": 0, "rejected": 0}
    files = spark.sparkContext.wholeTextFiles(",".join(f"{p}/{contract['file_glob']}" for p in paths),
                                              minPartitions=max(8, len(paths) // 8))
    cols = [c["name"] for c in contract["columns"]]
    control, dropped, extra_reject = [], 0, {}
    now = C.now_utc().replace(tzinfo=None)

    if fmt in ("psv", "csv"):
        parsed = files.map(lambda kv, c=contract: delimited_rows(kv[0], kv[1], c)).cache()
        stats = parsed.map(lambda r: r[1]).collect()
        for s in stats:
            bad = s["has_trailer"] and s["trailer_records"] != s["data_records"]
            missing = contract.get("trailer") and not s["has_trailer"]
            status = "MISMATCH" if bad else "NO_TRAILER" if missing else "MATCH" if s["has_trailer"] else "NO_CONTROL"
            if not s["header_ok"]:
                status = "BAD_HEADER"
            if status in ("MISMATCH", "NO_TRAILER", "BAD_HEADER"):
                extra_reject[s["file"]] = f"file:{'TRAILER_MISMATCH' if bad else status}"
            control.append((C.batch_id(C.parse_date(C.date_of_path(s["file"]))), C.parse_date(C.date_of_path(s["file"])),
                            src, entity, s["file"].rsplit("/", 1)[-1], s["header_ok"], s["data_records"],
                            s["trailer_records"], status, f"trailer {s['trailer_records']} vs rows {s['data_records']}"
                            if s["has_trailer"] else "", now))
        rows = parsed.flatMap(lambda r: r[0])
        df = spark.createDataFrame(rows, "_source_file string, _source_row int, " +
                                   ", ".join(f"`{c}` string" for c in cols))
        parsed_cols = cols
    elif fmt == "ndjson":
        rows = files.flatMap(lambda kv, c=contract: ndjson_rows(kv[0], kv[1], c))
        df = spark.createDataFrame(rows, "_source_file string, _source_row int, " +
                                   ", ".join(f"`{c}` string" for c in cols) + ", payload string, _parse_error string")
        parsed_cols = cols + ["payload"]
    elif fmt == "odata":
        pages = files.map(lambda kv, c=contract: odata_page(kv[0], kv[1], c)).collect()
        seen = state.setdefault("delta_tokens", set())
        tbl = names.t("ref", "file_control")
        if C.table_exists(spark, tbl):
            seen |= {r[0] for r in spark.sql(
                f"SELECT detail FROM {tbl} WHERE entity = '{entity}' AND status = 'LOADED' "
                f"AND business_date < DATE '{dates[0]}'").collect()}
        known = contract["known_fields"]
        changes = state.setdefault("schema_seen", set())
        sc_tbl = names.t("ref", "schema_change")
        if C.table_exists(spark, sc_tbl):
            changes |= {(r[0], r[1]) for r in spark.sql(
                f"SELECT entity_set, field FROM {sc_tbl} WHERE first_seen_date < DATE '{dates[0]}'").collect()}
        keep, sc_rows = [], []
        for pg in sorted(pages, key=lambda p: (C.date_of_path(p["file"]), p["file"])):
            day = C.date_of_path(pg["file"])
            token_key = f"{pg['entity_set']}|{pg['page']}|{pg['delta_token']}"
            fname = pg["file"].rsplit("/", 1)[-1]
            if pg["error"]:
                status = "MALFORMED"
                extra_reject[pg["file"]] = f"file:{pg['error'].split(':')[0]}"
                keep.extend(pg["rows"])
            elif token_key in seen:
                status, dropped = "DUPLICATE_DELTA", dropped + len(pg["rows"])
            else:
                status = "LOADED"
                seen.add(token_key)
                keep.extend(pg["rows"])
                for fld in pg["fields"]:
                    if fld not in known.get(pg["entity_set"], []) and (pg["entity_set"], fld) not in changes:
                        changes.add((pg["entity_set"], fld))
                        sc_rows.append((entity, pg["entity_set"], fld, C.parse_date(day), "FIELD_ADDED",
                                        "ADDED_NULLABLE: kept in payload; silver maps it when the contract adds it",
                                        fname, now))
            control.append((C.batch_id(C.parse_date(day)), C.parse_date(day), src, entity, fname, pg["error"] is None,
                            len(pg["rows"]), None, status, token_key if status == "LOADED" else
                            f"{token_key} already loaded" if status == "DUPLICATE_DELTA" else pg["error"], now))
        if sc_rows or C.table_exists(spark, sc_tbl):
            C.replace_rows(spark, sc_tbl, SCHEMA_CHANGE_SCHEMA,
                         f"entity = '{entity}' AND first_seen_date IN ({C.date_list(dates)})", sc_rows)
        df = spark.createDataFrame(keep, "_source_file string, _source_row int, entity_set string, "
                                         "record_key string, delta_token string, payload string")
        parsed_cols = cols
    else:
        raise ValueError(f"unknown format {fmt}")

    rejects, warns = _checks(contract["columns"])
    if "_parse_error" in df.columns:
        rejects.append(F.when(F.col("_parse_error").isNotNull(), F.lit("payload:MALFORMED_JSON")))
    if extra_reject:
        m = F.create_map(*[x for k, v in extra_reject.items() for x in (F.lit(k), F.lit(v))])
        rejects.append(m[F.col("_source_file")])
    from pyspark.sql import Window

    warns.append(F.when(F.count(F.lit(1)).over(Window.partitionBy(F.col("_bd"), *contract["key"])) > 1,
                        F.lit("key:DUPLICATE_IN_BATCH")))
    bd = F.regexp_extract("_source_file", r"/(\d{4}-\d{2}-\d{2})/[^/]+$", 1)
    df = df.withColumn("_bd", bd)
    hash_src = F.col("payload") if "payload" in df.columns else F.concat_ws(
        "\u0001", *[F.coalesce(F.col(c), F.lit("\u0000")) for c in cols])
    record = F.col("payload") if "payload" in df.columns else F.to_json(F.struct(*[F.col(c) for c in cols]))
    checked = (df.withColumn("_reject_reasons", _compact(rejects)).withColumn("_warnings", _compact(warns))
               .withColumn("_record_hash", F.sha2(hash_src, 256)).withColumn("_record", record)).cache()
    meta = [F.concat(F.lit("B"), F.regexp_replace("_bd", "-", "")).alias("_batch_id"),
            F.to_date("_bd").alias("_business_date"), F.lit(src).alias("_source_system"),
            F.regexp_extract("_source_file", r"([^/]+)$", 1).alias("_source_file"), "_source_row",
            F.current_timestamp().cast("timestamp_ntz").alias("_ingested_at")]
    good = checked.where(F.size("_reject_reasons") == 0).select(*meta, *parsed_cols, "_warnings", "_record_hash")
    bad = checked.where(F.size("_reject_reasons") > 0).select(
        F.lit(entity).alias("_entity"), *meta, "_reject_reasons", "_warnings", "_record", "_record_hash") \
        .select(*QUARANTINE_COLS)
    rows_in = checked.count()
    n_bad = bad.count()
    n_good = rows_in - n_bad
    C.write_dates(spark, good, target, dates)
    q = names.t("bronze", "quarantine")
    C.ensure_table(spark, q, QUARANTINE_SCHEMA, ["_business_date"])
    spark.sql(f"DELETE FROM {q} WHERE _entity = '{entity}' AND _business_date IN ({C.date_list(dates)})")
    if n_bad:
        bad.writeTo(q).append()
    C.replace_rows(spark, names.t("ref", "file_control"), FILE_CONTROL_SCHEMA,
                 f"entity = '{entity}' AND business_date IN ({C.date_list(dates)})", control)
    reasons = (checked.where(F.size("_reject_reasons") > 0).select(F.explode("_reject_reasons").alias("r"))
               .groupBy("r").count().orderBy(F.desc("count")).limit(5).collect())
    msg = ", ".join(f"{r['r']}={r['count']}" for r in reasons) or "none"
    checked.unpersist()
    after = C.snapshot_id(spark, target)
    files_n = len(control)
    statuses = {}
    for c in control:
        statuses[c[8]] = statuses.get(c[8], 0) + 1
    detail = f"{len(paths)} dates, {files_n} files {statuses}; rejected: {msg}" + \
        (f"; re-sent pages dropped: {dropped} records" if dropped else "")
    audit.load(STAGE, entity, "COMMITTED", rows_in=rows_in, rows_out=n_good, rows_rejected=n_bad,
               snapshot_before=before, snapshot_after=after, started_at=started, message=detail)
    audit.transform(f"ingest {entity}", "ingest", f"landing:{src}/{{{dates[0]}..{dates[-1]}}}", target,
                    rows_in, n_good, f"format {fmt}")
    audit.transform(f"validate {entity}", "validate", target, q, rows_in, n_bad, msg)
    print(f"  {entity:<24} {rows_in:>9} in {n_good:>9} ok {n_bad:>6} quarantined  {detail}", flush=True)
    return {"rows_in": rows_in, "accepted": n_good, "rejected": n_bad}


# ---------------------------------------------------------------- objects


def _fingerprint(contracts: dict):
    def run(rows):
        from extract.objects import MIME, format_of, validate

        for r in rows:
            data = bytes(r.content)
            name = r.path.rsplit("/", 1)[-1]
            fmt = format_of(name)
            ok, reason, facts = validate(fmt, data, contracts.get(fmt or "", {})) if fmt else \
                (False, "UNKNOWN_FORMAT", {})
            yield (r.path, name, fmt, MIME.get(fmt or ""), len(data), hashlib.sha256(data).hexdigest(), ok, reason,
                   json.dumps(facts, default=str)[:4000])
    return run


def ingest_objects(spark, names, audit, landing: str, raw_root: str, dates: list[str], fs, threads: int) -> dict:
    started = C.now_utc()
    target = names.t("bronze", "doc_object")
    before = C.snapshot_id(spark, target)
    contracts = C.object_contracts()
    sources = sorted({s for c in contracts.values() for s in c.get("sources", [])})
    paths, manifest_sha = [], {}
    for src in sources:
        for folder in folders(fs, landing, src, dates):
            paths.append(folder)
            if fs.exists(f"{folder}/_manifest.json"):
                for f in json.loads(fs.read_text(f"{folder}/_manifest.json")).get("files", []):
                    manifest_sha[f"{folder}/{f['name']}"] = f["sha256"]
    if not paths:
        return {"rows_in": 0, "accepted": 0, "rejected": 0}
    bin_df = spark.read.format("binaryFile").option("pathGlobFilter", "*.{png,pdf,jdx,edr}").load(paths)
    facts = bin_df.select("path", "content").rdd.mapPartitions(_fingerprint(contracts)).collect()
    seen = {}
    if C.table_exists(spark, target):
        seen = {r[0]: r[1] for r in spark.table(target).where(
            (~F.col("_business_date").isin(dates)) & (F.col("ingest_status") == "VALID"))
            .select("sha256", "source_path").collect()}
    now = C.now_utc().replace(tzinfo=None)
    rows, q_rows, to_copy = [], [], []
    counts = {"VALID": 0, "DUPLICATE": 0, "QUARANTINED": 0}
    def norm(p: str) -> str:
        return re.sub(r"^file:/+", "/", p)

    manifest_sha = {norm(k): v for k, v in manifest_sha.items()}
    for path, name, fmt, mime, size, sha, ok, reason, fjson in sorted(facts):
        day = C.date_of_path(path)
        src = path.rsplit("/", 3)[-3]
        msha = manifest_sha.get(norm(path))
        if ok and msha and msha != sha:
            ok, reason = False, "SHA_MISMATCH"
        ext = name.rsplit(".", 1)[-1].lower()
        raw_path = f"{raw_root}/{fmt}/{sha}.{ext}"
        dup_of = seen.get(sha)
        if ok and dup_of is None:
            status = "VALID"
            seen[sha] = path
            to_copy.append((path, raw_path))
        elif ok:
            status = "DUPLICATE"
        else:
            status = "QUARANTINED"
            q_rows.append(("doc_object", C.batch_id(C.parse_date(day)), C.parse_date(day), src, name, 1,
                           [f"object:{reason}"], [], json.dumps({"sha256": sha, "size": size, "facts": fjson})[:4000],
                           sha, now))
        counts[status] += 1
        rows.append((sha, src, path, name, fmt, mime, size, sha, msha, raw_path if status != "QUARANTINED" else None,
                     status, reason, dup_of if status == "DUPLICATE" else None, fjson,
                     C.batch_id(C.parse_date(day)), C.parse_date(day), now))
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=threads) as pool:
        def copy(item):
            s, d = item
            if not fs.exists(d):
                fs.write_bytes(d, fs.read_bytes(s))
        list(pool.map(copy, to_copy))
    print(f"  raw store: {len(to_copy)} objects in {time.time() - t0:.0f}s", flush=True)
    df = spark.createDataFrame(rows, DOC_OBJECT_SCHEMA)
    C.write_dates(spark, df, target, dates)
    q = names.t("bronze", "quarantine")
    C.ensure_table(spark, q, QUARANTINE_SCHEMA, ["_business_date"])
    spark.sql(f"DELETE FROM {q} WHERE _entity = 'doc_object' AND _business_date IN ({C.date_list(dates)})")
    if q_rows:
        spark.createDataFrame(q_rows, QUARANTINE_SCHEMA).writeTo(q).append()
    after = C.snapshot_id(spark, target)
    msg = ", ".join(f"{k}={v}" for k, v in counts.items())
    reasons = {}
    for r in rows:
        if r[10] == "QUARANTINED":
            reasons[r[11]] = reasons.get(r[11], 0) + 1
    audit.load(STAGE, "doc_object", "COMMITTED", rows_in=len(rows), rows_out=counts["VALID"] + counts["DUPLICATE"],
               rows_rejected=counts["QUARANTINED"], snapshot_before=before, snapshot_after=after, started_at=started,
               message=f"manifest {len(manifest_sha)}; {msg}; quarantined {reasons or 'none'}")
    audit.transform("catalog objects", "ingest", f"landing:{{{','.join(sources)}}}", target, len(rows),
                    counts["VALID"] + counts["DUPLICATE"], f"raw store {raw_root}; {msg}")
    audit.transform("validate objects", "validate", target, q, len(rows), counts["QUARANTINED"],
                    "contracts/objects/*.json")
    audit.transform("deduplicate objects", "deduplicate", target, target, len(rows),
                    len(rows) - counts["DUPLICATE"], "by sha256 across batches")
    print(f"  {'doc_object':<24} {len(rows):>9} objects ({msg}; quarantined {reasons or 'none'})", flush=True)
    return {"rows_in": len(rows), "accepted": counts["VALID"] + counts["DUPLICATE"], "rejected": counts["QUARANTINED"]}


# ---------------------------------------------------------------- run


def run(spark, argv=None) -> dict:
    args = C.parse(parser(), argv)
    names = C.Names(args.db_prefix)
    C.ensure_databases(spark, names, ("bronze", "ref"))
    dates = [d.isoformat() for d in C.date_range(args.date_from, args.date_to)]
    landing, raw_root = C.as_uri(args.landing), C.as_uri(args.raw)
    fs = C.filesystem(spark, landing)
    only = {e for e in args.entities.split(",") if e}
    audit = C.Audit(spark, names, "ingest_bronze", args.date_to, args.pipeline_run)
    audit.load(STAGE, "*", "STARTED", message=f"landing {landing}; dates {dates[0]}..{dates[-1]}")
    summary, state, t0 = {}, {}, time.time()
    print(f"bronze {dates[0]}..{dates[-1]} ({len(dates)} dates) from {landing}", flush=True)
    try:
        for entity, contract in C.contracts().items():
            if only and entity not in only:
                continue
            summary[entity] = ingest_records(spark, names, audit, contract, landing, dates, fs, state)
        if not only or "doc_object" in only:
            summary["doc_object"] = ingest_objects(spark, names, audit, landing, raw_root, dates, fs, args.threads)
        audit.load(STAGE, "*", "COMPLETED", rows_in=sum(s["rows_in"] for s in summary.values()),
                   rows_out=sum(s["accepted"] for s in summary.values()),
                   rows_rejected=sum(s["rejected"] for s in summary.values()),
                   message=f"{len(dates)} dates in {time.time() - t0:.0f}s")
    except Exception as e:
        audit.load(STAGE, "*", "FAILED", message=C.error_summary(e))
        raise
    finally:
        audit.flush()
    print(f"BRONZE_RESULT dates={len(dates)} seconds={time.time() - t0:.0f}", flush=True)
    for k, v in summary.items():
        print(f"BRONZE_ENTITY {k} {v['rows_in']} {v['accepted']} {v['rejected']}", flush=True)
    return summary


def main() -> int:
    spark = C.get_spark("evx-ingest-bronze")
    C.ship_packages(spark)
    run(spark, sys.argv[1:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
