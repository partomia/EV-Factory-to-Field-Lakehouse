"""
Score pack_risk_eol and apply the M1 guardrails.

The model file (outputs/uc01/pack_risk_eol/model.joblib) stays on the machine that trained it.
Scoring reads the factory features, writes a parquet, and a Spark job publishes that parquet
(cde/jobs/publish_uc01_scores.py) into gold.uc01_pack_risk_score, gold.model_score and
ref.guardrail_event. Nothing here reads generator truth.

  .venv/bin/python -m usecases.uc01_pack_risk.score --source impala

A run whose feature salt is not "platform" prints SCORE_RESULT SKIPPED. A platform run refuses,
before any score is written, when the model's schema hash or salt does not match the factory
columns (mlcore.hashlock). Decisions, in order: a thermal DTC or thermal EDR known on or before
the score date is P1 (the safety floor; this is the vehicle's own batch-history event, not the
empty end-of-line sibling flag); a row that fails the feature contract abstains; an open
genealogy abstains; an out-of-distribution row abstains; the band between the two out-of-fold
thresholds abstains; at or above the worklist threshold is the worklist; below it is clear.
action is always NONE.
"""
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "cde" / "jobs"))

import evx_common as C  # noqa: E402
from mlcore.hashlock import schema_hash  # noqa: E402
from usecases.uc01_pack_risk.build_features import (  # noqa: E402
    CATEGORICAL, FEATURE_COLUMNS, FEATURE_SET, feature_hash,
)

USE_CASE = "uc01"
HEAD = "pack_risk_eol"
ACTION = "NONE"
SCORE_TABLE = "uc01_pack_risk_score"
MODEL_SCORE = "model_score"
DECISIONS = ("SAFETY_FLOOR", "ABSTAIN", "WORKLIST", "CLEAR")


class ScoreRefused(RuntimeError):
    """The feature-hash lock failed. No score table is written."""


def assert_locked(artifact: dict, salt: str, columns: list[str]) -> str:
    """Refuse a model trained on another salt, another column list, or never gated deployable."""
    locked = schema_hash(salt, columns)
    if list(artifact.get("feature_columns") or []) != list(columns):
        raise ScoreRefused("feature columns differ from the model")
    if artifact.get("hash_salt") != salt:
        raise ScoreRefused(f"model salt {artifact.get('hash_salt')} != {salt}")
    if artifact.get("schema_hash") != locked:
        raise ScoreRefused("schema hash does not match the salt and the factory columns")
    if salt != "platform" or not artifact.get("deployable"):
        raise ScoreRefused("model is not deployable")
    return locked


def decide_one(score: float, ood: bool, genealogy_open: bool, safety_floor: bool,
               contract_ok: bool, thresholds: dict) -> tuple[str, str, str | None]:
    """One vehicle. Safety floor first, then rows that cannot be ranked, then the band."""
    if safety_floor:
        return "SAFETY_FLOOR", "safety_floor", "P1"
    if not contract_ok:
        return "ABSTAIN", "contract", None
    if genealogy_open:
        return "ABSTAIN", "genealogy", None
    if ood:
        return "ABSTAIN", "ood", None
    if score >= thresholds["worklist"]:
        return "WORKLIST", "worklist", None
    if score >= thresholds["abstain"]:
        return "ABSTAIN", "abstain_band", None
    return "CLEAR", "clear", None


def guardrail_kinds(row: dict) -> list[tuple[str, str]]:
    """Guardrails that fired for one scored row. A higher-precedence decision still logs the rest."""
    kinds = []
    if row["safety_floor"]:
        kinds.append(("safety_floor", "P1"))
    if not row["contract_ok"]:
        kinds.append(("contract", "ABSTAIN"))
    if row["genealogy_open"]:
        kinds.append(("genealogy", "ABSTAIN"))
    if row["ood"] and row["contract_ok"] and not row["genealogy_open"] and not row["safety_floor"]:
        kinds.append(("ood", "ABSTAIN"))
    if row["reason"] == "abstain_band":
        kinds.append(("abstain_band", "ABSTAIN"))
    return kinds


def _probabilities(df, artifact: dict):
    import numpy as np

    from usecases.uc01_pack_risk.train import FORMATION, _base_matrix

    base = _base_matrix(df, artifact["maps"], artifact["medians"])
    form_idx = [FEATURE_COLUMNS.index(c) for c in FORMATION]
    anomaly = -artifact["forest"].decision_function(base[:, form_idx])
    x = np.column_stack([base, anomaly])
    if artifact.get("scaler") is not None:
        x = artifact["scaler"].transform(x)
    raw = artifact["model"].predict_proba(x)[:, 1]
    cal = artifact["isotonic"].predict(raw)
    med = np.asarray(artifact["ood"]["median"], dtype=float)
    mad = np.asarray(artifact["ood"]["mad"], dtype=float)
    dist = np.mean(np.abs(x - med) / mad, axis=1)
    return raw, cal, dist


def score_frame(df, artifact: dict, salt: str):
    """Scored copy of df. Raises ScoreRefused when the model does not lock to this salt."""
    import pandas as pd

    locked = assert_locked(artifact, salt, FEATURE_COLUMNS)
    thresholds = artifact["thresholds"]
    _raw, cal, dist = _probabilities(df, artifact)
    limit = float(artifact["ood"]["limit"])
    out = df[["vin", "pack_serial"]].copy()
    out["genealogy_open"] = pd.to_numeric(df["genealogy_open"]).fillna(1).astype(int).to_numpy()
    out["safety_floor"] = pd.to_numeric(df.get("safety_floor", 0)).fillna(0).astype(int).to_numpy()
    if "contract_ok" in df.columns:
        out["contract_ok"] = df["contract_ok"].astype(bool).to_numpy()
    else:
        present = df["feature_hash"].astype(str).str.strip().str.lower().str.fullmatch(r"[0-9a-f]{64}").fillna(False)
        out["contract_ok"] = ((df["feature_set"] == FEATURE_SET) & present).to_numpy()
    out["score"] = cal
    out["ood_distance"] = dist
    out["ood"] = dist > limit
    decided = [decide_one(float(s), bool(o), bool(g), bool(f), bool(c), thresholds)
               for s, o, g, f, c in zip(out["score"], out["ood"], out["genealogy_open"],
                                        out["safety_floor"], out["contract_ok"])]
    out["decision"] = [d[0] for d in decided]
    out["reason"] = [d[1] for d in decided]
    out["priority"] = pd.Series([d[2] for d in decided], index=out.index, dtype=object)
    out["action"] = ACTION
    out["schema_hash"] = locked
    out["feature_set"] = FEATURE_SET
    out["use_case"] = USE_CASE
    out["head"] = HEAD
    if set(out["action"]) != {ACTION} or not set(out["decision"]) <= set(DECISIONS):
        raise ScoreRefused("a decision carried an action or an unknown band")
    return out


def _floor_vins(conn, gold: str, score_date: str) -> set[str]:
    """Thermal DTC or thermal EDR on or before the score date. The vehicle's own history."""
    from lakehouse.impala import query

    sql = f"""
        SELECT vin FROM {gold}.fact_dtc
        WHERE safety_floor = true AND to_date(event_ts) <= to_date('{score_date}')
        UNION
        SELECT vin FROM {gold}.fact_edr_event
        WHERE safety_floor = true AND to_date(trigger_ts) <= to_date('{score_date}')
    """
    _cols, rows = query(conn, sql)
    return {r[0] for r in rows}


def _load_impala(prefix: str, score_date: str):
    import pandas as pd

    from lakehouse.impala import connect, query

    listed = ", ".join(f"`{c}`" for c in ["vin", "pack_serial", "as_of", "feature_hash", "feature_set", *FEATURE_COLUMNS])
    sql = (f"SELECT {listed} FROM {prefix}_features.{FEATURE_SET} "
           f"WHERE as_of <= to_date('{score_date}')")
    conn = connect()
    for table in (f"{prefix}_features.{FEATURE_SET}", f"{prefix}_gold.fact_dtc", f"{prefix}_gold.fact_edr_event"):
        query(conn, f"INVALIDATE METADATA {table}")
    names, rows = query(conn, sql)
    frame = pd.DataFrame(rows, columns=names)
    floor = _floor_vins(conn, f"{prefix}_gold", score_date)
    frame["safety_floor"] = frame["vin"].isin(floor).astype(int)
    return frame


def _summarise(scored) -> str:
    n = len(scored)
    counts = scored["decision"].value_counts().to_dict()
    parts = [f"rows={n}"]
    for name in DECISIONS:
        parts.append(f"{name.lower()}={int(counts.get(name, 0))}")
    parts.append(f"schema={scored['schema_hash'].iloc[0][:12]}" if n else "schema=none")
    return " ".join(parts)


def run(source: str, overlay: str | None, score_date: str | None, model_path: str | None, out: str | None) -> dict:
    import joblib

    cfg = C.config(overlay)
    salt = cfg.get("features", {}).get("hash_salt", "platform")
    if salt != "platform":
        print(f"SCORE_RESULT SKIPPED salt={salt} (a non-platform model cannot score)", flush=True)
        return {"skipped": True}
    day = score_date or cfg["calendar"]["history_end"]
    path = Path(model_path or (ROOT / "outputs" / "uc01" / "pack_risk_eol" / "model.joblib"))
    artifact = joblib.load(path)
    if source == "impala":
        frame = _load_impala(cfg["db_prefix"], day)
    else:
        raise ScoreRefused("the platform score reads Impala; Spark scoring is the publish job")
    scored = score_frame(frame, artifact, salt)
    scored.insert(0, "score_date", day)
    dest = Path(out or (ROOT / "outputs" / "uc01" / "pack_risk_eol" / "scores.parquet"))
    dest.parent.mkdir(parents=True, exist_ok=True)
    scored.to_parquet(dest, index=False)
    print(f"SCORE_RESULT {_summarise(scored)}", flush=True)
    print(f"SCORE_PARQUET {dest}", flush=True)
    return {"rows": len(scored), "path": str(dest), "score_date": day}


def _replace_where(spark, table: str, df, where: str) -> None:
    if C.table_exists(spark, table):
        C.add_new_columns(spark, df, table)
        spark.sql(f"DELETE FROM {table} WHERE {where}")
        df.writeTo(table).append()
    else:
        df.writeTo(table).using("iceberg").tableProperty("format-version", "2").create()


def _event_id(score_date: str, subject: str, kind: str) -> str:
    return hashlib.sha256(f"{USE_CASE}|{HEAD}|{score_date}|{subject}|{kind}".encode()).hexdigest()[:32]


def publish_scores(spark, parquet: str, overlay: str | None = None) -> dict:
    """Write a scored parquet into the lakehouse. Verifies the schema hash and each row hash first."""
    from pyspark.sql import functions as F

    import publish
    from publish import Spec

    cfg = C.config(overlay)
    salt = cfg.get("features", {}).get("hash_salt", "platform")
    names = C.Names(cfg["db_prefix"])
    C.ensure_databases(spark, names, ("gold", "ref", "features"))
    src = spark.read.parquet(parquet)
    day = src.select("score_date").limit(1).collect()[0][0]
    day = str(day)[:10]
    locked = schema_hash(salt, FEATURE_COLUMNS)
    got = src.select("schema_hash").limit(1).collect()[0][0]
    if salt != "platform" or got != locked:
        raise ScoreRefused(f"parquet schema {got} does not lock to salt {salt}")
    feats = spark.table(names.t("features", FEATURE_SET))
    rebuilt = feature_hash(feats.drop("feature_hash", "feature_set"), salt)
    mismatch = feats.join(rebuilt.select("vin", F.col("feature_hash").alias("_rehash")), "vin") \
        .where(F.col("feature_hash") != F.col("_rehash")).count()
    if mismatch:
        raise ScoreRefused(f"{mismatch} feature rows no longer hash with this salt and these columns")
    if src.where(F.col("action") != ACTION).count():
        raise ScoreRefused("a scored row carried an action")
    scored = src.withColumn("_business_date", F.to_date(F.lit(day))) \
        .withColumn("score_date", F.to_date(F.lit(day)))
    audit = C.Audit(spark, names, "uc01_score", C.parse_date(day), None)
    where = f"use_case = '{USE_CASE}' AND head = '{HEAD}' AND score_date = DATE '{day}'"
    try:
        publish.publish(spark, scored, Spec(
            SCORE_TABLE, ["vin"], mode="dates", transform="score",
            checks={
                "no_action": f"action = '{ACTION}'",
                "decision_known": "decision IN ('SAFETY_FLOOR','ABSTAIN','WORKLIST','CLEAR')",
                "floor_is_p1": "safety_floor = 0 OR (decision = 'SAFETY_FLOOR' AND priority = 'P1')",
                "open_genealogy_abstains": "genealogy_open = 0 OR safety_floor = 1 OR decision = 'ABSTAIN'",
            }), names, audit, [day], [names.t("features", FEATURE_SET)])
        shared = scored.select("_business_date", "score_date", "use_case", "head", "vin", "pack_serial",
                               "score", "decision", "priority", "action")
        _replace_where(spark, names.t("gold", MODEL_SCORE), shared, where)
        events = []
        for row in scored.select("vin", "safety_floor", "contract_ok", "genealogy_open", "ood", "reason").toLocalIterator():
            item = {"safety_floor": int(row.safety_floor), "contract_ok": bool(row.contract_ok),
                    "genealogy_open": int(row.genealogy_open), "ood": bool(row.ood), "reason": row.reason}
            for kind, outcome in guardrail_kinds(item):
                events.append((_event_id(day, row.vin, kind), USE_CASE, HEAD, day, "vehicle", row.vin,
                               kind, outcome, ACTION))
        events.append((_event_id(day, HEAD, "feature_hash"), USE_CASE, HEAD, day, "model", locked,
                       "feature_hash", "PASS", ACTION))
        ev = spark.createDataFrame(events, "event_id string, use_case string, head string, score_date string, "
                                    "subject_type string, subject_id string, guardrail string, outcome string, action string") \
            .withColumn("score_date", F.to_date("score_date")) \
            .withColumn("recorded_at", F.lit(C.now_utc().replace(tzinfo=None)).cast("timestamp"))
        _replace_where(spark, names.t("ref", "guardrail_event"), ev, where)
        audit.load("gold", SCORE_TABLE, "COMPLETED", rows_out=scored.count(), message=f"schema {locked[:12]}")
    except Exception as e:
        audit.load("gold", SCORE_TABLE, "FAILED", message=C.error_summary(e))
        raise
    finally:
        audit.flush()
    n = scored.count()
    print(f"SCORE_PUBLISHED rows={n} score_date={day} schema={locked[:12]}", flush=True)
    return {"rows": n, "score_date": day}


def main(argv=None) -> int:
    p = C.base_parser(__doc__.split("\n\n")[0], dated=False)
    p.add_argument("--source", choices=("impala", "spark"), default="impala")
    p.add_argument("--score-date", default=None)
    p.add_argument("--model", default=None)
    p.add_argument("--out", default=None)
    args = C.parse(p, argv)
    try:
        run(args.source, args.overlay, args.score_date, args.model, args.out)
    except ScoreRefused as e:
        print(f"SCORE_REFUSED {e}", flush=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
