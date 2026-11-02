"""
Train the pack_risk_eol champion.

The population is matured vehicles with complete genealogy. Each pack follows its majority cell
batch. TEST is the latest fifth of those batches by last in-service date; the five TRAIN folds
are grouped by batch. Three candidates (LightGBM, logistic regression, histogram gradient
boosting) are scored by pooled out-of-fold PR-AUC. An isolation forest on the formation
summaries is fit inside each fold, on the other folds only. Isotonic calibration and the
worklist and abstain thresholds are fit on the winner's out-of-fold scores, never on TEST.

  .venv/bin/python -m usecases.uc01_pack_risk.train --source impala
  .venv/bin/python -m usecases.uc01_pack_risk.train --source spark

A CI-salt run with too few matured vehicles prints GATE_RESULT SKIPPED and does not write a
deployable model. The saved model carries the feature schema hash (mlcore.hashlock).
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "cde" / "jobs"))

import evx_common as C  # noqa: E402
from mlcore.gate import decide  # noqa: E402
from mlcore.hashlock import schema_hash  # noqa: E402
from mlcore.metrics import auroc, ece, lift, pr_auc  # noqa: E402
from mlcore.split import batch_split, mixed_share  # noqa: E402
from usecases.uc01_pack_risk.build_features import CATEGORICAL, FEATURE_COLUMNS, FEATURE_SET  # noqa: E402

FORMATION = ["ce_mean", "ce_std", "formation_z_max"]
LIMITS = {
    "min_positives_per_split": 100,
    "min_positives_per_fold": 100,
    "min_oof_auroc": 0.65,
    "min_pr_auc_lift": 1.5,
    "max_ece": 0.08,
    "max_pr_auc_drop": 0.02,
    "mixed_pack_max": 0.15,
}
MODEL_DEFAULTS = {"test_fraction": 0.2, "folds": 5, "worklist_fraction": 0.10,
                  "abstain_fraction": 0.20, "n_estimators": 200, "gate": LIMITS}


def settings(overlay: str | None) -> dict:
    from generators.core import deep_merge

    cfg = deep_merge(MODEL_DEFAULTS, C.config(overlay).get("model", {}))
    cfg["seed"] = int(C.config(overlay)["seed"])
    cfg["salt"] = C.config(overlay).get("features", {}).get("hash_salt", "platform")
    cfg["gate"] = deep_merge(LIMITS, cfg.get("gate", {}))
    return cfg


def _sql(prefix: str) -> str:
    keys = ["vin", "pack_serial", "in_service_date", "matured", "label"]
    cols = keys + [c for c in FEATURE_COLUMNS if c not in keys]
    listed = ", ".join(f"e.`{c}`" for c in cols)
    return (f"SELECT {listed}, p.batch_id, p.n_batches FROM {prefix}_gold.uc01_pack_eol_example e "
            f"JOIN (SELECT pack_serial, max(majority_cell_batch) batch_id, max(n_cell_batches) n_batches "
            f"FROM {prefix}_gold.fact_pack_eol GROUP BY pack_serial) p ON p.pack_serial = e.pack_serial")


def load_frame(source: str, prefix: str):
    import pandas as pd

    sql = _sql(prefix)
    if source == "impala":
        from lakehouse.impala import connect, query

        conn = connect()
        for table in (f"{prefix}_gold.uc01_pack_eol_example", f"{prefix}_gold.fact_pack_eol"):
            query(conn, f"INVALIDATE METADATA {table}")
        names, rows = query(conn, sql)
        return pd.DataFrame(rows, columns=names)
    from run_local import DATA, local_spark

    spark = local_spark(DATA / "warehouse")
    return spark.sql(sql).toPandas()


def _day(value) -> str:
    return str(value)[:10]


def population(df, cfg: dict):
    """Matured, fully graded packs, with a batch assignment."""
    import pandas as pd

    frame = df.copy()
    frame["matured"] = pd.to_numeric(frame["matured"]).fillna(0).astype(int)
    frame["label"] = pd.to_numeric(frame["label"]).fillna(0).astype(int)
    frame["genealogy_open"] = pd.to_numeric(frame["genealogy_open"]).fillna(1).astype(int)
    frame["n_batches"] = pd.to_numeric(frame["n_batches"]).fillna(1).astype(int)
    frame["day"] = frame["in_service_date"].map(_day)
    matured = frame[frame["matured"] == 1]
    fit = matured[matured["genealogy_open"] == 0].copy()
    missing = fit["batch_id"].isna() | (fit["batch_id"].astype(str) == "")
    fit.loc[missing, "batch_id"] = "PACK:" + fit.loc[missing, "pack_serial"].astype(str)
    fit["batch_id"] = fit["batch_id"].astype(str)
    last = fit.groupby("batch_id")["day"].max().to_dict()
    assigned = batch_split(last, cfg["test_fraction"], cfg["folds"])
    fit["split"] = fit["batch_id"].map(lambda b: assigned[b][0])
    fit["fold"] = fit["batch_id"].map(lambda b: assigned[b][1])
    return fit


def _maps(df) -> dict:
    return {c: {v: i for i, v in enumerate(sorted(set(df[c].dropna().astype(str))))} for c in CATEGORICAL}


def _medians(df) -> dict:
    import pandas as pd

    out = {}
    for c in FEATURE_COLUMNS:
        if c in CATEGORICAL:
            continue
        v = pd.to_numeric(df[c], errors="coerce")
        out[c] = float(np.nanmedian(v)) if v.notna().any() else 0.0
    return out


def _base_matrix(df, maps: dict, medians: dict) -> np.ndarray:
    import pandas as pd

    cols = []
    for c in FEATURE_COLUMNS:
        if c in CATEGORICAL:
            cols.append(np.array([maps[c].get(str(v), -1) if pd.notna(v) else -1 for v in df[c]], dtype=float))
        else:
            x = pd.to_numeric(df[c], errors="coerce").to_numpy(dtype=float)
            cols.append(np.where(np.isnan(x), medians[c], x))
    return np.column_stack(cols)


def _with_forest(base: np.ndarray, form_idx: list[int], train: np.ndarray, score: np.ndarray, seed: int):
    from sklearn.ensemble import IsolationForest

    forest = IsolationForest(n_estimators=100, contamination="auto", random_state=seed, n_jobs=1)
    forest.fit(base[train][:, form_idx])
    def anomaly(idx):
        return -forest.decision_function(base[idx][:, form_idx])
    return (np.column_stack([base[train], anomaly(train)]),
            np.column_stack([base[score], anomaly(score)]), forest)


def _candidate(kind: str, seed: int, y_train: np.ndarray, n_estimators: int):
    pos = max(1, int(y_train.sum()))
    weight = (len(y_train) - pos) / pos
    if kind == "lightgbm":
        from lightgbm import LGBMClassifier

        return LGBMClassifier(n_estimators=n_estimators, learning_rate=0.05, num_leaves=31,
                              min_child_samples=20, subsample=0.8, colsample_bytree=0.8,
                              scale_pos_weight=weight, random_state=seed, n_jobs=1, verbosity=-1)
    if kind == "logistic":
        from sklearn.linear_model import LogisticRegression

        return LogisticRegression(max_iter=500, class_weight="balanced", random_state=seed)
    from sklearn.ensemble import HistGradientBoostingClassifier

    return HistGradientBoostingClassifier(max_iter=n_estimators, learning_rate=0.05,
                                          class_weight="balanced", random_state=seed, early_stopping=False)


def _scaled(kind, x_train, x_score):
    if kind != "logistic":
        return x_train, x_score, None
    from sklearn.preprocessing import StandardScaler

    scaler = StandardScaler()
    return scaler.fit_transform(x_train), scaler.transform(x_score), scaler


def oof_scores(base: np.ndarray, y: np.ndarray, folds: np.ndarray, form_idx: list[int], cfg: dict) -> dict[str, np.ndarray]:
    kinds = ("lightgbm", "logistic", "hist_gradient_boosting")
    out = {k: np.zeros(len(y)) for k in kinds}
    for fold in sorted(set(folds.tolist())):
        train, hold = np.flatnonzero(folds != fold), np.flatnonzero(folds == fold)
        x_train, x_hold, _ = _with_forest(base, form_idx, train, hold, cfg["seed"])
        for kind in kinds:
            model = _candidate(kind, cfg["seed"], y[train], cfg["n_estimators"])
            a, b, _ = _scaled(kind, x_train, x_hold)
            model.fit(a, y[train])
            out[kind][hold] = model.predict_proba(b)[:, 1]
    return out


def ood_fit(x: np.ndarray) -> dict:
    med = np.median(x, axis=0)
    mad = np.median(np.abs(x - med), axis=0)
    mad = np.where(mad < 1e-6, 1.0, mad)
    dist = np.mean(np.abs(x - med) / mad, axis=1)
    return {"median": med.tolist(), "mad": mad.tolist(), "limit": float(np.quantile(dist, 0.99))}


def fit_champion(df, cfg: dict) -> dict:
    from sklearn.isotonic import IsotonicRegression

    fit = population(df, cfg)
    train = fit[fit["split"] == "train"]
    y = fit["label"].to_numpy(int)
    y_train = train["label"].to_numpy(int)
    maps, medians = _maps(train), _medians(train)
    base = _base_matrix(fit, maps, medians)
    form_idx = [FEATURE_COLUMNS.index(c) for c in FORMATION]
    train_mask = (fit["split"] == "train").to_numpy()
    folds = fit["fold"].to_numpy()
    scores = oof_scores(base[train_mask], y[train_mask], folds[train_mask].astype(int), form_idx, cfg)
    prevalence = float(y[train_mask].mean())
    ranking = {k: pr_auc(y[train_mask], v) for k, v in scores.items()}
    winner = max(ranking, key=ranking.get)
    raw = scores[winner]
    held = np.zeros(len(raw))
    train_folds = folds[train_mask].astype(int)
    for fold in sorted(set(train_folds.tolist())):
        inside, hold = train_folds != fold, train_folds == fold
        held[hold] = IsotonicRegression(out_of_bounds="clip").fit(raw[inside], y[train_mask][inside]).predict(raw[hold])
    iso = IsotonicRegression(out_of_bounds="clip").fit(raw, y[train_mask])
    calibrated = iso.predict(raw)
    hi = float(np.quantile(calibrated, 1 - cfg["worklist_fraction"]))
    lo = float(np.quantile(calibrated, 1 - cfg["worklist_fraction"] - cfg["abstain_fraction"]))
    x_train, _, forest = _with_forest(base, form_idx, np.flatnonzero(train_mask), np.flatnonzero(train_mask), cfg["seed"])
    model = _candidate(winner, cfg["seed"], y_train, cfg["n_estimators"])
    x_fit, _, scaler = _scaled(winner, x_train, x_train)
    model.fit(x_fit, y_train)
    fold_pos = [int(y[train_mask][folds[train_mask] == f].sum()) for f in range(cfg["folds"])]
    test_mask = (fit["split"] == "test").to_numpy()
    metrics = {
        "winner": winner,
        "candidates": {k: {"oof_pr_auc": v, "oof_auroc": auroc(y[train_mask], scores[k])} for k, v in ranking.items()},
        "oof_pr_auc": ranking[winner],
        "oof_auroc": auroc(y[train_mask], raw),
        "oof_pr_lift": lift(ranking[winner], prevalence),
        "oof_ece": ece(y[train_mask], held),
        "prevalence": prevalence,
        "train_rows": int(train_mask.sum()),
        "test_rows": int(test_mask.sum()),
        "train_positives": int(y[train_mask].sum()),
        "test_positives": int(y[test_mask].sum()),
        "fold_positives_min": min(fold_pos) if fold_pos else 0,
        "fold_positives": fold_pos,
        "mixed_share": mixed_share(fit["n_batches"].tolist()),
        "worklist_threshold": hi,
        "abstain_threshold": lo,
        "batches_train": int(train["batch_id"].nunique()),
        "batches_test": int(fit.loc[fit["split"] == "test", "batch_id"].nunique()),
    }
    artifact = {
        "winner": winner, "model": model, "forest": forest, "scaler": scaler, "isotonic": iso,
        "maps": maps, "medians": medians, "ood": ood_fit(x_fit), "thresholds": {"worklist": hi, "abstain": lo},
        "feature_columns": FEATURE_COLUMNS, "formation": FORMATION, "feature_set": FEATURE_SET,
        "hash_salt": cfg["salt"], "schema_hash": schema_hash(cfg["salt"], FEATURE_COLUMNS),
        "deployable": False, "metrics": metrics,
    }
    return {"metrics": metrics, "artifact": artifact, "frame": fit}


def save_model(artifact: dict, path: Path) -> None:
    import joblib

    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(artifact, path)


def run(source: str, overlay: str | None, write_baseline: bool) -> int:
    cfg = settings(overlay)
    prefix = C.config(overlay)["db_prefix"]
    df = load_frame(source, prefix)
    matured = int((np.asarray(df["matured"].astype(float)) == 1).sum())
    floor = cfg["gate"]["min_positives_per_split"]
    if matured < floor and cfg["salt"] != "platform":
        print(f"GATE_RESULT SKIPPED matured={matured} salt={cfg['salt']} (below {floor}; not deployable)", flush=True)
        return 0
    fitted = fit_champion(df, cfg)
    metrics, artifact = fitted["metrics"], fitted["artifact"]
    baseline_path = ROOT / "usecases" / "uc01_pack_risk" / "baseline.json"
    baseline = json.loads(baseline_path.read_text()) if baseline_path.exists() else None
    errors = decide(metrics, cfg["gate"], baseline)
    artifact["deployable"] = cfg["salt"] == "platform" and not errors
    out = ROOT / "outputs" / "uc01" / "pack_risk_eol"
    save_model(artifact, out / "model.joblib")
    fitted["frame"][["vin", "pack_serial", "batch_id", "split", "fold", "label"]].to_csv(out / "split.csv", index=False)
    (out / "metrics.json").write_text(json.dumps(metrics, indent=2) + "\n")
    print("TRAIN_RESULT " + " ".join(f"{k}={metrics[k]}" for k in
          ("winner", "oof_pr_auc", "oof_auroc", "oof_pr_lift", "oof_ece", "train_positives",
           "test_positives", "fold_positives_min", "mixed_share")), flush=True)
    for name, row in metrics["candidates"].items():
        print(f"TRAIN_CANDIDATE {name} pr_auc={row['oof_pr_auc']:.4f} auroc={row['oof_auroc']:.4f}", flush=True)
    print(f"GATE_RESULT {'PASS' if not errors else 'FAIL'} deployable={artifact['deployable']} "
          f"schema={artifact['schema_hash'][:12]}", flush=True)
    for e in errors:
        print(f"GATE_FAIL {e}", flush=True)
    if write_baseline and artifact["deployable"]:
        kept = {k: metrics[k] for k in ("winner", "oof_pr_auc", "oof_auroc", "oof_pr_lift", "oof_ece")}
        kept.update(schema_hash=artifact["schema_hash"], hash_salt=cfg["salt"], feature_set=FEATURE_SET)
        baseline_path.write_text(json.dumps(kept, indent=2) + "\n")
        print(f"BASELINE_WRITTEN {baseline_path}", flush=True)
    return 1 if errors else 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--source", choices=("impala", "spark"), default="impala")
    p.add_argument("--overlay", default=None)
    p.add_argument("--write-baseline", action="store_true")
    args = p.parse_args(argv)
    return run(args.source, args.overlay, args.write_baseline)


if __name__ == "__main__":
    sys.exit(main())
