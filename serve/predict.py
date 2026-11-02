"""
CAI model evx-uc01-service. predict() scores one vehicle from the factory features.

The champion joblib is models/champion/model.joblib (uploaded; not in git). The same
guardrails as the lakehouse score apply: feature-hash lock, open genealogy abstains,
out-of-distribution abstains, the abstain band, and a thermal event passed in as
safety_floor is P1. action is always NONE.

Request: {"vin", "pack_serial", "safety_floor": 0 or 1, "features": {<factory column>: value}}
"""
from __future__ import annotations

import os
import sys
import traceback
from pathlib import Path

for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"):
    os.environ.setdefault(_v, "1")


def _repo_root() -> Path:
    try:
        return Path(__file__).resolve().parents[1]
    except NameError:
        return Path(os.getcwd())


ROOT = _repo_root()
sys.path.insert(0, str(ROOT))

try:
    import cml.models_v1 as _cml_models

    cml_model = _cml_models.cml_model
except ImportError:
    def cml_model(fn):
        return fn


CHAMPION = ROOT / os.environ.get("EVX_CHAMPION_DIR", "models/champion") / "model.joblib"
_ARTIFACT, _LOAD_ERROR = None, None
try:
    import joblib

    _ARTIFACT = joblib.load(CHAMPION)
except Exception as e:  # noqa: BLE001  a load failure must not kill the replica
    _LOAD_ERROR = {"error": f"model not loaded: {type(e).__name__}: {e}", "action": "NONE"}


def score_request(artifact: dict, payload: dict) -> dict:
    """One vehicle. Raises nothing for a lock failure: the caller gets an error object."""
    import pandas as pd

    from usecases.uc01_pack_risk.build_features import FEATURE_COLUMNS, FEATURE_SET
    from usecases.uc01_pack_risk.score import ACTION, ScoreRefused, score_frame

    feats = payload.get("features") if isinstance(payload.get("features"), dict) else payload
    missing = [c for c in FEATURE_COLUMNS if c not in feats]
    if missing:
        return {"error": f"missing factory columns: {missing[:4]}", "action": ACTION}
    row = {c: feats[c] for c in FEATURE_COLUMNS}
    row.update(vin=str(payload.get("vin", "request")), pack_serial=str(payload.get("pack_serial", "request")),
               safety_floor=int(payload.get("safety_floor") or 0), feature_set=FEATURE_SET,
               feature_hash="a" * 64, contract_ok=True)
    try:
        scored = score_frame(pd.DataFrame([row]), artifact, artifact["hash_salt"])
    except ScoreRefused as e:
        return {"error": str(e), "action": ACTION}
    r = scored.iloc[0]
    pri = r["priority"]
    priority = None if pri is None or pri != pri else pri
    return {"vin": r["vin"], "pack_serial": r["pack_serial"], "score": float(r["score"]),
            "decision": r["decision"], "reason": r["reason"], "priority": priority,
            "ood": bool(r["ood"]), "action": ACTION, "schema_hash": r["schema_hash"]}


@cml_model
def predict(args: dict) -> dict:
    if _LOAD_ERROR:
        return _LOAD_ERROR
    try:
        if isinstance(args, str):
            import json
            args = json.loads(args)
        return score_request(_ARTIFACT, args)
    except Exception as e:  # noqa: BLE001
        return {"error": f"{type(e).__name__}: {e}", "action": "NONE", "trace": traceback.format_exc()[-800:]}
