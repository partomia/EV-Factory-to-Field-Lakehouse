"""The serving function applies the same guardrails as the lakehouse score, and never an action."""
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from mlcore.hashlock import schema_hash  # noqa: E402
from serve.predict import score_request  # noqa: E402
from usecases.uc01_pack_risk.build_features import CATEGORICAL, FEATURE_COLUMNS  # noqa: E402


def _artifact():
    class Model:
        def predict_proba(self, x):
            p = np.array([0.99])
            return np.column_stack([1 - p, p])

    class Forest:
        def decision_function(self, x):
            return np.zeros(len(x))

    class Iso:
        def predict(self, raw):
            return raw

    width = len(FEATURE_COLUMNS) + 1
    return {
        "model": Model(), "forest": Forest(), "scaler": None, "isotonic": Iso(),
        "maps": {c: {"A": 0} for c in CATEGORICAL},
        "medians": {c: 0.0 for c in FEATURE_COLUMNS if c not in CATEGORICAL},
        "ood": {"median": [0.0] * width, "mad": [1.0] * width, "limit": 5.0},
        "thresholds": {"worklist": 0.5, "abstain": 0.2},
        "feature_columns": list(FEATURE_COLUMNS), "hash_salt": "platform",
        "schema_hash": schema_hash("platform", FEATURE_COLUMNS), "deployable": True,
    }


def _payload(**extra):
    features = {c: (0 if c not in CATEGORICAL else "A") for c in FEATURE_COLUMNS}
    return {"vin": "V1", "pack_serial": "P1", "features": features, **extra}


def test_predict_source_is_wrapped_for_the_model_server():
    text = (ROOT / "serve" / "predict.py").read_text()
    assert "@cml_model\ndef predict" in text


def test_a_high_score_is_the_worklist_and_a_thermal_event_is_p1():
    art = _artifact()
    work = score_request(art, _payload())
    assert work["decision"] == "WORKLIST" and work["action"] == "NONE" and work["priority"] is None
    floor = score_request(art, _payload(safety_floor=1))
    assert floor["decision"] == "SAFETY_FLOOR" and floor["priority"] == "P1" and floor["action"] == "NONE"


def test_a_model_that_is_not_deployable_is_refused():
    art = _artifact()
    art["deployable"] = False
    refused = score_request(art, _payload())
    assert "error" in refused and refused["action"] == "NONE"
