"""M1 scoring decisions: the safety floor, abstains, the worklist, and the feature-hash lock."""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from mlcore.hashlock import schema_hash  # noqa: E402
from usecases.uc01_pack_risk.build_features import CATEGORICAL, FEATURE_COLUMNS, FEATURE_SET  # noqa: E402
from usecases.uc01_pack_risk.score import (  # noqa: E402
    ACTION, ScoreRefused, assert_locked, decide_one, guardrail_kinds, score_frame,
)

THRESHOLDS = {"worklist": 0.9, "abstain": 0.05}


def _artifact():
    class Model:
        def predict_proba(self, x):
            # Row order is the frame order: worklist, band, clear, then three overrides.
            p = np.array([0.95, 0.20, 0.01, 0.95, 0.01, 0.95])
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
        "thresholds": THRESHOLDS, "feature_columns": list(FEATURE_COLUMNS),
        "hash_salt": "platform", "schema_hash": schema_hash("platform", FEATURE_COLUMNS),
        "deployable": True,
    }


def _frame():
    n = 6
    frame = pd.DataFrame({c: np.zeros(n) for c in FEATURE_COLUMNS})
    for c in CATEGORICAL:
        frame[c] = "A"
    frame["vin"] = [f"V{i}" for i in range(n)]
    frame["pack_serial"] = frame["vin"]
    frame["genealogy_open"] = [0, 0, 0, 1, 0, 0]
    frame["safety_floor"] = [0, 0, 0, 0, 1, 0]
    frame["feature_set"] = FEATURE_SET
    frame["feature_hash"] = "a" * 64
    frame.loc[5, "ce_mean"] = 1e6
    frame.loc[2, "feature_hash"] = "nope"
    return frame


def test_decisions_follow_the_guardrail_order():
    assert decide_one(0.01, False, False, True, True, THRESHOLDS) == ("SAFETY_FLOOR", "safety_floor", "P1")
    assert decide_one(0.99, False, True, False, False, THRESHOLDS) == ("ABSTAIN", "contract", None)
    assert decide_one(0.99, True, True, False, True, THRESHOLDS) == ("ABSTAIN", "genealogy", None)
    assert decide_one(0.99, True, False, False, True, THRESHOLDS) == ("ABSTAIN", "ood", None)
    assert decide_one(0.95, False, False, False, True, THRESHOLDS) == ("WORKLIST", "worklist", None)
    assert decide_one(0.20, False, False, False, True, THRESHOLDS) == ("ABSTAIN", "abstain_band", None)
    assert decide_one(0.01, False, False, False, True, THRESHOLDS) == ("CLEAR", "clear", None)


def test_a_safety_floor_row_still_logs_an_open_genealogy():
    kinds = guardrail_kinds({"safety_floor": 1, "contract_ok": True, "genealogy_open": 1,
                             "ood": True, "reason": "safety_floor"})
    assert ("safety_floor", "P1") in kinds
    assert ("genealogy", "ABSTAIN") in kinds
    assert not any(k == "ood" for k, _ in kinds)


def test_the_lock_refuses_another_salt_and_a_model_that_is_not_deployable():
    artifact = _artifact()
    assert_locked(artifact, "platform", FEATURE_COLUMNS)
    artifact["hash_salt"] = "ci"
    artifact["schema_hash"] = schema_hash("ci", FEATURE_COLUMNS)
    try:
        assert_locked(artifact, "platform", FEATURE_COLUMNS)
        raise AssertionError("expected ScoreRefused")
    except ScoreRefused:
        pass
    artifact = _artifact()
    artifact["deployable"] = False
    try:
        assert_locked(artifact, "platform", FEATURE_COLUMNS)
        raise AssertionError("expected ScoreRefused")
    except ScoreRefused:
        pass


def test_score_frame_applies_every_band_and_never_an_action():
    scored = score_frame(_frame(), _artifact(), "platform")
    assert list(scored["decision"]) == ["WORKLIST", "ABSTAIN", "ABSTAIN", "ABSTAIN", "SAFETY_FLOOR", "ABSTAIN"]
    assert list(scored["reason"]) == ["worklist", "abstain_band", "contract", "genealogy", "safety_floor", "ood"]
    assert list(scored["priority"]) == [None, None, None, None, "P1", None]
    assert set(scored["action"]) == {ACTION}
    assert scored.loc[4, "priority"] == "P1"
