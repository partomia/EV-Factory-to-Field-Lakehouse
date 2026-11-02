import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from mlcore.gate import decide  # noqa: E402
from mlcore.hashlock import schema_hash  # noqa: E402
from mlcore.split import batch_split, mixed_share  # noqa: E402
from scripts.check_cause_split import coverage  # noqa: E402
from usecases.uc01_pack_risk.build_features import FEATURE_COLUMNS  # noqa: E402
from usecases.uc01_pack_risk.train import LIMITS, fit_champion  # noqa: E402


def test_latest_batches_are_test_and_folds_do_not_share_one():
    last = {f"B{i:03d}": f"2026-{(i // 28) + 1:02d}-{(i % 27) + 1:02d}" for i in range(50)}
    got = batch_split(last, 0.2, 5)
    ordered = sorted(last, key=lambda b: (last[b], b))
    test = [b for b in ordered if got[b][0] == "test"]
    assert test == ordered[-len(test):]
    assert len(test) == 10
    folds = {}
    for b, (side, fold) in got.items():
        assert b not in folds
        folds[b] = (side, fold)
        if side == "train":
            assert fold in range(5)
        else:
            assert fold is None


def test_gate_rejects_a_short_split_and_a_regression():
    metrics = {"train_positives": 10, "test_positives": 200, "fold_positives_min": 200,
               "oof_auroc": 0.8, "oof_pr_lift": 3, "oof_ece": 0.01, "mixed_share": 0.0, "oof_pr_auc": 0.4}
    assert any("train_positives" in e for e in decide(metrics, LIMITS))
    good = {**metrics, "train_positives": 200, "fold_positives_min": 120}
    assert decide(good, LIMITS) == []
    assert decide(good, LIMITS, {"oof_pr_auc": 0.5})


def test_schema_hash_changes_with_the_salt():
    assert schema_hash("ci", FEATURE_COLUMNS) != schema_hash("platform", FEATURE_COLUMNS)


def test_a_cause_on_one_side_is_reported():
    splits = {"p1": "train", "p2": "test", "p3": "train"}
    rows = [("p1", "drift"), ("p2", "drift"), ("p3", "weak_bond")]
    assert coverage(rows, splits) == ["weak_bond"]


def test_mixed_share():
    assert mixed_share([1, 1, 2, 3]) == 0.5


def test_champion_beats_the_base_rate_on_a_planted_signal():
    rng = np.random.default_rng(20261101)
    n, batches = 800, 40
    batch = np.array([f"B{i // (n // batches):02d}" for i in range(n)])
    day = np.array([f"2026-{(i // 28) + 1:02d}-01" for i in range(batches)])
    signal = rng.normal(np.where(np.arange(n) % 5 == 0, 2.0, 0.0))
    label = (signal + rng.normal(scale=0.4, size=n) > 1.2).astype(int)
    frame = pd.DataFrame({c: np.zeros(n) for c in FEATURE_COLUMNS})
    frame["ce_mean"] = signal
    frame["vin"] = [f"V{i}" for i in range(n)]
    frame["pack_serial"] = frame["vin"]
    frame["matured"] = 1
    frame["label"] = label
    frame["genealogy_open"] = 0
    frame["n_batches"] = 1
    frame["batch_id"] = batch
    frame["in_service_date"] = [day[int(b[1:])] for b in batch]
    for c in ("pack_line", "shift", "wb_station", "batch_status"):
        frame[c] = "A"
    cfg = {"test_fraction": 0.2, "folds": 5, "worklist_fraction": 0.1, "abstain_fraction": 0.2,
           "n_estimators": 40, "seed": 20261101, "salt": "platform", "gate": LIMITS}
    metrics = fit_champion(frame, cfg)["metrics"]
    assert metrics["oof_pr_lift"] > 1.5
    assert metrics["test_positives"] > 0 and metrics["train_positives"] > 0
    assert metrics["winner"] in ("lightgbm", "logistic", "hist_gradient_boosting")
