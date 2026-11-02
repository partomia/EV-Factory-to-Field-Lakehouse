"""The published champion passes the gate. A stricter AUROC floor, the one the demo notebook uses, rejects it."""
import json
from pathlib import Path

from mlcore.gate import decide

ROOT = Path(__file__).resolve().parents[1]
# Population counts from the platform training run. The rates live in baseline.json.
CHAMPION = {
    "train_positives": 646,
    "test_positives": 217,
    "fold_positives_min": 122,
    "mixed_share": 0.09577552288258438,
}


def _metrics() -> tuple[dict, dict, dict]:
    base = json.loads((ROOT / "usecases" / "uc01_pack_risk" / "baseline.json").read_text())
    limits = json.loads((ROOT / "config" / "lakehouse.json").read_text())["model"]["gate"]
    return {**base, **CHAMPION}, limits, base


def test_the_published_champion_passes():
    metrics, limits, baseline = _metrics()
    assert metrics["winner"] == "logistic"
    assert decide(metrics, limits, baseline) == []


def test_an_auroc_floor_of_0_90_rejects_the_champion():
    metrics, limits, baseline = _metrics()
    errors = decide(metrics, {**limits, "min_oof_auroc": 0.90}, baseline)
    assert any("AUROC" in e for e in errors)


def test_a_pr_auc_drop_past_the_floor_rejects():
    metrics, limits, baseline = _metrics()
    worse = {**metrics, "oof_pr_auc": baseline["oof_pr_auc"] - 0.03}
    assert any("PR-AUC" in e for e in decide(worse, limits, baseline))
