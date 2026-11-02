"""The KPI gate. Thresholds are not chosen here: the caller passes metrics already taken from
out-of-fold scores. A drop against the stored baseline is a non-regression failure."""
from __future__ import annotations


def decide(metrics: dict, limits: dict, baseline: dict | None = None) -> list[str]:
    errors = []
    need = (
        ("train_positives", "min_positives_per_split"),
        ("test_positives", "min_positives_per_split"),
        ("fold_positives_min", "min_positives_per_fold"),
    )
    for got, floor in need:
        if metrics.get(got, 0) < limits[floor]:
            errors.append(f"{got} {metrics.get(got, 0)} < {limits[floor]}")
    if metrics.get("oof_auroc", 0) < limits["min_oof_auroc"]:
        errors.append(f"oof AUROC {metrics.get('oof_auroc')} < {limits['min_oof_auroc']}")
    if metrics.get("oof_pr_lift", 0) < limits["min_pr_auc_lift"]:
        errors.append(f"oof PR-AUC lift {metrics.get('oof_pr_lift')} < {limits['min_pr_auc_lift']}")
    if metrics.get("oof_ece", 1) > limits["max_ece"]:
        errors.append(f"oof ECE {metrics.get('oof_ece')} > {limits['max_ece']}")
    if metrics.get("mixed_share", 0) > limits["mixed_pack_max"]:
        errors.append(f"mixed packs {metrics.get('mixed_share'):.3f} > {limits['mixed_pack_max']}")
    if baseline and metrics.get("oof_pr_auc", 0) < baseline["oof_pr_auc"] - limits.get("max_pr_auc_drop", 0.02):
        errors.append(f"oof PR-AUC {metrics.get('oof_pr_auc')} regressed from {baseline['oof_pr_auc']}")
    return errors
