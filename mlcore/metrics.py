"""Precision-recall, AUROC and expected calibration error on one vector of scores."""
from __future__ import annotations

import numpy as np


def pr_auc(y, score) -> float:
    from sklearn.metrics import average_precision_score

    if len(np.unique(y)) < 2:
        return float("nan")
    return float(average_precision_score(y, score))


def auroc(y, score) -> float:
    from sklearn.metrics import roc_auc_score

    if len(np.unique(y)) < 2:
        return float("nan")
    return float(roc_auc_score(y, score))


def ece(y, p, bins: int = 10) -> float:
    y, p = np.asarray(y, float), np.clip(np.asarray(p, float), 0, 1)
    edges = np.linspace(0, 1, bins + 1)
    err, n = 0.0, len(y)
    if not n:
        return float("nan")
    for i in range(bins):
        hi = p <= edges[i + 1] if i == bins - 1 else p < edges[i + 1]
        m = (p >= edges[i]) & hi
        if m.any():
            err += m.sum() / n * abs(y[m].mean() - p[m].mean())
    return float(err)


def lift(score_pr: float, prevalence: float) -> float:
    if not prevalence:
        return float("nan")
    return float(score_pr / prevalence)
