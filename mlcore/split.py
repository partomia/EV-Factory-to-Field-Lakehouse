"""Time-ordered split grouped by cell production batch.

Batches are ordered by the last in-service date of their packs. TEST is the latest fraction.
TRAIN is the rest, and its folds are interleaved along that order, so no batch is in two folds
or on both sides. A pack follows its majority batch.
"""
from __future__ import annotations


def batch_split(last_day: dict[str, str], test_fraction: float = 0.2, folds: int = 5) -> dict[str, tuple[str, int | None]]:
    """batch id -> (\"train\"|\"test\", fold or None)."""
    ordered = sorted(last_day, key=lambda b: (last_day[b], b))
    if len(ordered) < folds + 1:
        return {b: ("train", 0) for b in ordered}
    n_test = int(round(len(ordered) * test_fraction))
    n_test = min(max(n_test, 1), len(ordered) - folds)
    out = {b: ("test", None) for b in ordered[-n_test:]}
    for i, b in enumerate(ordered[:-n_test]):
        out[b] = ("train", i % folds)
    return out


def mixed_share(n_batches: list[int]) -> float:
    if not n_batches:
        return 0.0
    return sum(1 for n in n_batches if n and n > 1) / len(n_batches)
