"""The feature-hash lock: a model records the salt and the factory column list it was trained on.
Scoring refuses a frame built with another salt, so a CI model cannot score platform features."""
from __future__ import annotations

import hashlib


def schema_hash(salt: str, columns: list[str]) -> str:
    return hashlib.sha256((salt + "|" + "|".join(columns)).encode()).hexdigest()
