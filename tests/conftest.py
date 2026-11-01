import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "cde" / "jobs"))
os.environ.setdefault("EVX_CONFIG_OVERLAY", "config/ci.yaml")

SAMPLE_KEYS = [("SUPPLY", "-"), ("CL1", "2026-09"), ("CL1", "2026-11")]


@pytest.fixture(scope="session")
def ci_cfg():
    from generators.run import prepare
    import evx_common as C

    cfg = C.config("config/ci.yaml")
    return prepare(cfg, float(cfg["generator"]["scale"]))


@pytest.fixture(scope="session")
def generated(tmp_path_factory, ci_cfg):
    """CI-scale output of the supply partition and two production partitions (one with the live dates)."""
    from generators.run import run_local

    cfg, keys = ci_cfg
    out = tmp_path_factory.mktemp("generated")
    stats = run_local(out, cfg, [k for k in keys if k in SAMPLE_KEYS])
    return out, stats
