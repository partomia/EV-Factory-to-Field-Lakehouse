"""cde/jobs/generate.py: a twin partition runs where config/twin.json cannot be read (CDE executors)."""
import pytest

import evx_common as C
import generate as G
from generators import core, run
from generators import twin as T
from generators.run import TWIN, partition_records

KEY = (TWIN, "2026-11")


@pytest.fixture(scope="module")
def driver_cfg():
    cfg = C.config("config/ci.yaml")
    cfg, keys = G.executor_config(cfg, float(cfg["generator"]["scale"]))
    assert KEY in keys
    return cfg


def test_twin_config_is_read_on_the_driver(driver_cfg):
    assert driver_cfg["twin_cfg"]["line_events_from"]


def test_twin_partition_without_config_file(driver_cfg, monkeypatch, tmp_path):
    for mod in (T, core, run):
        monkeypatch.setattr(mod, "ROOT", tmp_path)
    with pytest.raises(FileNotFoundError):
        T.twin_config({})
    recs = list(partition_records(driver_cfg, KEY))
    files = [r[1] for r in recs if r[0] == "file"]
    assert any(f.startswith("line_event/2026-11-09/") for f in files)
    assert any(f.startswith("twin_scenario/") for f in files)


def test_twin_partition_fails_without_driver_config(driver_cfg, monkeypatch, tmp_path):
    monkeypatch.setattr(T, "ROOT", tmp_path)
    cfg = {k: v for k, v in driver_cfg.items() if k != "twin_cfg"}
    with pytest.raises(FileNotFoundError):
        list(partition_records(cfg, KEY))
