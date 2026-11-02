"""platform.json and platform_ctl CLI contract (no live API)."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
CFG = json.loads((ROOT / "config" / "platform.json").read_text())


def test_platform_json_project_names():
    prj = CFG["project"]
    assert prj["cai_project"] == "rsingh-ev-factory-to-field"
    assert prj["cai_job_prefix"] == "evx-"
    assert prj["cde_job_prefix"] == "rsingh-evx-"
    assert "EVX Quality and Reliability Workbench" in prj["cai_apps"]
    assert "evx-uc01-service" in prj["cai_models"]


def test_platform_json_layer_order():
    assert set(CFG["stop_order"]) == set(CFG["start_order"])
    assert CFG["stop_order"][0] == "cai"
    assert CFG["start_order"][-1] == "cai"


def test_platform_resources_defaults_and_ceilings():
    res = CFG["resources"]
    assert res["defaults"]["cai_app"]["cpu"] == 2
    assert res["ceilings"]["cai_app"]["cpu"] >= res["defaults"]["cai_app"]["cpu"]
    assert isinstance(res["changes"], list)


def test_project_stop_script_exists():
    assert (ROOT / "ci" / "project_stop.py").is_file()
    assert (ROOT / "scripts" / "platform_ctl.py").is_file()


def test_platform_ctl_dry_run_stop(capsys):
    import importlib.util

    spec = importlib.util.spec_from_file_location("platform_ctl", ROOT / "scripts" / "platform_ctl.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    class FakeCai:
        pid = "fake"

        def __call__(self, method, path, **kw):
            if path.endswith("/applications"):
                return {"applications": [{"name": n, "id": "a1", "status": "APPLICATION_RUNNING",
                                          "cpu": 2, "memory": 8} for n in CFG["project"]["cai_apps"]]}
            if path.endswith("/jobs"):
                return {"jobs": []}
            if path.endswith("/models"):
                return {"models": []}
            return {}

        def p(self, path):
            return path

        def apps(self):
            return self("GET", self.p("/applications"))["applications"]

        def jobs(self):
            return []

        def deployments(self):
            return []

        def active_runs(self, job):
            return []

    mod.Cai = FakeCai
    rc = mod.main(["stop", "--only", "cai", "--dry-run"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "would stop application" in out
