"""scripts/platform_upgrade.py against a fake cdp CLI (no network)."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
CFG = json.loads((ROOT / "config" / "platform.json").read_text())
BUILD = CFG["upgrade"]["target_build"]


def _img(gbn, image_id):
    return {"imageId": image_id, "componentVersions": {"cdp": f"7.3.1-1.cdh7.3.1.p500.{gbn}", "cdpGBN": gbn,
                                                       "cm": "7.13.1", "os": "redhat8"}}


class FakeCdp:
    def __init__(self, current_gbn="70000000", candidates=(BUILD,), vc_cpu="20", vc_mem="80Gi", vw_max=5):
        self.calls = []
        self.current_gbn, self.candidates = current_gbn, candidates
        self.vc_cpu, self.vc_mem, self.vw_max = vc_cpu, vc_mem, vw_max

    def __call__(self, *args):
        self.calls.append(args)
        a = " ".join(args[:2])
        if a == "datalake list-datalakes":
            return {"datalakes": [{"datalakeName": "federal-dl"}]}
        if a == "datalake describe-datalake":
            return {"datalake": {"status": "RUNNING"}}
        if a == "datalake upgrade-datalake":
            if "--show-available-images" in args:
                return {"current": _img(self.current_gbn, "img-cur"),
                        "upgradeCandidates": [_img(g, f"img-{g}") for g in self.candidates]}
            return {}
        if a == "datalake get-operation":
            return {"operationId": "op0", "operationStatus": "FINISHED"}
        if a == "de list-services":
            return {"services": [{"name": CFG["platform"]["cde_service"], "clusterId": "cluster-1",
                                  "status": "ClusterCreationCompleted"}]}
        if a == "de describe-service":
            return {"service": {"name": CFG["platform"]["cde_service"], "clusterId": "cluster-1", "version": "1.24.1",
                                "status": "ClusterCreationCompleted",
                                "resources": {"instance_type": "m5.4xlarge", "min_instances": "0", "max_instances": "5"}}}
        if a == "de list-vcs":
            return {"vcs": [{"vcName": CFG["platform"]["cde_vc"], "vcId": "dex-app-1"}]}
        if a == "de describe-vc":
            return {"vc": {"vcName": CFG["platform"]["cde_vc"], "vcId": "dex-app-1", "status": "AppInstalled",
                           "resources": {"cpuRequests": self.vc_cpu, "memRequests": self.vc_mem}}}
        if a == "de get-upgrade-status":
            return {"nextStep": "prepare", "allStepsCompleted": False}
        if a == "environments describe-environment":
            return {"environment": {"crn": "env-crn"}}
        if a == "dw list-clusters":
            return {"clusters": [{"id": "env-x", "environmentCrn": "env-crn"}]}
        if a == "dw list-vws":
            return {"vws": [{"name": CFG["platform"]["cdw_vws"][0], "id": "compute-1", "dbcId": "warehouse-1",
                             "status": "Running", "imageVersion": "2025.0.19.0-1",
                             "autoscalingOptions": {"minClusters": 1, "maxClusters": self.vw_max}}]}
        if a in ("dw get-upgrade-vw-versions", "dw get-upgrade-dbc-versions"):
            return {"latestVersion": "2025.0.20.1-9", "latestCDPVersion": f"7.3.1.500-{BUILD}"}
        return {}


@pytest.fixture
def mod():
    spec = importlib.util.spec_from_file_location("evx_platform_upgrade", ROOT / "scripts" / "platform_upgrade.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def _wire(mod, fake, monkeypatch):
    monkeypatch.setattr(mod, "cdp", fake)
    monkeypatch.setattr(mod.pc, "cdp", fake)
    platform_calls = []
    monkeypatch.setattr(mod.pc, "main", lambda argv: platform_calls.append(argv) or 0)
    return platform_calls


def _writes(fake):
    reads = ("list-", "describe-", "get-", "--show-available-images", "--dry-run")
    return [c for c in fake.calls if not any(r in " ".join(c) for r in reads)]


def test_changes_need_yes_shared(mod):
    with pytest.raises(SystemExit, match="yes-shared"):
        mod.main(["upgrade"])
    with pytest.raises(SystemExit, match="yes-shared"):
        mod.main(["resize"])


def test_upgrade_dry_run_plans_every_step_and_writes_nothing(mod, monkeypatch, capsys):
    fake = FakeCdp()
    platform_calls = _wire(mod, fake, monkeypatch)
    assert mod.main(["upgrade", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert _writes(fake) == []
    assert ("datalake", "upgrade-datalake", "--datalake-name", "federal-dl", "--image-id", f"img-{BUILD}",
            "--dry-run") in fake.calls
    for step in ("validate and prepare", "back up the Data Lake", f"upgrade the Data Lake to 7.3.1-1.cdh7.3.1.p500.{BUILD}",
                 "CDE upgrade-service --step prepare", "CDE upgrade-service --step backup",
                 "CDE upgrade-service --step upgrade",
                 "upgrade CDW Database Catalog warehouse-1 to 2025.0.20.1-9", "upgrade CDW VW"):
        assert f"would {step}" in out, step
    assert out.index("validate and prepare") < out.index("back up the Data Lake") < out.index("CDE upgrade-service")
    assert [c[0] for c in platform_calls] == ["stop", "start"]
    assert all("--scope" in c and "platform" in c and "--dry-run" in c for c in platform_calls)


def test_missing_build_stops_before_anything_is_stopped(mod, monkeypatch):
    fake = FakeCdp(candidates=("80000001",))
    platform_calls = _wire(mod, fake, monkeypatch)
    with pytest.raises(SystemExit, match=f"build {BUILD} is not an upgrade candidate"):
        mod.main(["upgrade", "--dry-run"])
    assert platform_calls == []


def test_datalake_already_on_build_is_skipped(mod, monkeypatch, capsys):
    fake = FakeCdp(current_gbn=BUILD, candidates=())
    _wire(mod, fake, monkeypatch)
    mod.main(["upgrade", "--dry-run", "--only", "datalake"])
    out = capsys.readouterr().out
    assert f"already runs build {BUILD}" in out
    assert "would back up the Data Lake" not in out


def test_resize_dry_run(mod, monkeypatch, capsys):
    fake = FakeCdp()
    _wire(mod, fake, monkeypatch)
    mod.main(["resize", "--dry-run"])
    out = capsys.readouterr().out
    cap = CFG["capacity"]
    assert f"would raise CDW VW {CFG['platform']['cdw_vws'][0]} autoscale max executor groups 5 -> " \
           f"{cap['impala_autoscale_max_groups']}" in out
    # 5 nodes scaled by the larger of the CPU (48/20) and memory (192/80) increase, rounded up
    assert "would raise CDE service federal-cde max instances 5 -> 12" in out
    assert "manual step" in out and "Capacity" in out
    assert _writes(fake) == []


def test_resize_fails_until_vc_quota_reads_back(mod, monkeypatch):
    fake = FakeCdp(vw_max=CFG["capacity"]["impala_autoscale_max_groups"])
    _wire(mod, fake, monkeypatch)
    with pytest.raises(SystemExit, match="no VC quota update"):
        mod.main(["resize", "--yes-shared", "--only", "cde"])
    ok = FakeCdp(vc_cpu=str(CFG["capacity"]["cde_vc_quota_cpu"]), vc_mem=f"{CFG['capacity']['cde_vc_quota_gib']}Gi")
    _wire(mod, ok, monkeypatch)
    ok.calls.clear()
    assert mod.main(["resize", "--yes-shared", "--only", "cde"]) == 0
    assert _writes(ok) == []


def test_vw_version_prefers_the_target_build(mod):
    v = {"latestVersion": "L", "latestCDPVersion": "7.3.2-99999999", "compatibleVersion": "C",
         "compatibleCDPVersion": f"7.3.1-{BUILD}"}
    assert mod._pick(v, BUILD) == "C"
    assert mod._pick({"latestVersion": "L"}, BUILD) == "L"
    assert mod._pick({}, BUILD) is None
