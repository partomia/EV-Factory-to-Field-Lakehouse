#!/usr/bin/env python3
"""
Resize and upgrade the shared federal services: CDE virtual cluster, CDW Impala warehouse, Data Lake.

Every action here touches services that sibling demos share, so anything but status and --dry-run
needs --yes-shared. Targets live in config/platform.json: "capacity" and "upgrade".

  status   versions, upgrade candidates and sizes of the Data Lake, the CDE service + VC and the
           CDW DBC + VW. Read-only.
  resize   raise the Impala autoscale max (cdp dw update-vw) and the CDE service max instances
           (cdp de update-service), then check the CDE VC quota. The public CLI cannot change a
           VC quota: the script prints the UI step and fails until the quota reads back.
  upgrade  Data Lake to the image whose Runtime build (cdpGBN) is upgrade.target_build, then the
           CDE service and the CDW DBC + VW to the latest version each offers:
             1. checks: target image offered, dry run; PostgreSQL 14 first when the dry run asks
             2. validate and prepare (no downtime)
             3. stop: project workloads, Data Hubs, Impala VW, CDE VC, CAI workspace
                (platform_ctl.py stop --scope platform)
             4. Data Lake backup, then upgrade with --skip-backup (the backup is step 4)
             5. CDE: resume the VC, upgrade-service prepare, backup, upgrade
             6. CDW: upgrade the DBC, start the VW, upgrade the VW
             7. start everything (platform_ctl.py start --scope platform) and read the versions back
           Each step reads state first, so a rerun after a failure resumes where it stopped.

  set -a; source .env; set +a      # CDP_ACCESS_KEY_ID / CDP_PRIVATE_KEY or ~/.cdp/credentials
  python scripts/platform_upgrade.py status
  python scripts/platform_upgrade.py resize --dry-run
  python scripts/platform_upgrade.py resize --yes-shared
  python scripts/platform_upgrade.py upgrade --dry-run [--only datalake,cde,cdw]
  python scripts/platform_upgrade.py upgrade --yes-shared [--build 81274879]
"""
from __future__ import annotations

import argparse
import importlib.util
import math
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

_spec = importlib.util.spec_from_file_location("platform_ctl", Path(__file__).resolve().parent / "platform_ctl.py")
pc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(pc)

CFG, PLT = pc.CFG, pc.PLT
CAP, UPG = CFG["capacity"], CFG["upgrade"]
PARTS = ("datalake", "cde", "cdw")
say, cdp = pc.say, pc.cdp


def act(what: str, fn, *a, **kw):
    return pc.act(what, fn, *a, **kw)


def wait(what: str, fn, ok: set, bad: tuple = ("FAIL", "CANCEL"), timeout: int = 4 * 3600, poll: int = 30) -> str:
    if pc.DRY:
        return ""
    t0, last = time.time(), None
    while time.time() - t0 < timeout:
        st = str(fn())
        if st != last:
            say(f"    {what}: {st} ({time.time() - t0:.0f} s)")
            last = st
        if st.upper() in {s.upper() for s in ok}:
            return st
        if any(b in st.upper() for b in bad):
            raise SystemExit(f"{what}: {st}")
        time.sleep(poll)
    raise SystemExit(f"{what}: still {last} after {timeout} s")


# ------------------------------------------------------------------ Data Lake
def dl_name() -> str:
    lakes = cdp("datalake", "list-datalakes", "--environment-name", CFG["environment"])["datalakes"]
    return lakes[0]["datalakeName"]


def dl_status(name: str) -> str:
    return cdp("datalake", "describe-datalake", "--datalake-name", name)["datalake"]["status"]


def dl_images(name: str) -> dict:
    return cdp("datalake", "upgrade-datalake", "--datalake-name", name, "--show-available-images")


def _label(img: dict) -> str:
    v = img.get("componentVersions") or {}
    return f"{v.get('cdp', '?')} (build {v.get('cdpGBN', '?')}, CM {v.get('cm', '?')}, {v.get('os', '?')}) image {img.get('imageId')}"


def _is_build(img: dict, build: str) -> bool:
    v = img.get("componentVersions") or {}
    return v.get("cdpGBN") == build or str(v.get("cdp", "")).endswith("." + build)


def dl_last_op(name: str) -> dict:
    try:
        return cdp("datalake", "get-operation", "--datalake-name", name)
    except RuntimeError:
        return {}


def dl_run(name: str, what: str, *args: str) -> dict:
    """Start a Data Lake operation and wait for it: get-operation FINISHED, then the lake RUNNING."""
    before = dl_last_op(name).get("operationId")
    out = act(what, cdp, *args) or {}
    op = out.get("operationId")

    def status() -> str:
        cur = cdp("datalake", "get-operation", "--datalake-name", name, *(["--operation-id", op] if op else []))
        return "PENDING" if not op and cur.get("operationId") == before else cur.get("operationStatus", "UNKNOWN")

    wait(what, status, {"FINISHED"})
    wait(f"{name} status", lambda: dl_status(name), {"RUNNING"})
    return out


def dl_status_report() -> None:
    name = dl_name()
    imgs = dl_images(name)
    say(f"  Data Lake {name}: {dl_status(name)}")
    say(f"    current   {_label(imgs.get('current') or {})}")
    for c in imgs.get("upgradeCandidates") or []:
        mark = "  <- target" if _is_build(c, UPG["target_build"]) else ""
        say(f"    candidate {_label(c)}{mark}")
    if imgs.get("reason"):
        say(f"    reason: {imgs['reason']}")


def dl_check(build: str) -> tuple[str, dict | None]:
    """(name, target image) or (name, None) when the lake already runs the build."""
    name = dl_name()
    imgs = dl_images(name)
    if _is_build(imgs.get("current") or {}, build):
        say(f"  Data Lake {name} already runs build {build}: {_label(imgs['current'])}")
        return name, None
    target = next((c for c in imgs.get("upgradeCandidates") or [] if _is_build(c, build)), None)
    if not target:
        offered = "\n    ".join(_label(c) for c in imgs.get("upgradeCandidates") or []) or "none"
        raise SystemExit(f"Data Lake {name}: build {build} is not an upgrade candidate. Offered:\n    {offered}"
                         + (f"\n  reason: {imgs['reason']}" if imgs.get("reason") else ""))
    say(f"  Data Lake {name}: {_label(imgs['current'])}\n    -> {_label(target)}")
    st = dl_status(name)
    if st != "RUNNING":
        raise SystemExit(f"Data Lake {name} is {st}: it must be RUNNING to upgrade")
    dry = ["datalake", "upgrade-datalake", "--datalake-name", name, "--image-id", target["imageId"], "--dry-run"]
    try:
        res = cdp(*dry)
        if res.get("reason"):
            say(f"    dry run: {res['reason']}")
    except RuntimeError as e:
        if not any(k in str(e).lower() for k in ("database", "postgres")):
            raise SystemExit(f"Data Lake dry run refused: {e}")
        say(f"    dry run asks for a database upgrade first: {e}")
        if pc.DRY:
            say("  would upgrade the Data Lake database to PostgreSQL 14, then repeat the dry run")
        else:
            dl_run(name, "upgrade the Data Lake database to PostgreSQL 14 (start-database-upgrade)",
                   "datalake", "start-database-upgrade", "--datalake", name, "--target-version", "VERSION_14")
            cdp(*dry)
    say("    dry run passed")
    return name, target


def dl_prepare(name: str, target: dict) -> None:
    dl_run(name, f"validate and prepare the Data Lake upgrade to build {UPG['target_build']} (no downtime)",
           "datalake", "prepare-datalake-upgrade", "--datalake", name, "--image-id", target["imageId"])


def dl_backup(name: str) -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M")
    bname = f"{UPG['datalake_backup_prefix']}-{UPG['target_build']}-{stamp}"
    out = act(f"back up the Data Lake as {bname} (backup-datalake)", cdp,
              "datalake", "backup-datalake", "--datalake-name", name, "--backup-name", bname) or {}
    bid = out.get("backupId")
    wait(f"backup {bname}", lambda: cdp("datalake", "backup-datalake-status", "--datalake-name", name,
                                         "--backup-id", bid)["status"],
         {"SUCCESSFUL", "SUCCESS", "COMPLETED"})
    if out.get("backupLocation"):
        say(f"    backup location {out['backupLocation']}")


def dl_upgrade(name: str, target: dict) -> None:
    args = ["datalake", "upgrade-datalake", "--datalake-name", name, "--image-id", target["imageId"], "--skip-backup"]
    if UPG.get("datalake_rolling"):
        args.append("--rolling-upgrade-enabled")
    dl_run(name, f"upgrade the Data Lake to {_label(target)}", *args)
    if not pc.DRY:
        cur = dl_images(name).get("current") or {}
        if not _is_build(cur, UPG["target_build"]):
            raise SystemExit(f"Data Lake reads {_label(cur)} after the upgrade, not build {UPG['target_build']}")
        say(f"  Data Lake {name} now runs {_label(cur)}")


# ------------------------------------------------------------------ CDE
def cde_service() -> dict:
    s = next(s for s in cdp("de", "list-services")["services"]
             if s["name"] == PLT["cde_service"] and "delet" not in s["status"].lower())
    return cdp("de", "describe-service", "--cluster-id", s["clusterId"])["service"]


def cde_vc(cluster_id: str) -> dict:
    vc = next(v for v in cdp("de", "list-vcs", "--cluster-id", cluster_id)["vcs"] if v["vcName"] == PLT["cde_vc"])
    return cdp("de", "describe-vc", "--cluster-id", cluster_id, "--vc-id", vc["vcId"])["vc"]


def _num(x) -> float:
    s = str(x or "0").strip()
    for suf, mul in (("Gi", 1), ("G", 1), ("Mi", 1 / 1024), ("Ti", 1024)):
        if s.endswith(suf):
            return float(s[: -len(suf)]) * mul
    return float(s)


def cde_status_report() -> None:
    svc = cde_service()
    r = svc.get("resources") or {}
    say(f"  CDE service {svc['name']}: {svc['status']}, version {svc.get('version', '?')}, "
        f"{r.get('instance_type')} x {r.get('min_instances')}..{r.get('max_instances')}")
    try:
        up = cdp("de", "get-upgrade-status", "--cluster-id", svc["clusterId"])
        say(f"    upgrade: next step {up.get('nextStep') or '-'}, all steps completed {up.get('allStepsCompleted')}, "
            f"{up.get('upgradeStatusMessage') or ''}")
    except RuntimeError as e:
        say(f"    upgrade status: {e}")
    vc = cde_vc(svc["clusterId"])
    vr = vc.get("resources") or {}
    say(f"  CDE virtual cluster {vc['vcName']}: {vc['status']}, quota {vr.get('cpuRequests')} CPU / "
        f"{vr.get('memRequests')} (target {CAP['cde_vc_quota_cpu']} CPU / {CAP['cde_vc_quota_gib']} GiB), "
        f"in use {vr.get('actualCpuRequests')} CPU / {vr.get('actualMemoryRequests')}")


def cde_resize() -> None:
    svc = cde_service()
    r = svc.get("resources") or {}
    vc = cde_vc(svc["clusterId"])
    vr = vc.get("resources") or {}
    cpu, gib = _num(vr.get("cpuRequests")), _num(vr.get("memRequests"))
    cur_max = int(r.get("max_instances") or 0)
    want = CAP.get("cde_service_max_instances")
    if want is None and cpu:
        want = math.ceil(cur_max * max(CAP["cde_vc_quota_cpu"] / cpu, CAP["cde_vc_quota_gib"] / (gib or 1), 1))
    if want and want > cur_max:
        act(f"raise CDE service {svc['name']} max instances {cur_max} -> {want} ({r.get('instance_type')})",
            cdp, "de", "update-service", "--cluster-id", svc["clusterId"], "--maximum-instances", str(want))
    else:
        say(f"  CDE service {svc['name']} max instances {cur_max}: no change")
    if cpu >= CAP["cde_vc_quota_cpu"] and gib >= CAP["cde_vc_quota_gib"]:
        say(f"  CDE VC {vc['vcName']} quota {cpu:g} CPU / {gib:g} GiB: at or above target")
        return
    msg = (f"CDE VC {vc['vcName']} quota is {cpu:g} CPU / {gib:g} GiB; target {CAP['cde_vc_quota_cpu']} CPU / "
           f"{CAP['cde_vc_quota_gib']} GiB. The CDP CLI has no VC quota update: in the Data Engineering UI open "
           f"{PLT['cde_vc']} > Edit (pencil) > Capacity, set CPU {CAP['cde_vc_quota_cpu']} and memory "
           f"{CAP['cde_vc_quota_gib']} GiB, save, then run resize again to read it back.")
    if pc.DRY:
        say(f"  manual step: {msg}")
    else:
        raise SystemExit(msg)


def _upgrade_status(cid: str) -> dict:
    return cdp("de", "get-upgrade-status", "--cluster-id", cid)


def cde_upgrade() -> None:
    svc = cde_service()
    cid, before = svc["clusterId"], svc.get("version")
    vc = cde_vc(cid)
    if "suspend" in vc["status"].lower():
        act(f"resume CDE VC {vc['vcName']} for the service backup", cdp, "de", "resume-vc",
            "--cluster-id", cid, "--vc-id", vc["vcId"])
        wait(vc["vcName"], lambda: cde_vc(cid)["status"], {"AppInstalled", "Running"})
    steps = ["prepare", "backup", "upgrade"]
    st = _upgrade_status(cid)
    start = steps.index(st["nextStep"]) if st.get("nextStep") in steps else 0
    say(f"  CDE service {svc['name']} version {before}: {st.get('upgradeStatusMessage') or 'no message'}")
    for i, step in enumerate(steps[start:], start):
        try:
            act(f"CDE upgrade-service --step {step}", cdp, "de", "upgrade-service", "--cluster-id", cid, "--step", step)
        except RuntimeError as e:
            if step == "prepare" and any(k in str(e).lower() for k in ("no upgrade", "latest", "up to date", "not available")):
                say(f"  CDE service: no upgrade offered ({e})")
                return
            raise
        nxt = steps[i + 1] if i + 1 < len(steps) else None

        def done(nxt=nxt) -> str:
            s = _upgrade_status(cid)
            if (nxt and s.get("nextStep") == nxt) or (not nxt and s.get("allStepsCompleted")):
                return "DONE"
            return s.get("upgradeStatusMessage") or "RUNNING"
        wait(f"CDE {step}", done, {"DONE"})
    wait(f"CDE service {svc['name']}", lambda: cde_service()["status"], {"ClusterCreationCompleted"})
    if not pc.DRY:
        say(f"  CDE service {svc['name']} version {before} -> {cde_service().get('version')}")


# ------------------------------------------------------------------ CDW
def cdw_ids() -> tuple[str, dict]:
    env_crn = cdp("environments", "describe-environment", "--environment-name", CFG["environment"])["environment"]["crn"]
    cid = next(c["id"] for c in cdp("dw", "list-clusters")["clusters"] if c["environmentCrn"] == env_crn)
    vws = {v["name"]: v for v in cdp("dw", "list-vws", "--cluster-id", cid)["vws"]}
    return cid, vws[PLT["cdw_vws"][0]]


def _pick(versions: dict, build: str) -> str | None:
    for k in ("latestVersion", "compatibleVersion"):
        cdp_v = versions.get(k.replace("Version", "CDPVersion")) or ""
        if build in str(cdp_v) and versions.get(k):
            return versions[k]
    return versions.get("latestVersion") or None


def cdw_status_report() -> None:
    cid, vw = cdw_ids()
    a = vw.get("autoscalingOptions") or {}
    say(f"  CDW VW {vw['name']}: {vw['status']}, image {vw.get('imageVersion')}, CDH {vw.get('cdhVersion')}, "
        f"autoscale {a.get('minClusters')}..{a.get('maxClusters')} groups (target max {CAP['impala_autoscale_max_groups']})")
    for kind, args in (("VW", ["get-upgrade-vw-versions", "--vw-id", vw["id"]]),
                       ("DBC", ["get-upgrade-dbc-versions", "--dbc-id", vw["dbcId"]])):
        try:
            v = cdp("dw", *args, "--cluster-id", cid)
            say(f"    {kind} upgrade: latest {v.get('latestVersion')} (CDP {v.get('latestCDPVersion')}), "
                f"compatible {v.get('compatibleVersion')} (CDP {v.get('compatibleCDPVersion')})")
        except RuntimeError as e:
            say(f"    {kind} upgrade versions: {e}")


def cdw_resize() -> None:
    cid, vw = cdw_ids()
    a = vw.get("autoscalingOptions") or {}
    cur, want = int(a.get("maxClusters") or 0), int(CAP["impala_autoscale_max_groups"])
    if cur >= want:
        say(f"  CDW VW {vw['name']} autoscale max {cur}: at or above target {want}")
        return
    act(f"raise CDW VW {vw['name']} autoscale max executor groups {cur} -> {want}", cdp,
        "dw", "update-vw", "--cluster-id", cid, "--vw-id", vw["id"], "--autoscaling", f"maxClusters={want}")
    wait(vw["name"], lambda: cdw_ids()[1].get("status", ""), {"Running", "Stopped", "Suspended"}, bad=("ERROR", "FAIL"))
    if not pc.DRY:
        got = (cdw_ids()[1].get("autoscalingOptions") or {}).get("maxClusters")
        if int(got or 0) < want:
            raise SystemExit(f"CDW VW {vw['name']} autoscale max reads {got} after the update, not {want}")
        say(f"  CDW VW {vw['name']} autoscale max now {got}")


def cdw_upgrade(build: str) -> None:
    cid, vw = cdw_ids()
    dv = cdp("dw", "get-upgrade-dbc-versions", "--cluster-id", cid, "--dbc-id", vw["dbcId"])
    dbc_to = _pick(dv, build)
    if dbc_to:
        act(f"upgrade CDW Database Catalog {vw['dbcId']} to {dbc_to}", cdp, "dw", "upgrade-dbc",
            "--cluster-id", cid, "--dbc-id", vw["dbcId"], "--image-version", dbc_to)
        wait("Database Catalog", lambda: cdp("dw", "describe-dbc", "--cluster-id", cid, "--dbc-id", vw["dbcId"])["dbc"]["status"],
             {"Running"}, bad=("ERROR", "FAIL"))
    else:
        say("  CDW Database Catalog: no upgrade offered")
    if vw["status"].lower() != "running":
        act(f"start CDW VW {vw['name']} for its upgrade", cdp, "dw", "start-vw", "--cluster-id", cid, "--vw-id", vw["id"])
        wait(vw["name"], lambda: cdw_ids()[1]["status"], {"Running"}, bad=("ERROR", "FAIL"))
    vv = cdp("dw", "get-upgrade-vw-versions", "--cluster-id", cid, "--vw-id", vw["id"])
    vw_to = _pick(vv, build)
    if not vw_to or vw_to == vw.get("imageVersion"):
        say(f"  CDW VW {vw['name']}: no upgrade offered (image {vw.get('imageVersion')})")
        return
    act(f"upgrade CDW VW {vw['name']} {vw.get('imageVersion')} -> {vw_to}", cdp, "dw", "upgrade-vw",
        "--cluster-id", cid, "--vw-id", vw["id"], "--dbc-id", vw["dbcId"], "--image-version", vw_to)
    wait(vw["name"], lambda: cdw_ids()[1]["status"], {"Running"}, bad=("ERROR", "FAIL"))
    if not pc.DRY:
        say(f"  CDW VW {vw['name']} image now {cdw_ids()[1].get('imageVersion')}")


# ------------------------------------------------------------------ flows
def status() -> None:
    for title, fn in (("datalake", dl_status_report), ("cde", cde_status_report), ("cdw", cdw_status_report)):
        say(f"[{title}]")
        try:
            fn()
        except Exception as e:  # noqa: BLE001  one part's failure is reported; the others still print
            say(f"  ERROR {type(e).__name__}: {e}")


def resize(only: set) -> None:
    if "cdw" in only:
        say("[cdw]")
        cdw_resize()
    if "cde" in only:
        say("[cde]")
        cde_resize()


def _platform(action: str) -> None:
    say(f"[platform {action}] scripts/platform_ctl.py {action} --scope platform")
    rc = pc.main([action, "--scope", "platform", "--yes-shared"] + (["--dry-run"] if pc.DRY else []))
    if rc:
        raise SystemExit(f"platform_ctl {action} returned {rc}")


def upgrade(only: set, build: str) -> None:
    UPG["target_build"] = build
    target = None
    if "datalake" in only:
        say("[datalake checks]")
        name, target = dl_check(build)
        if target:
            dl_prepare(name, target)
    _platform("stop")
    if target:
        say("[datalake]")
        dl_backup(name)
        dl_upgrade(name, target)
    if "cde" in only:
        say("[cde]")
        cde_upgrade()
    if "cdw" in only:
        say("[cdw]")
        cdw_upgrade(build)
    _platform("start")
    say("[after]")
    if not pc.DRY:
        status()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("action", choices=["status", "resize", "upgrade"])
    ap.add_argument("--only", default="", help=f"comma-separated parts from {', '.join(PARTS)}")
    ap.add_argument("--build", default=UPG["target_build"], help="Runtime build number (cdpGBN) for the Data Lake")
    ap.add_argument("--yes-shared", action="store_true", help="required: every change here hits shared services")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)
    pc.DRY = a.dry_run
    only = {x.strip() for x in a.only.split(",") if x.strip()} or set(PARTS)
    if only - set(PARTS):
        raise SystemExit(f"unknown parts {sorted(only - set(PARTS))}")
    if a.action != "status" and not (a.yes_shared or a.dry_run):
        raise SystemExit(f"{a.action} changes services that sibling demos share: add --yes-shared (or --dry-run)")
    if a.action == "status":
        status()
    elif a.action == "resize":
        resize(only)
    else:
        upgrade(only, a.build)
    return 0


if __name__ == "__main__":
    sys.exit(main())
