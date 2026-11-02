"""
CAI job stop-evx-project: scheduled end of a live session. Runs
`scripts/platform_ctl.py stop --only cai,nifi` (applications, model deployment, evx-* schedules and
runs, the NiFi process group when built), then clears its own schedule so it fires once.

CDE needs nothing here: the DAG is paused and its end_date bounds the nightly window. The job name
does not start with evx-, so the CAI layer does not stop this run.
Restart later from a laptop: python scripts/platform_ctl.py start
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

JOB_NAME = "stop-evx-project"


def _repo_root() -> Path:
    try:
        return Path(__file__).resolve().parents[1]
    except NameError:
        return Path.cwd()


ROOT = _repo_root()
sys.path.insert(0, str(ROOT))

import importlib.util  # noqa: E402

spec = importlib.util.spec_from_file_location("evx_platform_ctl", ROOT / "scripts" / "platform_ctl.py")
P = importlib.util.module_from_spec(spec)
spec.loader.exec_module(P)


def main() -> int:
    rc = P.main(["stop", "--only", "cai,nifi"] + (["--dry-run"] if os.environ.get("EVX_STOP_DRY_RUN") == "1" else []))
    if os.environ.get("EVX_STOP_DRY_RUN") != "1":
        c = P.Cai()
        for j in c("GET", c.p("/jobs"), params={"page_size": 200})["jobs"]:
            if j["name"] == JOB_NAME and j.get("schedule"):
                c("PATCH", c.p(f"/jobs/{j['id']}"), json={"schedule": ""})
                print(f"cleared the schedule of {JOB_NAME}", flush=True)
    return rc


if __name__ == "__main__":
    rc = main()
    if rc:
        sys.exit(rc)
