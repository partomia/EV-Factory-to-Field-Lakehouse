"""
CAI job evx-walkthrough-check: run scripts/m1_walkthrough_check.py on the project kernel.

Installs requirements.txt when impyla is missing (same pattern as sync_code). Writes
outputs/walkthrough.log for download from the project files API.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def _repo_root() -> Path:
    try:
        return Path(__file__).resolve().parents[1]
    except NameError:
        return Path.cwd()


ROOT = _repo_root()


def ensure_deps() -> None:
    try:
        import impala.dbapi  # noqa: F401
        return
    except ImportError:
        pass
    reqs = ROOT / "requirements.txt"
    if reqs.is_file():
        print("installing requirements.txt for walkthrough", flush=True)
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "-r", str(reqs)])


def main() -> int:
    ensure_deps()
    ROOT.joinpath("outputs").mkdir(exist_ok=True)
    log = ROOT / "outputs" / "walkthrough.log"
    env = {**os.environ, "EVX_WALKTHROUGH_ON_CAI": "1"}
    proc = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "m1_walkthrough_check.py")],
        capture_output=True, text=True, cwd=ROOT, env=env,
    )
    text = (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")
    log.write_text(text)
    print(text, flush=True)
    print(f"WALKTHROUGH_LOG outputs/walkthrough.log exit={proc.returncode}", flush=True)
    return proc.returncode


if __name__ == "__main__":
    rc = main()
    if rc:
        sys.exit(rc)
