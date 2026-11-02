"""
Job evx-uc01-00-sync-code: pull origin/main into the CAI project.

  git fetch + reset --hard (data, models and outputs are gitignored and stay put)
  pip install requirements.txt when the file hash changes (marker in outputs/.requirements.sha256)

EXPECTED_GIT_SHA (optional, from a trigger) stops the chain if the branch moved on.
"""
from __future__ import annotations

import hashlib
import os
import subprocess
import sys
from pathlib import Path


def _repo_root() -> Path:
    try:
        return Path(__file__).resolve().parents[1]
    except NameError:
        return Path(os.getcwd())


ROOT = _repo_root()


def git(*args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def install_requirements(root: Path) -> bool:
    reqs = root / "requirements.txt"
    digest = hashlib.sha256(reqs.read_bytes()).hexdigest()
    marker = root / "outputs" / ".requirements.sha256"
    if marker.exists() and marker.read_text().strip() == digest:
        print(f"requirements.txt unchanged ({digest[:12]}): nothing to install")
        return False
    print(f"installing requirements.txt ({digest[:12]})", flush=True)
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "-r", str(reqs)])
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(digest)
    return True


def main() -> int:
    branch = os.environ.get("GIT_BRANCH", "main")
    git("fetch", "origin", branch)
    git("reset", "--hard", f"origin/{branch}")
    sha = git("rev-parse", "HEAD")
    (ROOT / "outputs").mkdir(exist_ok=True)
    (ROOT / "outputs" / "git_sha.txt").write_text(sha)
    print(f"SYNC_RESULT sha={sha}")

    expected = os.environ.get("EXPECTED_GIT_SHA", "")
    if expected and not sha.startswith(expected):
        print(f"expected {expected} but origin/{branch} is {sha}: stopping the chain")
        return 1
    install_requirements(ROOT)
    return 0


if __name__ == "__main__":
    rc = main()
    if rc:
        sys.exit(rc)
