#!/usr/bin/env python3
"""
Create the CAI project and its jobs. Idempotent. The repository is public.

  set -a; source .env; set +a
  python ci/setup_cai.py --dry-run
  python ci/setup_cai.py
  python ci/setup_cai.py --run evx-uc01-05-register-deploy

Values from .env are sent as the project environment and never printed.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ci.cai_jobs import GIT_URL, JOBS, PROJECT, RUNTIME  # noqa: E402
from common import load_env  # noqa: E402

ENV_FROM_CALLER = ("EVX_IMPALA_USER", "EVX_IMPALA_PASSWORD", "EVX_WORKLOAD_USER", "EVX_WORKLOAD_PASSWORD")


class Workbench:
    def __init__(self, url: str, key: str):
        self.base = f"{url.rstrip('/')}/api/v2"
        self.h = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}

    def __call__(self, method: str, path: str, params=None, body=None) -> dict:
        r = requests.request(method, self.base + path, params=params, json=body, headers=self.h, timeout=120)
        if r.status_code >= 400:
            raise SystemExit(f"{method} {path}: HTTP {r.status_code}")
        return r.json() if r.text else {}


def find_project(wb: Workbench, name: str = PROJECT) -> dict | None:
    found = wb("GET", "/projects", params={"search_filter": json.dumps({"name": name}), "page_size": 100})
    return next((p for p in found.get("projects", []) if p["name"] == name), None)


def ensure_project(wb: Workbench, dry_run: bool) -> dict | None:
    project = find_project(wb)
    if project:
        print(f"project {PROJECT}: exists ({project['id']})")
        return project
    if dry_run:
        print(f"project {PROJECT}: would create from the public git URL")
        return None
    project = wb("POST", "/projects", body={
        "name": PROJECT, "template": "git", "git_url": GIT_URL, "visibility": "private",
        "default_project_engine_type": "ml_runtime",
        "description": "EV factory-to-field lakehouse. Synthetic data. github.com/partomia/EV-Factory-to-Field-Lakehouse"})
    print(f"project {PROJECT}: created ({project['id']}), cloning", end="", flush=True)
    for _ in range(60):
        status = str(wb("GET", f"/projects/{project['id']}").get("creation_status", "")).lower()
        if status in ("success", "succeeded", ""):
            break
        if "fail" in status or "error" in status:
            raise SystemExit(f"\nproject creation {status}")
        print(".", end="", flush=True)
        time.sleep(5)
    print(" done")
    return project


def wanted_env() -> dict:
    missing = [k for k in ENV_FROM_CALLER if not os.environ.get(k)]
    if missing:
        raise SystemExit(f"set {missing} in the environment (source .env) first")
    env = {k: os.environ[k] for k in ENV_FROM_CALLER}
    if os.environ.get("EVX_BRAND_NAME"):
        env["EVX_BRAND_NAME"] = os.environ["EVX_BRAND_NAME"]
    return env


def ensure_env(wb: Workbench, project: dict, dry_run: bool) -> None:
    current = json.loads(wb("GET", f"/projects/{project['id']}").get("environment") or "{}")
    wanted = wanted_env()
    changed = sorted(k for k, v in wanted.items() if current.get(k) != v)
    if not changed:
        print("project environment: up to date")
    elif dry_run:
        print(f"project environment: would set {changed}")
    else:
        wb("PATCH", f"/projects/{project['id']}", body={"environment": json.dumps({**current, **wanted})})
        print(f"project environment: set {changed}")


def job_ids(wb: Workbench, pid: str) -> dict:
    return {j["name"]: j for j in wb("GET", f"/projects/{pid}/jobs", params={"page_size": 200}).get("jobs", [])}


def ensure_jobs(wb: Workbench, project: dict, dry_run: bool) -> dict:
    pid = project["id"]
    existing = job_ids(wb, pid)
    ids = {n: j["id"] for n, j in existing.items()}
    root = Path(__file__).resolve().parents[1]
    for job in JOBS:
        if job["name"] in existing:
            print(f"job {job['name']}: exists ({existing[job['name']]['id']})")
            continue
        if not (root / job["script"]).exists():
            print(f"job {job['name']}: skipped ({job['script']} is not in this checkout)")
            continue
        if dry_run:
            print(f"job {job['name']}: would create ({job['cpu']} vCPU / {job['memory']} GB)")
            continue
        created = wb("POST", f"/projects/{pid}/jobs", body={
            "name": job["name"], "script": job["script"], "cpu": job["cpu"], "memory": job["memory"],
            "runtime_identifier": RUNTIME, "timeout": job["timeout"], "kill_on_timeout": True,
            "arguments": "", "environment": job.get("env") or {}})
        ids[job["name"]] = created["id"]
        print(f"job {job['name']}: created ({created['id']})")
    return ids


def run_job(wb: Workbench, pid: str, job_id: str, name: str, poll: int = 20) -> str:
    run = wb("POST", f"/projects/{pid}/jobs/{job_id}/runs", body={"environment": {}})
    print(f"{name}: run {run['id']} started", flush=True)
    t0, last = time.time(), None
    while time.time() - t0 < 5400:
        time.sleep(poll)
        st = str(wb("GET", f"/projects/{pid}/jobs/{job_id}/runs/{run['id']}").get("status", "")).lower()
        st = st.replace("engine_", "")
        if st != last:
            print(f"{name}: {st} after {time.time() - t0:.0f}s", flush=True)
            last = st
        if st in ("succeeded", "failed", "stopped", "timedout"):
            return st
    return "timedout"


def main(argv=None) -> int:
    load_env()
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--run", default="")
    p.add_argument("--poll", type=int, default=20)
    args = p.parse_args(argv)
    wb = Workbench(os.environ["EVX_CAI_HOST"], os.environ["EVX_CAI_API_KEY"])
    project = ensure_project(wb, args.dry_run)
    if project is None:
        return 0
    if args.run:
        job = job_ids(wb, project["id"]).get(args.run)
        if not job:
            raise SystemExit(f"no job {args.run}")
        return 0 if run_job(wb, project["id"], job["id"], args.run, args.poll) == "succeeded" else 1
    ensure_env(wb, project, args.dry_run)
    ensure_jobs(wb, project, args.dry_run)
    print(f"project id {project['id']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
