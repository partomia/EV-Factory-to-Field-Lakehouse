"""CAI job definitions. Pure Python so a laptop can create the jobs without cmlapi."""
from __future__ import annotations

PROJECT = "rsingh-ev-factory-to-field"
GIT_URL = "https://github.com/partomia/EV-Factory-to-Field-Lakehouse"
RUNTIME = "docker.repository.cloudera.com/cloudera/cdsw/ml-runtime-pbj-jupyterlab-python3.11-standard:2026.08.1-b5"
SERVICE_MODEL = "evx-uc01-service"
EXPERIMENT = "evx-uc01"

JOBS = [
    {"name": "evx-uc01-00-sync-code", "script": "ci/sync_code.py", "parent": None,
     "cpu": 2, "memory": 8, "timeout": 3600, "schedule": None, "env": {}},
    {"name": "evx-uc01-05-register-deploy", "script": "ci/register_deploy.py", "parent": None,
     "cpu": 1, "memory": 4, "timeout": 5400, "schedule": None, "env": {}},
    {"name": "stop-evx-project", "script": "ci/project_stop.py", "parent": None,
     "cpu": 1, "memory": 4, "timeout": 3600, "schedule": None, "env": {}},
    {"name": "evx-walkthrough-check", "script": "scripts/m1_walkthrough_check.py", "parent": None,
     "cpu": 2, "memory": 8, "timeout": 3600, "schedule": None, "env": {}},
]
