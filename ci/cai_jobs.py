"""CAI job definitions. Pure Python so a laptop can create the jobs without cmlapi."""
from __future__ import annotations

PROJECT = "rsingh-ev-factory-to-field"
GIT_URL = "https://github.com/partomia/EV-Factory-to-Field-Lakehouse"
RUNTIME = "docker.repository.cloudera.com/cloudera/cdsw/ml-runtime-pbj-jupyterlab-python3.11-standard:2026.08.1-b5"
SERVICE_MODEL = "evx-uc01-service"
EXPERIMENT = "evx-uc01"

JOBS = [
    {"name": "evx-uc01-05-register-deploy", "script": "ci/register_deploy.py", "parent": None,
     "cpu": 1, "memory": 4, "timeout": 5400, "schedule": None, "env": {}},
]
