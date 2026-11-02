"""
CAI job evx-uc01-05-register-deploy.

Registers the champion in the AI Registry, then builds and deploys serve/predict.py as
evx-uc01-service (1 vCPU / 2 GB). Registration happens first. A retry that finds the same
MLflow run already registered does not create another version. sys.exit only on failure.
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path


def _repo_root() -> Path:
    try:
        return Path(__file__).resolve().parents[1]
    except NameError:
        return Path(os.getcwd())


ROOT = _repo_root()
sys.path.insert(0, str(ROOT))

from ci.cai_jobs import EXPERIMENT, RUNTIME, SERVICE_MODEL  # noqa: E402
from mlcore.registry import REGISTRY_NAME, register_version  # noqa: E402

CHAMPION = ROOT / "models" / "champion" / "model.joblib"
RUN_FILE = ROOT / "models" / "champion" / "mlflow_run_id"
MODEL_CPU, MODEL_MEMORY = 1, 2


def finish(rc: int) -> None:
    if rc:
        sys.exit(rc)


def log_winner(artifact: dict) -> tuple[str, str]:
    """Return (run_id, experiment_id). Reuses mlflow_run_id when this champion was already logged."""
    import mlflow
    import mlflow.sklearn

    experiment = mlflow.set_experiment(EXPERIMENT)
    if RUN_FILE.exists():
        return RUN_FILE.read_text().strip(), experiment.experiment_id
    with mlflow.start_run(run_name="pack_risk_eol"):
        mlflow.log_param("winner", artifact.get("winner"))
        mlflow.log_param("schema_hash", artifact["schema_hash"])
        mlflow.log_metric("oof_auroc", float(artifact["metrics"]["oof_auroc"]))
        mlflow.sklearn.log_model(artifact["model"], "model")
        run_id = mlflow.active_run().info.run_id
    RUN_FILE.write_text(run_id + "\n")
    print(f"MLFLOW_RUN {run_id}", flush=True)
    return run_id, experiment.experiment_id


def _status(obj) -> str:
    return str(getattr(obj, "status", None) or (obj.get("status") if isinstance(obj, dict) else "")).lower()


def wait(fn, ok: set, what: str, timeout: int) -> str:
    t0, last = time.time(), None
    while time.time() - t0 < timeout:
        status = _status(fn())
        if status != last:
            print(f"  {what}: {status}", flush=True)
            try:
                with (ROOT / "evx_deploy.log").open("a") as f:
                    f.write(f"{what}: {status}\n")
            except OSError:
                pass
            last = status
        if status in ok:
            return status
        if "fail" in status or status in {"stopped", "timedout"}:
            raise RuntimeError(f"{what} ended in status {status}")
        time.sleep(20)
    raise RuntimeError(f"{what} timed out after {timeout}s")


def choose_runtime(candidates: list[tuple[str, str]], full: str | None, pinned: str) -> str:
    """Pick the model image. `full` is the short release (ML_RUNTIME_FULL_VERSION), shared by every
    runtime in that release, so the last match can be Scala. Use the pinned Python image when it is listed."""
    images = [img for _, img in candidates if img]
    if pinned in images or not full:
        return pinned
    python = [img for ver, img in candidates if ver == full and "python3.11" in img and "standard" in img and "cuda" not in img]
    jupyter = [img for img in python if "jupyterlab" in img]
    return (jupyter or python or [pinned])[-1]


def _runtime(client) -> str:
    full = os.environ.get("ML_RUNTIME_FULL_VERSION")
    try:
        resp = client.list_runtimes(page_size=100)
        found = [(getattr(r, "full_version", "") or "", getattr(r, "image_identifier", "") or "")
                 for r in (resp.runtimes or [])]
    except Exception as e:  # noqa: BLE001
        print(f"runtime lookup failed ({type(e).__name__}); using the pinned image", flush=True)
        return RUNTIME
    return choose_runtime(found, full, RUNTIME)


def _deployment_after_deadline(client, pid, model_id, build_id):
    """create_model_deployment can 500 when the gateway's 30s deadline passes. The deployment may still exist."""
    time.sleep(10)
    resp = client.list_model_deployments(pid, model_id, build_id)
    deps = list(getattr(resp, "model_deployments", None) or [])
    return deps[-1] if deps else None


def deploy(client, pid: str, comment: str) -> dict:
    import cmlapi

    name = SERVICE_MODEL
    found = [m for m in client.list_models(pid, search_filter=json.dumps({"name": name})).models if m.name == name]
    model = found[0] if found else client.create_model(cmlapi.CreateModelRequest(
        project_id=pid, name=name, description="pack_risk_eol scores. Workflow prioritisation only; no automated action.",
        disable_authentication=False), pid)
    runtime = _runtime(client)
    print(f"model {model.name} id={model.id}", flush=True)
    build = None
    for attempt in range(1, 4):
        build = client.create_model_build(cmlapi.CreateModelBuildRequest(
            project_id=pid, model_id=model.id, file_path="serve/predict.py", function_name="predict",
            runtime_identifier=runtime, comment=comment), pid, model.id)
        try:
            wait(lambda: client.get_model_build(pid, model.id, build.id), {"built", "succeeded"}, "build", 600)
            break
        except RuntimeError as e:
            if attempt == 3:
                raise
            print(f"build attempt {attempt}: {e}; starting another", flush=True)
    try:
        dep = client.create_model_deployment(cmlapi.CreateModelDeploymentRequest(
            project_id=pid, model_id=model.id, build_id=build.id, cpu=MODEL_CPU, memory=MODEL_MEMORY),
            pid, model.id, build.id)
    except Exception as e:  # noqa: BLE001
        if "deadline" not in str(e).lower() and "500" not in str(e):
            raise
        print(f"deploy create returned {type(e).__name__}; looking up the deployment", flush=True)
        dep = _deployment_after_deadline(client, pid, model.id, build.id)
        if dep is None:
            raise
    wait(lambda: client.get_model_deployment(pid, model.id, build.id, dep.id), {"deployed"}, "deployment", 2400)
    print(f"DEPLOY_RESULT model={model.id} build={build.id} deployment={dep.id}", flush=True)
    return {"model_id": model.id, "build_id": build.id, "deployment_id": dep.id}


def _note(msg: str) -> None:
    print(msg, flush=True)
    try:
        with (ROOT / "evx_deploy.log").open("a") as f:
            f.write(msg + "\n")
    except OSError:
        pass


def _import_joblib():
    try:
        import joblib
        import sklearn
        if sklearn.__version__ != "1.9.1":
            raise ImportError(f"scikit-learn {sklearn.__version__}")
        return joblib
    except ImportError:
        import subprocess
        _note("pip install requirements-model.txt")
        proc = subprocess.Popen(
            [sys.executable, "-m", "pip", "install", "--disable-pip-version-check", "--only-binary=:all:",
             "-r", str(ROOT / "requirements-model.txt")],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in proc.stdout:
            _note("pip " + line.rstrip())
        code = proc.wait()
        if code != 0:
            _note(f"pip exit {code}")
            raise RuntimeError("pip install failed")
        import joblib
        return joblib


def main() -> int:
    try:
        (ROOT / "evx_deploy.log").write_text("")
    except OSError:
        pass
    here = Path(globals().get("__file__", "nofile"))
    _note(f"cwd={os.getcwd()} root={ROOT} champion={CHAMPION.exists()} script={here}")
    models = ROOT / "models"
    _note("models=" + (", ".join(str(p.relative_to(ROOT)) for p in list(models.rglob("*"))[:30]) if models.exists() else "missing"))
    _note("importing the model packages")
    joblib = _import_joblib()
    if not CHAMPION.exists():
        _note("SCORE_REFUSED models/champion/model.joblib is not in the project")
        return 1
    _note("loading the champion")
    artifact = joblib.load(CHAMPION)
    if artifact.get("hash_salt") != "platform" or not artifact.get("deployable"):
        _note("SCORE_REFUSED model is not a deployable platform champion")
        return 1
    import cmlapi

    client = cmlapi.default_client()
    pid = os.environ["CDSW_PROJECT_ID"]
    _note("logging the MLflow run")
    run_id, experiment_id = log_winner(artifact)
    metrics = artifact.get("metrics") or {}
    tags = [{"key": k, "value": str(v)} for k, v in {
        "stage": "champion", "use_case": "uc01", "head": "pack_risk_eol",
        "winner": artifact.get("winner"), "schema_hash": artifact["schema_hash"][:12],
        "oof_auroc": f"{float(metrics.get('oof_auroc', 0)):.4f}",
    }.items()]
    _note("registering the champion")
    reg = register_version(client, name=REGISTRY_NAME, run_id=run_id, experiment_id=experiment_id,
                           project_id=pid, tags=tags, description="pack_risk_eol champion")
    _note(f"REGISTRY_RESULT name={REGISTRY_NAME} version={reg['number']} created={reg['created']}")
    comment = f"{artifact.get('winner')} schema {artifact['schema_hash'][:12]} auroc {float(metrics.get('oof_auroc', 0)):.3f}"
    _note("building the endpoint")
    deploy(client, pid, comment)
    return 0


if __name__ == "__main__":
    try:
        finish(main())
    except Exception as e:  # noqa: BLE001
        import traceback
        msg = f"DEPLOY_FAILED {type(e).__name__}: {e}\n{traceback.format_exc()[-1500:]}"
        print(msg, flush=True)
        try:
            with (ROOT / "evx_deploy.log").open("a") as f:
                f.write(msg + "\n")
        except OSError:
            pass
        finish(1)
