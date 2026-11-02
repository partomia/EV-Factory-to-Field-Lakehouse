"""Register one version of evx-uc01-pack-risk-eol in the Cloudera AI Registry.

A version is created from an MLflow run. A second call with the same run id returns the
version already created, so a deploy retry does not register a duplicate. Adapted from
CXR and OGX serve/registry.py.
"""
from __future__ import annotations

REGISTRY_NAME = "evx-uc01-pack-risk-eol"


def _get(obj, key, default=None):
    return obj.get(key, default) if isinstance(obj, dict) else getattr(obj, key, default)


def _run_id(version) -> str | None:
    md = _get(version, "model_version_metadata") or {}
    return _get(_get(md, "mlflow_metadata") or {}, "run_id")


def find_model(client, name: str):
    models = _get(client.list_registered_models(page_size=100), "models") or []
    return next((m for m in models if _get(m, "name") == name), None)


def versions(client, name: str) -> list:
    model = find_model(client, name)
    if model is None:
        return []
    return _get(client.get_registered_model(_get(model, "model_id")), "model_versions") or []


def register_version(client, *, name: str, run_id: str, experiment_id: str, project_id: str,
                     tags: list[dict], description: str = "") -> dict:
    """{model_id, version, number, previous, created}. created is False when this run is already registered."""
    before = list(versions(client, name))
    for v in before:
        if _run_id(v) == run_id:
            number = _get(v, "number")
            prev = max((_get(o, "number") or 0 for o in before if _get(o, "number") != number), default=None)
            print(f"registry: {name} version {number} already holds run {run_id}", flush=True)
            return {"model_id": _get(find_model(client, name), "model_id"), "version": _get(v, "model_version_id"),
                    "number": number, "previous": prev, "created": False}
    created = client.create_registered_model({
        "project_id": project_id, "experiment_id": experiment_id, "run_id": run_id,
        "model_path": "model", "model_name": name, "tags": tags, "description": description,
        "notes": "champion: gate passed", "visibility": "PRIVATE"})
    model_id = _get(created, "model_id")
    after = _get(client.get_registered_model(model_id), "model_versions") or []
    new = max(after, key=lambda v: _get(v, "number") or 0)
    prev = max((_get(v, "number") or 0 for v in before), default=None)
    number = _get(new, "number")
    print(f"registry: {name} version {number} (previous {prev})", flush=True)
    return {"model_id": model_id, "version": _get(new, "model_version_id"), "number": number,
            "previous": prev, "created": True}
