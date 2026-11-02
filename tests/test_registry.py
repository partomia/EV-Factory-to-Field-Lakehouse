"""The registry records one version per MLflow run."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from mlcore.registry import register_version  # noqa: E402


class Version:
    def __init__(self, number, run_id):
        self.number = number
        self.model_version_id = f"mv{number}"
        self.model_version_metadata = {"mlflow_metadata": {"run_id": run_id}}


class Client:
    def __init__(self):
        self.created = 0
        self._versions = []
        self.model = {"name": "evx-uc01-pack-risk-eol", "model_id": "m1"}

    def list_registered_models(self, page_size=100):
        return {"models": [self.model] if self._versions or self.created else []}

    def get_registered_model(self, model_id):
        return {"model_id": model_id, "model_versions": self._versions}

    def create_registered_model(self, body):
        self.created += 1
        self._versions.append(Version(self.created, body["run_id"]))
        return {"model_id": "m1"}


def test_the_same_run_is_not_registered_twice():
    client = Client()
    kwargs = dict(name="evx-uc01-pack-risk-eol", run_id="run-a", experiment_id="e", project_id="p", tags=[])
    first = register_version(client, **kwargs)
    second = register_version(client, **kwargs)
    assert first["created"] is True and first["number"] == 1
    assert second["created"] is False and second["number"] == 1
    assert client.created == 1


def test_a_new_run_is_a_new_version():
    client = Client()
    register_version(client, name="evx-uc01-pack-risk-eol", run_id="run-a", experiment_id="e", project_id="p", tags=[])
    nxt = register_version(client, name="evx-uc01-pack-risk-eol", run_id="run-b", experiment_id="e", project_id="p", tags=[])
    assert nxt["number"] == 2 and nxt["previous"] == 1 and nxt["created"] is True
