"""Vision workbench SQL helpers."""
from usecases.uc01_pack_risk import media_page as M


def test_xray_index_reads_semantic_view():
    sql = M.xray_index_sql("rsingh_evx_semantic", "FAIL")
    assert "media_xray" in sql
    assert "FAIL" in sql


def test_object_path_uses_bronze_catalog():
    sql = M.object_path_sql("rsingh_evx_bronze", "qc_xray", "CELL1.png")
    assert "doc_object" in sql
    assert "CELL1.png" in sql


def test_three_media_pages():
    pages = M.pages("g", "s", "b", "landing", "sv")
    assert [t for t, _ in pages] == ["Cell X-ray viewer", "Film defect gallery", "EDR event replay"]


def test_read_s3_logs_a_missing_boto3_and_returns_none(monkeypatch, caplog):
    import sys
    from app import media

    monkeypatch.setitem(sys.modules, "boto3", None)
    with caplog.at_level("WARNING", logger="evx.media"):
        assert media.read_s3("s3://bucket/qc_xray/2026-11-05/CELL1.png") is None
    assert "import boto3 failed: ModuleNotFoundError" in caplog.text


def test_read_s3_logs_the_error_class_and_code_only(monkeypatch, caplog):
    import sys
    import types
    from app import media

    class ClientError(Exception):
        def __init__(self):
            super().__init__("An error occurred (AccessDenied) when calling the GetObject operation: Access Denied")
            self.response = {"Error": {"Code": "AccessDenied"}}

    class Client:
        def get_object(self, **_kw):
            raise ClientError()

    monkeypatch.setitem(sys.modules, "boto3", types.SimpleNamespace(client=lambda _svc: Client()))
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "never-logged")
    with caplog.at_level("WARNING", logger="evx.media"):
        assert media.read_s3("s3a://bucket/key.png") is None
    assert "get_object failed: ClientError [AccessDenied]" in caplog.text
    assert "never-logged" not in caplog.text


def test_last_failure_is_set_for_a_missing_boto3_and_a_denied_get_object_and_cleared_on_success(monkeypatch):
    import sys
    import types
    from app import media

    monkeypatch.setitem(sys.modules, "boto3", None)
    media.read_s3("s3://bucket/a.png")
    assert media.last_failure() == "import boto3: ModuleNotFoundError"

    class ClientError(Exception):
        response = {"Error": {"Code": "AccessDenied"}}

    class Denied:
        def get_object(self, **_kw):
            raise ClientError("Access Denied for bucket")

    monkeypatch.setitem(sys.modules, "boto3", types.SimpleNamespace(client=lambda _svc: Denied()))
    media.read_s3("s3://bucket/a.png")
    assert media.last_failure() == "get_object: ClientError [AccessDenied]"

    class Body:
        def read(self):
            return b"png"

    class Ok:
        def get_object(self, **_kw):
            return {"Body": Body()}

    monkeypatch.setitem(sys.modules, "boto3", types.SimpleNamespace(client=lambda _svc: Ok()))
    assert media.read_s3("s3://bucket/a.png") == b"png"
    assert media.last_failure() is None


def test_vision_launcher_requires_boto3():
    from pathlib import Path

    src = (Path(__file__).resolve().parents[1] / "app" / "launch_vision_app.py").read_text()
    assert "import boto3" in src.split("def _ensure_app_packages")[0]
    assert '"boto3"]' in src


def test_s3_session_drops_the_static_key_providers():
    import pytest

    pytest.importorskip("boto3")
    from app import media

    methods = [p.METHOD for p in media.credential_session().get_component("credential_provider").providers]
    assert "env" not in methods and "shared-credentials-file" not in methods
    assert "sso" not in methods and "custom-process" not in methods
    assert {"assume-role-with-web-identity", "container-role", "iam-role"} <= set(methods)


def test_real_client_failure_does_not_log_the_env_key(monkeypatch, caplog):
    import pytest

    boto3 = pytest.importorskip("boto3")
    from botocore.stub import Stubber
    from app import media

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIATESTNOTAREALKEY1")
    client = media._s3_client(boto3)
    stub = Stubber(client)
    stub.add_client_error("get_object", service_error_code="InvalidAccessKeyId", http_status_code=403)
    stub.activate()
    monkeypatch.setattr(media, "_s3_client", lambda _b: client)
    with caplog.at_level("WARNING", logger="evx.media"):
        assert media.read_s3("s3://bucket/qc_xray/a.png") is None
    assert media.last_failure() == "get_object: ClientError [InvalidAccessKeyId]"
    assert "AKIATESTNOTAREALKEY1" not in caplog.text


import pytest  # noqa: E402


@pytest.fixture(autouse=True)
def _no_workload_user(monkeypatch):
    from app import media

    monkeypatch.delenv("EVX_WORKLOAD_USER", raising=False)
    monkeypatch.delenv("EVX_WORKLOAD_PASSWORD", raising=False)
    media._VENDED.update(creds=None, until=0.0)


class _Resp:
    def __init__(self, status, body=None):
        self.status_code, self._body = status, body or {}

    def json(self):
        return self._body


def _fake_boto3(calls):
    import types

    class Body:
        def read(self):
            return b"png"

    class Client:
        def get_object(self, **_kw):
            return {"Body": Body()}

    def client(service, **kw):
        calls.append((service, kw))
        return Client()

    return types.SimpleNamespace(client=client)


def test_idbroker_200_builds_the_client_and_logs_no_secret(monkeypatch, caplog):
    import sys

    import requests
    from app import media

    seen = {}

    def get(url, auth, timeout):
        seen.update(url=url, auth=auth, timeout=timeout)
        return _Resp(200, {"Credentials": {"AccessKeyId": "ASIAVENDED", "SecretAccessKey": "planted-secret",
                                           "SessionToken": "planted-token", "Expiration": 4102444800000}})

    calls = []
    monkeypatch.setitem(sys.modules, "boto3", _fake_boto3(calls))
    monkeypatch.setattr(requests, "get", get)
    monkeypatch.setenv("EVX_WORKLOAD_USER", "workload-user")
    monkeypatch.setenv("EVX_WORKLOAD_PASSWORD", "planted-password")
    with caplog.at_level("DEBUG", logger="evx.media"):
        assert media.read_s3("s3://bucket/a.png") == b"png"
        assert media.read_s3("s3://bucket/b.png") == b"png"
    assert seen["url"].endswith("/cab/api/v1/credentials") and seen["timeout"] == 20
    assert seen["auth"] == ("workload-user", "planted-password")
    assert calls[0] == ("s3", {"aws_access_key_id": "ASIAVENDED", "aws_secret_access_key": "planted-secret",
                               "aws_session_token": "planted-token"})
    assert media.last_failure() is None
    for secret in ("planted-password", "planted-secret", "planted-token"):
        assert secret not in caplog.text


def test_idbroker_403_sets_the_status_only(monkeypatch, caplog):
    import sys

    import requests
    from app import media

    monkeypatch.setitem(sys.modules, "boto3", _fake_boto3([]))
    monkeypatch.setattr(requests, "get", lambda url, auth, timeout: _Resp(403, {"error": "planted-body"}))
    monkeypatch.setenv("EVX_WORKLOAD_USER", "workload-user")
    monkeypatch.setenv("EVX_WORKLOAD_PASSWORD", "planted-password")
    with caplog.at_level("WARNING", logger="evx.media"):
        assert media.read_s3("s3://bucket/a.png") is None
    assert media.last_failure() == "idbroker: HTTP 403"
    assert "planted-body" not in caplog.text and "planted-password" not in caplog.text


def test_without_workload_user_the_filtered_role_session_is_used(monkeypatch):
    import sys
    import types

    import requests
    from app import media

    used = []
    monkeypatch.setattr(requests, "get", lambda *a, **k: pytest.fail("IDBroker called without a workload user"))
    monkeypatch.setattr(media, "credential_session", lambda: used.append("filtered") or "core")

    class Session:
        def __init__(self, botocore_session):
            used.append(botocore_session)

        def client(self, _svc):
            return _fake_boto3([]).client("s3")

    monkeypatch.setitem(sys.modules, "boto3", types.SimpleNamespace(Session=Session, client=None))
    assert media.read_s3("s3://bucket/a.png") == b"png"
    assert used == ["filtered", "core"]
