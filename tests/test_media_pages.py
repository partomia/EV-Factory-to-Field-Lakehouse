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
