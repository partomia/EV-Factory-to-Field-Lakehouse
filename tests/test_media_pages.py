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
