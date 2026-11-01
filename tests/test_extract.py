"""Format parsers and object validation (standard library; what bronze and extract run on executors)."""
import json
import random
from types import SimpleNamespace
from pathlib import Path

import pytest

from extract.edr import EdrError, parse as parse_edr
from extract.jcamp import JcampError, parse as parse_jcamp
from extract.metrics import film_metrics, xray_metrics, xrf_peaks
from extract.objects import validate
from extract.png import PngError, png_decode
from extract.records import delimited_rows, ndjson_rows, odata_page
from generators.docs import edr_dump, jcamp, text_pdf, xrf_spectrum
from generators.factory import film_png, xray_png
from generators.formats import png_gray

ROOT = Path(__file__).resolve().parents[1]


def contract(name):
    return json.loads((ROOT / "contracts" / "sources" / f"{name}.json").read_text())


def test_png_roundtrip_and_truncation():
    px = bytes(range(256)) * 16
    data = png_gray(64, 64, px)
    w, h, out = png_decode(data)
    assert (w, h, out) == (64, 64, px)
    with pytest.raises(PngError, match="truncated"):
        png_decode(data[: len(data) // 3])
    ok, reason, _ = validate("png", data[: len(data) // 3], {"min_bytes": 10})
    assert (ok, reason) == (False, "PNG_TRUNCATED")


def test_edr_roundtrip():
    samples = [[float(i), 1.0, 3.5, 3.6, 30.0, 40.0, 25.0, 80.0] for i in range(400)]
    data = edr_dump("XEVS1K26090100001", "EDR0901", "thermal", 1_790_000_000_000, samples)
    e = parse_edr(data)
    assert e["vin"] == "XEVS1K26090100001" and e["trigger"] == "thermal"
    assert (e["n_pre"], e["n_post"], len(e["samples"])) == (300, 100, 400)
    with pytest.raises(EdrError):
        parse_edr(data[:-4])


def test_jcamp_roundtrip():
    xs, ys = xrf_spectrum(random.Random(1), contaminated=True)
    s = parse_jcamp(jcamp("XRF", "CATHCL10001", xs, ys))
    assert len(s["x"]) == 300 and s["header"]["$LOT"] == "CATHCL10001"
    with pytest.raises(JcampError):
        parse_jcamp("##TITLE=x\n##XYDATA=(XY..XY)\n1,2\n")


def test_pdf_validation_reports_text_layer():
    ok, _, facts = validate("pdf", text_pdf("Job card", [("VIN", "X")], []), {"min_pages": 1})
    assert ok and facts["text_pages"] == 1


def test_trailer_is_read_and_compared():
    text = "CELL_ID|BATCH_ID|LINE|STEP|STATION|START_TS|END_TS|RESULT|PARAMS\n" \
           "CBCL1260914B0000|CBCL1260914B|CL1|formation|FM|t0|t1|OK|x=1\nTRL|2\n"
    rows, stats = delimited_rows("cell_mes/2026-09-15/f.csv", text, contract("cell_mes_step"))
    assert len(rows) == 1 and rows[0][2] == "CBCL1260914B0000"
    assert (stats["data_records"], stats["trailer_records"], stats["header_ok"]) == (1, 2, True)


def test_ndjson_fields_by_path_and_malformed_line():
    doc = {"pack_serial": "PKPL12609100001", "pack_line": "PL1", "build_ts": "2026-09-10T05:27:00+05:30",
           "shift": "A", "eol": {"result": "PASS"}}
    rows = ndjson_rows("f", json.dumps(doc) + "\n{bad\n", contract("pack_build_doc"))
    assert rows[0][2:7] == ("PKPL12609100001", "PL1", "2026-09-10T05:27:00+05:30", "A", "PASS")
    assert rows[1][-1].startswith("MALFORMED_JSON")


def test_odata_page_keys_token_and_fields():
    page = {"d": {"results": [{"MBLNR": "49", "ZEILE": "0001", "ZZ_NEW": "1"}],
                  "__delta": "A_MaterialDocument?!deltatoken='D20261106MATE'"}}
    p = odata_page("sap_odata/2026-11-07/A_MaterialDocument_resent_001.json", json.dumps(page),
                   contract("sap_odata_record"))
    assert p["entity_set"] == "A_MaterialDocument" and p["page"] == "001"
    assert p["rows"][0][3] == "49|0001" and "ZZ_NEW" in p["fields"]
    assert p["delta_token"].endswith("'D20261106MATE'")


@pytest.mark.parametrize("overhang,blob", [(0.2, False), (0.75, True), (1.4, True)])
def test_xray_overhang_blob(overhang, blob):
    data, bbox = xray_png(SimpleNamespace(overhang=overhang), random.Random(7))
    m = xray_metrics(*png_decode(data))
    assert (m["overhang_px"] > 0) == blob == bool(bbox)
    if blob:
        assert m["overhang_box"] == bbox


def test_film_patch_and_streaks():
    clean = film_metrics(*png_decode(film_png("S1", 0.1, random.Random(3))))
    bad = film_metrics(*png_decode(film_png("S2", 0.9, random.Random(3))))
    assert not clean["defect_patch"] and bad["defect_patch"]
    assert bad["streak_cols"] > clean["streak_cols"]


def test_xrf_fe_peak_separates_contaminated_lots():
    clean = xrf_peaks(*xrf_spectrum(random.Random(2), contaminated=False))
    dirty = xrf_peaks(*xrf_spectrum(random.Random(2), contaminated=True))
    assert dirty["fe_ka"] > 4 * clean["fe_ka"]
