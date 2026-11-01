"""The synthetic source systems: deterministic, shaped like the real formats, faults planted where configured."""
import csv
import hashlib
import json
import struct

from generators.core import build_plan, partitions
from generators.run import label_stats, partition_records


def _digest(cfg, key):
    h = hashlib.sha256()
    for rec in partition_records(cfg, key):
        h.update(repr(rec[:2]).encode())
        if rec[0] == "file":
            h.update(rec[2])
    return h.hexdigest()


def test_deterministic(ci_cfg):
    cfg, _ = ci_cfg
    assert _digest(cfg, ("CL2", "2026-04")) == _digest(cfg, ("CL2", "2026-04"))


def test_partitions_cover_the_calendar(ci_cfg):
    cfg, keys = ci_cfg
    plan = build_plan(cfg, cfg["_scale"])
    assert keys[0] == ("SUPPLY", "-")
    assert {k[0] for k in partitions(plan)} == {"CL1", "CL2"}
    assert sum(b.contaminated for b in plan.batches) > 0
    assert sum(b.drift > 0 for b in plan.batches) > 0


def test_mes_trailer_fault_and_counts(generated):
    out, _ = generated
    bad = out / "cell_mes/2026-09-15/CL1_formation_B.csv"
    lines = bad.read_text().splitlines()
    assert lines[-1] == f"TRL|{len(lines) - 2 + 1}"
    good = out / "cell_mes/2026-09-15/CL1_formation_A.csv"
    lines = good.read_text().splitlines()
    assert lines[-1] == f"TRL|{len(lines) - 2}"


def test_pack_and_vehicle_records(generated):
    out, _ = generated
    files = sorted((out / "pack_mes").glob("2026-09-1*/*.ndjson"))
    assert files
    pack = json.loads(files[0].read_text().splitlines()[0])
    assert pack["synthetic"] is True and pack["config"] == "13s4p"
    assert len(pack["cells"]) == 52
    veh = json.loads(next((out / "vehicle_eol").glob("*/*.ndjson")).read_text().splitlines()[0])
    assert len(veh["vin"]) == 17


def test_binary_formats(generated):
    out, _ = generated
    png = next((out / "qc_xray").glob("*/*.png")).read_bytes()
    assert png[:8] == b"\x89PNG\r\n\x1a\n"
    pdf = next((out / "dms_jobcards").glob("*/*.pdf")).read_bytes()
    assert pdf[:5] == b"%PDF-"
    edr = next((out / "edr").glob("*/*.edr")).read_bytes()
    assert edr[:8] == b"EVXEDR1\0"
    ver, vin, *_ = struct.unpack_from("<H17s24sBqHIIH", edr, 8)
    assert len(vin.decode()) == 17
    jdx = next((out / "spectra").glob("*/*.jdx")).read_text()
    assert jdx.startswith("##TITLE=") and "##END=" in jdx


def test_odata_pages_and_planted_supply_faults(generated):
    out, _ = generated
    page = json.loads((out / "sap_odata/2026-11-08/A_InspectionLot_001.json").read_text())
    rows = page["d"]["results"]
    assert rows and all("ZZ_MOISTURE_PPM" in r for r in rows)
    before = json.loads(next((out / "sap_odata").glob("2026-11-0[1-5]/A_InspectionLot_001.json")).read_text())
    assert all("ZZ_MOISTURE_PPM" not in r for r in before["d"]["results"])
    assert list((out / "sap_odata/2026-11-07").glob("A_MaterialDocument_resent_*.json"))


def test_live_date_faults(generated):
    out, _ = generated
    assert list((out / "edr/2026-11-09").glob("*.edr")), "late EDR arrives on 2026-11-09"
    corrupt = [p.parent.name for p in (out / "qc_xray").glob("*/*.png") if b"IEND" not in p.read_bytes()]
    assert corrupt == ["2026-11-08"], "one truncated X-ray PNG on the planted live date"


def test_tabular_exports_are_strings_with_header(generated):
    out, _ = generated
    f = next((out / "telematics_daily").glob("business_date=*/part-00000.csv"))
    rows = list(csv.reader(f.open()))
    assert rows[0][0] == "VIN" and len(rows) > 1
    assert len({len(r) for r in rows}) == 1


def test_manifests_count_what_landed(generated):
    out, _ = generated
    for m in list((out / "cell_mes").glob("*/_manifest.json"))[:5]:
        body = json.loads(m.read_text())
        assert body["synthetic"] is True
        assert body["objects"] == len([p for p in m.parent.iterdir() if p.name != "_manifest.json"])


def test_label_stats_on_matured_packs():
    packs = [
        {"delivery_date": "2026-05-01", "positive": True, "event_day": 30, "event_cause": "drift"},
        {"delivery_date": "2026-05-01", "positive": True, "event_day": 120, "event_cause": "usage"},
        {"delivery_date": "2026-05-02", "positive": False, "event_day": None, "event_cause": None},
        {"delivery_date": "2026-10-01", "positive": True, "event_day": 5, "event_cause": "overhang"},
    ]
    s = label_stats(packs)
    assert (s["matured"], s["positives"], s["by_cause"]["drift"]) == (3, 1, 1)
