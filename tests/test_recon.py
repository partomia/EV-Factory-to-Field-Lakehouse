import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "cde" / "jobs"))

from recon import classify  # noqa: E402

EXPECTED = json.loads((ROOT / "config" / "expected_recon.json").read_text())["faults"]


def test_listed_exception_takes_its_outcome():
    out = classify([("2026-09-15", "bronze", "cell_mes_step", "trailer count", 1, "f.csv")], EXPECTED, {"M1"},
                   {"2026-09-15"})
    assert out == [("2026-09-15", "bronze", "cell_mes_step", "trailer count", 1, "MISMATCH", 2, "f.csv")]


def test_unlisted_exception_and_missing_fault():
    out = classify([("2026-09-14", "bronze", "cell_mes_step", "trailer count", 1, "f.csv")], EXPECTED, {"M1"},
                   {"2026-09-14", "2026-09-15"})
    assert ("2026-09-14", "bronze", "cell_mes_step", "trailer count", 1, "UNEXPECTED", None, "f.csv") in out
    assert [r[5:7] for r in out if r[0] == "2026-09-15"] == [("MISSING", 2)]


def test_unbuilt_milestone_is_not_missing():
    out = classify([], EXPECTED, {"M1"}, {"2026-11-09"})
    assert out == []


def test_expected_rows_are_well_formed():
    ids = [e["id"] for e in EXPECTED]
    assert ids == sorted(set(ids))
    assert all(e["outcome"] in ("EXPLAINED", "MISMATCH") and e["milestone"] in ("M1", "M2") for e in EXPECTED)
