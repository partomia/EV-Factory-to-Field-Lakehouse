import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "cde" / "jobs"))

pytest.importorskip("pyspark")
import build_silver  # noqa: E402
import evx_common as C  # noqa: E402
import failed_batch_drill as D  # noqa: E402


def test_tables_in_build_silver_write_order():
    tables = D.silver_tables(build_silver)
    assert tables[:7] == list(build_silver.TYPED.values())
    assert tables[7:10] == ["pack_build", "pack_cell", "vehicle_build"]
    assert "sap_inspection_lot" in tables and tables[-3:] == ["service_job", "supplier_coa", "quality_report"]
    assert len(tables) == len(set(tables))


def test_compare_flags_rows_and_kept_snapshots():
    clean = {"a": {"rows": 3, "fingerprint": 7, "snapshot": 1, "files": 2},
             "b": {"rows": 5, "fingerprint": 9, "snapshot": 4, "files": 1}}
    same_rows_new_snapshot = {"a": dict(clean["a"], snapshot=2), "b": dict(clean["b"])}
    assert D.compare(clean, same_rows_new_snapshot, {"b"}) == []
    assert D.compare(clean, same_rows_new_snapshot, {"a"})
    partial = {"a": dict(clean["a"]), "b": dict(clean["b"], rows=2, fingerprint=1)}
    assert D.compare(clean, partial, set())


def test_failing_write_passes_other_tables_through():
    calls = []
    w = D.failing_write("rsingh_evx_silver.sap_inspection_lot", lambda *a: calls.append(a[2]))
    w(None, None, "rsingh_evx_silver.cell_step", ["2026-11-06"])
    assert calls == ["rsingh_evx_silver.cell_step"]


def test_injected_task_fails_on_one_partition_only():
    assert C.fail_on_partition(0) == 0
    with pytest.raises(RuntimeError):
        C.fail_on_partition(1)


def test_guard():
    with pytest.raises(ValueError):
        D.guard("default.cell_step")
