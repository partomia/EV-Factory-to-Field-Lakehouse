"""The nightly DAG discovers enabled use cases and stays on the 23:30 UTC window."""
from pathlib import Path

from cde.dags.evx_dag import END, SCHEDULE, START, enabled_use_cases


def test_uc01_is_the_only_enabled_use_case():
    assert enabled_use_cases() == ["uc01"]


def test_a_disabled_use_case_is_skipped(tmp_path: Path):
    root = tmp_path / "usecases"
    (root / "uc01_pack_risk").mkdir(parents=True)
    (root / "uc09_off").mkdir()
    (root / "uc01_pack_risk" / "config.yaml").write_text("use_case: uc01\nenabled: true\n")
    (root / "uc09_off" / "config.yaml").write_text("use_case: uc09\nenabled: false\n")
    assert enabled_use_cases(tmp_path) == ["uc01"]


def test_the_window_covers_the_five_live_dates():
    assert SCHEDULE == "30 23 * * *"
    assert START.isoformat() == "2026-11-05T23:30:00"
    assert END.isoformat() == "2026-11-09T23:30:00"
