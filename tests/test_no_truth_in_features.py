"""Generator truth (ref.generator_truth_*, generator_fleet, _truth/) is for evaluation only: no feature,
silver, gold or serving code may read it."""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ALLOWED = {"cde/jobs/generate.py", "generators/run.py"}
SCANNED = ("cde", "mlcore", "usecases", "cai", "app", "airflow", "stream", "extract")
PATTERN = re.compile(r"generator_truth|generator_fleet|_truth/|truth_pack|truth_batch")


def test_no_truth_reads():
    offenders = []
    for top in SCANNED:
        for p in (ROOT / top).rglob("*.py") if (ROOT / top).is_dir() else []:
            rel = p.relative_to(ROOT).as_posix()
            if rel in ALLOWED or "/eval" in rel or rel.endswith("_eval.py"):
                continue
            if PATTERN.search(p.read_text()):
                offenders.append(rel)
    assert not offenders, offenders
