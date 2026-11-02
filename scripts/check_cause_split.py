"""Each planted cause has at least one positive pack in TRAIN and in TEST.

Reads the split written by train.py and the generator truth (evaluation only; the model never
sees it). A cause wholly on one side means the model cannot learn it, or TEST cannot show it.

  .venv/bin/python scripts/check_cause_split.py outputs/uc01/pack_risk_eol/split.csv
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def coverage(rows: list[tuple[str, str]], splits: dict[str, str]) -> list[str]:
    """rows of (pack_serial, cause) for positive packs. Causes missing from one side."""
    seen: dict[str, set[str]] = {}
    for pack, cause in rows:
        side = splits.get(pack)
        if cause and side:
            seen.setdefault(cause, set()).add(side)
    return sorted(c for c, sides in seen.items() if sides != {"train", "test"})


def main(argv: list[str]) -> int:
    import pandas as pd

    from lakehouse.impala import connect, query

    split = pd.read_csv(argv[0], dtype=str)
    sides = dict(zip(split["pack_serial"], split["split"]))
    conn = connect()
    query(conn, "INVALIDATE METADATA rsingh_evx_ref.generator_truth_pack")
    _, rows = query(conn, "SELECT pack_id, event_cause FROM rsingh_evx_ref.generator_truth_pack "
                          "WHERE positive AND event_cause IS NOT NULL")
    missing = coverage([(p, c) for p, c in rows], sides)
    print(f"CAUSE_SPLIT {'OK' if not missing else 'FAIL'} causes_one_sided={missing or 'none'}", flush=True)
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
