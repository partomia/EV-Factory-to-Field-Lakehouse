"""
Run SQL statements on CDW Impala from a laptop and print the results as tab-separated rows.

  .venv/bin/python scripts/sql.py "REFRESH rsingh_evx_ref.load_audit" "SELECT count(*) FROM rsingh_evx_ref.load_audit"
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lakehouse.impala import connect, query  # noqa: E402


def main(argv: list[str]) -> int:
    conn = connect()
    for sql in argv:
        cols, rows = query(conn, sql)
        print(f"-- {sql[:120]}")
        if cols:
            print("\t".join(cols))
            for r in rows:
                print("\t".join("" if v is None else str(v) for v in r))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
