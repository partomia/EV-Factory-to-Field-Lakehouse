"""
EVX Quality & Reliability Workbench (Streamlit on Cloudera AI).

Pages come from every usecases/*/app_page.py. Figures are read from CDW Impala.
The brand is EVX_BRAND_NAME when the project environment sets it.

Needs EVX_IMPALA_USER / EVX_IMPALA_PASSWORD (the CAI project environment, or .env on a laptop).
"""
from __future__ import annotations

import os
import sys
import threading
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.discover import discover  # noqa: E402
from common import lakehouse_config, load_env  # noqa: E402

load_env()
CFG = lakehouse_config()
PREFIX = CFG.get("db_prefix", "rsingh_evx")
GOLD, SEMANTIC = f"{PREFIX}_gold", f"{PREFIX}_semantic"

_CONN = threading.local()


def connection(fresh: bool = False):
    from impala.dbapi import connect

    if fresh or getattr(_CONN, "c", None) is None:
        old = getattr(_CONN, "c", None)
        if old is not None:
            try:
                old.close()
            except Exception:  # noqa: BLE001
                pass
        imp = CFG["impala"]
        _CONN.c = connect(host=os.environ.get("EVX_IMPALA_HOST", imp["host"]), port=int(imp["port"]),
                          use_ssl=True, use_http_transport=True, http_path=imp["http_path"],
                          auth_mechanism=imp["auth_mechanism"],
                          user=os.environ["EVX_IMPALA_USER"], password=os.environ["EVX_IMPALA_PASSWORD"],
                          timeout=180)
    return _CONN.c


def _run(sql: str, fresh: bool) -> pd.DataFrame:
    cur = connection(fresh).cursor()
    try:
        cur.execute(sql)
        cols = [d[0].split(".")[-1] for d in cur.description or []]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        try:
            cur.close()
        except Exception:  # noqa: BLE001
            pass


@st.cache_data(ttl=300, show_spinner=False)
def q(sql: str) -> pd.DataFrame:
    try:
        return _run(sql, fresh=False)
    except Exception:  # noqa: BLE001  a closed Impala session: reconnect once
        return _run(sql, fresh=True)


def main() -> None:
    st.set_page_config(page_title="EVX Quality & Reliability Workbench", layout="wide")
    brand = os.environ.get("EVX_BRAND_NAME") or CFG.get("brand_default") or "EVX"
    st.title(f"{brand} Quality & Reliability Workbench")
    st.caption("Priorities for a person. No row carries an automated action.")

    if not os.environ.get("EVX_IMPALA_USER") or not os.environ.get("EVX_IMPALA_PASSWORD"):
        st.error("Set EVX_IMPALA_USER and EVX_IMPALA_PASSWORD in the CAI project environment.")
        st.stop()

    found = discover(ROOT, GOLD, SEMANTIC)
    if not found:
        st.info("No use-case pages found under usecases/*/app_page.py.")
        st.stop()

    choice = st.sidebar.radio("Page", [title for title, _ in found])
    for title, render in found:
        if title == choice:
            render(q)


if __name__ == "__main__":
    main()
