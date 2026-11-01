"""
CDW Impala over HTTPS (impyla, LDAP), for laptop checks, CAI jobs and the app.
Needs EVX_IMPALA_USER / EVX_IMPALA_PASSWORD (.env on a laptop, the CAI project environment on CAI).

Metadata refresh on the shared warehouse is always table-scoped: an unscoped INVALIDATE METADATA hung the
client once and touches every sibling database.
"""
from __future__ import annotations

import os
import re

from common import lakehouse_config, load_env

UNSCOPED_INVALIDATE = re.compile(r"^\s*invalidate\s+metadata\s*;?\s*$", re.I)


def connect():
    from impala.dbapi import connect as _connect

    load_env()
    imp = lakehouse_config()["impala"]
    return _connect(host=os.environ.get("EVX_IMPALA_HOST", imp["host"]), port=int(imp["port"]), use_ssl=True,
                    use_http_transport=True, http_path=imp["http_path"], auth_mechanism=imp["auth_mechanism"],
                    user=os.environ["EVX_IMPALA_USER"], password=os.environ["EVX_IMPALA_PASSWORD"])


def query(conn, sql: str) -> tuple[list[str], list[tuple]]:
    if UNSCOPED_INVALIDATE.match(sql):
        raise ValueError("INVALIDATE METADATA must name a table on the shared warehouse")
    cur = conn.cursor()
    try:
        cur.execute(sql)
        cols = [d[0].split(".")[-1] for d in cur.description or []]
        return cols, (cur.fetchall() if cur.description else [])
    finally:
        cur.close()
