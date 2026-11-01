"""
Record parsers for bronze, one landed file at a time (executors call them through wholeTextFiles).
Every row comes back as (source_file, source_row, *contract columns as strings), so the bronze job validates
all formats the same way. Standard library only.

  delimited  header row mapped to contract columns by name (case-insensitive); optional trailer `TRL|<n>`
  ndjson     one JSON document per line; contract columns read by dotted path, document kept as payload
  odata      OData v2 page {"d": {"results": [...], "__delta": ...}}; entity set from the file name
"""
from __future__ import annotations

import csv
import io
import json
import re


def _cols(contract: dict) -> list[str]:
    return [c["name"] for c in contract["columns"]]


def delimited_rows(path: str, text: str, contract: dict) -> tuple[list[tuple], dict]:
    """Rows plus the file's control facts (header ok, data rows, trailer count)."""
    delim = "|" if contract["format"] == "psv" else ","
    trailer = contract.get("trailer")
    lines = text.splitlines()
    trailer_n = None
    if trailer and lines and lines[-1].startswith(trailer + "|"):
        try:
            trailer_n = int(lines[-1].split("|", 1)[1])
        except ValueError:
            trailer_n = -1
        lines = lines[:-1]
    reader = csv.reader(io.StringIO("\n".join(lines)), delimiter=delim)
    header = [h.strip().lower() for h in next(reader, [])]
    cols = _cols(contract)
    idx = [header.index(c) if c in header else None for c in cols]
    rows = []
    for i, rec in enumerate(reader, start=2):
        if not rec:
            continue
        rows.append((path, i, *[(rec[j] if j is not None and j < len(rec) else None) for j in idx]))
    stats = {"file": path, "header_ok": all(j is not None for j in idx), "data_records": len(rows),
             "trailer_records": trailer_n, "has_trailer": trailer_n is not None}
    return rows, stats


def _get(doc, path: str):
    for part in path.split("."):
        if not isinstance(doc, dict):
            return None
        doc = doc.get(part)
    return doc


def ndjson_rows(path: str, text: str, contract: dict) -> list[tuple]:
    """(file, row, *columns, payload, parse_error)."""
    out = []
    paths = [c.get("json", c["name"]) for c in contract["columns"]]
    for i, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            doc = json.loads(line)
            vals = [None if (v := _get(doc, p)) is None else str(v) for p in paths]
            out.append((path, i, *vals, line, None))
        except ValueError as e:
            out.append((path, i, *[None] * len(paths), line, f"MALFORMED_JSON: {str(e)[:120]}"))
    return out


PAGE_RE = re.compile(r"(A_[A-Za-z]+?)(_resent)?_(\d{3})\.json$")


def odata_page(path: str, text: str, contract: dict) -> dict:
    """{entity_set, resent, page, delta_token, rows: [(file, row, entity_set, record_key, delta, payload)],
    fields: set of field names seen, error}."""
    name = path.rsplit("/", 1)[-1]
    m = PAGE_RE.search(name)
    entity = m.group(1) if m else None
    out = {"file": path, "entity_set": entity, "resent_name": bool(m and m.group(2)), "page": m.group(3) if m else None,
           "delta_token": None, "rows": [], "fields": [], "error": None}
    if not entity:
        out["error"] = "UNKNOWN_ENTITY_SET"
        return out
    try:
        d = json.loads(text)["d"]
    except (ValueError, KeyError, TypeError) as e:
        out["error"] = f"MALFORMED_PAGE: {str(e)[:120]}"
        return out
    out["delta_token"] = d.get("__delta") or d.get("__next")
    keys = contract["entity_keys"].get(entity, [])
    fields = set()
    for i, rec in enumerate(d.get("results", []), start=1):
        fields.update(rec)
        rk = "|".join(str(rec.get(k, "")) for k in keys) if keys else None
        out["rows"].append((path, i, entity, rk, out["delta_token"], json.dumps(rec, sort_keys=True)))
    out["fields"] = sorted(fields)
    return out
