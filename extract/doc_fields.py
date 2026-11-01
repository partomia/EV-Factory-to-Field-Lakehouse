"""
Fields of the synthetic business documents (job cards, CoA, 8D and teardown reports) from a PDF text layer or,
for image-only pages, from OCR. Every document has the same layout (generators/docs.py): banner, title, then
`LABEL: value` lines, then `SECTION:` headings with free text. Standard library only.

  read(pdf bytes) -> {"method": TEXT | OCR | NONE, "title", "fields": {label: value}, "sections": {head: text},
                      "pages", "text_pages"}
"""
from __future__ import annotations

import re

from extract.pdf_text import parse

BANNER = "SYNTHETIC DEMO DOCUMENT"
FIELD_RE = re.compile(r"^([A-Z][A-Z0-9 ()%/.-]{1,40}):\s+(\S.*)$")
SECTION_RE = re.compile(r"^([A-Z][A-Z0-9 ()/-]{1,40}):\s*$")


def _lines(doc: dict) -> tuple[list[str], str]:
    text = "\n".join(p["text"] for p in doc["pages"])
    if text.strip():
        return text.splitlines(), "TEXT"
    from extract.ocr import ocr_page

    out = []
    for p in doc["pages"]:
        if p["image"]:
            out += ocr_page(p["image"]).splitlines()
    return out, ("OCR" if any(ln.strip() for ln in out) else "NONE")


def fields_of(lines: list[str]) -> dict:
    lines = [" ".join(ln.split()) for ln in lines]
    lines = [ln for ln in lines if ln and BANNER not in ln.upper()]
    title = lines[0].upper() if lines else None
    fields, sections, head = {}, {}, None
    for ln in lines[1:]:
        up = ln.upper()
        m = SECTION_RE.match(up)
        if m:
            head = m.group(1).strip().lower().replace(" ", "_")
            sections[head] = ""
            continue
        if head is None:
            m = FIELD_RE.match(ln) or FIELD_RE.match(up)
            if m:
                fields[m.group(1).strip().lower().replace(" ", "_")] = m.group(2).strip()
                continue
        if head:
            sections[head] = (sections[head] + " " + ln).strip()
    return {"title": title, "fields": fields, "sections": sections}


def read(data: bytes) -> dict:
    doc = parse(data)
    lines, method = _lines(doc)
    out = fields_of(lines)
    out.update(method=method, pages=doc["page_count"], text_pages=sum(1 for p in doc["pages"] if p["text"].strip()))
    return out
