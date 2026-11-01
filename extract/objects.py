"""
Object contracts (contracts/objects/<format>.json): what a valid object of each format is. validate() is what
bronze runs on every landed object; a failure goes to bronze.quarantine with a reason code and is never extracted.
Standard library only (runs on executors). Adapted from OGX extract/objects.py.

  format_of(file name) -> pdf | png | jdx | edr | None
  validate(fmt, data, contract) -> (ok, reason_code, facts)
"""
from __future__ import annotations

EXT = {"pdf": "pdf", "png": "png", "jdx": "jdx", "dx": "jdx", "edr": "edr"}
MIME = {"pdf": "application/pdf", "png": "image/png", "jdx": "chemical/x-jcamp-dx",
        "edr": "application/x-evx-edr"}


def format_of(name: str) -> str | None:
    return EXT.get(name.rsplit(".", 1)[-1].lower()) if "." in name else None


def validate(fmt: str, data: bytes, contract: dict) -> tuple[bool, str | None, dict]:
    if len(data) < contract.get("min_bytes", 1):
        return False, "TOO_SMALL", {"size": len(data)}
    try:
        if fmt == "pdf":
            from extract.pdf_text import PdfError, parse

            try:
                doc = parse(data)
            except PdfError as e:
                return False, "PDF_TRUNCATED" if "truncated" in str(e) else "PDF_UNREADABLE", {"error": str(e)}
            if doc["page_count"] < contract.get("min_pages", 1):
                return False, "PDF_NO_PAGES", {}
            text_pages = sum(1 for p in doc["pages"] if p["text"].strip())
            return True, None, {"pages": doc["page_count"], "text_pages": text_pages}
        if fmt == "png":
            from extract.png import PngError, png_decode

            try:
                w, h, _ = png_decode(data)
            except PngError as e:
                return False, "PNG_TRUNCATED" if "truncated" in str(e) else "PNG_UNDECODABLE", {"error": str(e)}
            return True, None, {"width": w, "height": h}
        if fmt == "jdx":
            from extract.jcamp import JcampError, parse

            try:
                s = parse(data.decode("ascii", errors="replace"))
            except JcampError as e:
                return False, "JCAMP_INVALID", {"error": str(e)}
            if len(s["x"]) < contract.get("min_points", 1):
                return False, "JCAMP_TOO_FEW_POINTS", {"points": len(s["x"])}
            return True, None, {"points": len(s["x"]), "lot": s["header"].get("$LOT")}
        if fmt == "edr":
            from extract.edr import EdrError, parse

            try:
                e = parse(data, samples=False)
            except EdrError as err:
                return False, "EDR_INVALID", {"error": str(err)}
            return True, None, {k: e[k] for k in ("vin", "event_id", "trigger", "hz", "n_pre", "n_post")}
    except Exception as e:  # a parser crash is an invalid object, never a failed batch
        return False, "UNREADABLE", {"error": f"{type(e).__name__}: {e}"[:300]}
    return False, "UNKNOWN_FORMAT", {}
