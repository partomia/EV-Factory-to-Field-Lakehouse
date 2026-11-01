"""
Synthetic business documents as PDF (text layer) or as a scanned page (image only, OCR needed), plus the EDR
binary dump and the JCAMP-DX spectrum. Standard library only. Every document carries a SYNTHETIC banner.

Document layout: a title, then `LABEL: value` lines, then free-text sections. extract/doc_fields.py reads the
same labels back from the text layer or from OCR.
"""
from __future__ import annotations

import math
import struct

from generators.formats import PDF, scan_lines

BANNER = "SYNTHETIC DEMO DOCUMENT - NOT REAL DATA"


def _wrap(text: str, width: int = 88) -> list[str]:
    out, cur = [], ""
    for word in text.split():
        if len(cur) + len(word) + 1 > width:
            out.append(cur)
            cur = word
        else:
            cur = f"{cur} {word}".strip()
    if cur:
        out.append(cur)
    return out


def doc_lines(title: str, fields: list[tuple[str, str]], sections: list[tuple[str, str]]) -> list[list[str]]:
    """Pages of monospace-friendly lines (about 52 lines per page)."""
    lines = [BANNER, "", title.upper(), ""]
    lines += [f"{k.upper()}: {v}" for k, v in fields]
    for head, body in sections:
        lines += ["", f"{head.upper()}:"] + _wrap(body)
    pages, per = [], 52
    for i in range(0, len(lines), per):
        pages.append(lines[i:i + per])
    return pages


def text_pdf(title: str, fields, sections, info: dict | None = None) -> bytes:
    pdf = PDF(info={"Title": title, "Producer": "evx-synthetic", **(info or {})})
    for page in doc_lines(title, fields, sections):
        y, rows = 750.0, []
        for ln in page:
            rows.append((50.0, y, 10.0, ln))
            y -= 13.5
        pdf.text_page(rows)
    return pdf.tobytes()


def scanned_pdf(title: str, fields, sections, seed: int, info: dict | None = None) -> bytes:
    pdf = PDF(info={"Title": title, "Producer": "evx-synthetic-scanner", **(info or {})})
    for page in doc_lines(title, fields, sections):
        w, h = 1100, 1424
        pdf.image_page(w, h, scan_lines(page, w, h, seed=seed))
    return pdf.tobytes()


# ---------------------------------------------------------------- EDR binary dump

EDR_MAGIC = b"EVXEDR1\x00"
EDR_CHANNELS = ("pack_v", "pack_i", "cell_v_min", "cell_v_max", "cell_t_max", "mcu_t", "speed_kph", "soc")
TRIGGERS = {"thermal": 1, "fault": 2, "crash": 3}


def edr_dump(vin: str, event_id: str, trigger: str, trigger_ms: int, samples: list[list[float]],
             hz: int = 10, n_pre: int = 300) -> bytes:
    """Header (little-endian) + float32 samples, channel-interleaved:
    magic[8] ver:u16 vin[17] event_id[24] trigger:u8 trigger_ms:i64 hz:u16 n_pre:u32 n_post:u32 n_ch:u16
    channel names (n_ch x 12 bytes) then n_samples x n_ch float32."""
    n = len(samples)
    hdr = EDR_MAGIC + struct.pack("<H17s24sBqHIIH", 1, vin.encode()[:17].ljust(17, b" "),
                                  event_id.encode()[:24].ljust(24, b" "), TRIGGERS[trigger], trigger_ms,
                                  hz, n_pre, n - n_pre, len(EDR_CHANNELS))
    hdr += b"".join(c.encode().ljust(12, b"\x00") for c in EDR_CHANNELS)
    body = b"".join(struct.pack(f"<{len(EDR_CHANNELS)}f", *row) for row in samples)
    return hdr + body


# ---------------------------------------------------------------- JCAMP-DX spectrum


def jcamp(title: str, lot: str, xs: list[float], ys: list[float]) -> str:
    head = [f"##TITLE={title}", "##JCAMP-DX=4.24", "##DATA TYPE=X-RAY FLUORESCENCE SPECTRUM",
            f"##ORIGIN={BANNER}", f"##$LOT={lot}", "##XUNITS=KEV", "##YUNITS=COUNTS",
            f"##FIRSTX={xs[0]:.3f}", f"##LASTX={xs[-1]:.3f}", f"##NPOINTS={len(xs)}", "##XYDATA=(XY..XY)"]
    body = [f"{x:.3f},{y:.1f}" for x, y in zip(xs, ys)]
    return "\n".join(head + body + ["##END="]) + "\n"


def xrf_spectrum(r, contaminated: bool) -> tuple[list[float], list[float]]:
    """NMC cathode XRF-like spectrum: Ni, Mn, Co lines; a contaminated lot shows Fe and Cu lines."""
    xs = [0.5 + i * 0.05 for i in range(300)]
    peaks = [(7.48, 5200), (5.90, 640), (6.93, 650), (8.26, 900)]  # Ni Ka, Mn Ka, Co Ka, Ni Kb
    if contaminated:
        peaks += [(6.40, r.uniform(260, 420)), (8.05, r.uniform(180, 300))]  # Fe Ka, Cu Ka
    else:
        peaks += [(6.40, r.uniform(10, 40)), (8.05, r.uniform(5, 25))]
    ys = []
    for x in xs:
        y = 40 + 15 * math.exp(-x / 6)
        for c, a in peaks:
            y += a * math.exp(-((x - c) ** 2) / (2 * 0.07 ** 2))
        ys.append(max(0.0, y + r.gauss(0, 6)))
    return xs, ys
