"""PNG reader for the 8-bit grayscale images the QC cameras write: chunk walk with CRC check, IDAT inflate,
filter reversal. Standard library only."""
from __future__ import annotations

import struct
import zlib

SIGNATURE = b"\x89PNG\r\n\x1a\n"


class PngError(ValueError):
    pass


def png_decode(data: bytes) -> tuple[int, int, bytes]:
    """(width, height, pixels row-major) or PngError (truncated, bad CRC, unsupported, undecodable)."""
    if data[:8] != SIGNATURE:
        raise PngError("not a PNG signature")
    pos = 8
    w = h = depth = ctype = None
    idat, seen_end = [], False
    while pos + 8 <= len(data):
        length, kind = struct.unpack(">I4s", data[pos:pos + 8])
        body = data[pos + 8:pos + 8 + length]
        crc = data[pos + 8 + length:pos + 12 + length]
        if len(body) < length or len(crc) < 4:
            raise PngError("truncated chunk")
        if zlib.crc32(kind + body) & 0xFFFFFFFF != struct.unpack(">I", crc)[0]:
            raise PngError(f"bad CRC in {kind.decode('latin-1')}")
        if kind == b"IHDR":
            w, h, depth, ctype = struct.unpack(">IIBB", body[:10])
        elif kind == b"IDAT":
            idat.append(body)
        elif kind == b"IEND":
            seen_end = True
            break
        pos += 12 + length
    if not seen_end:
        raise PngError("truncated: no IEND")
    if w is None or depth != 8 or ctype != 0:
        raise PngError("unsupported PNG (8-bit grayscale only)")
    try:
        raw = zlib.decompress(b"".join(idat))
    except zlib.error as e:
        raise PngError(f"IDAT does not inflate: {e}") from e
    stride = w
    if len(raw) != h * (stride + 1):
        raise PngError("IDAT size does not match IHDR")
    out, prev = bytearray(), bytearray(stride)
    for y in range(h):
        f = raw[y * (stride + 1)]
        line = bytearray(raw[y * (stride + 1) + 1:(y + 1) * (stride + 1)])
        for x in range(stride):
            a = line[x - 1] if x else 0
            b = prev[x]
            c = prev[x - 1] if x else 0
            if f == 1:
                line[x] = (line[x] + a) & 0xFF
            elif f == 2:
                line[x] = (line[x] + b) & 0xFF
            elif f == 3:
                line[x] = (line[x] + (a + b) // 2) & 0xFF
            elif f == 4:
                p = a + b - c
                pa, pb, pc = abs(p - a), abs(p - b), abs(p - c)
                line[x] = (line[x] + (a if pa <= pb and pa <= pc else b if pb <= pc else c)) & 0xFF
            elif f != 0:
                raise PngError(f"bad filter type {f}")
        out += line
        prev = line
    return w, h, bytes(out)
