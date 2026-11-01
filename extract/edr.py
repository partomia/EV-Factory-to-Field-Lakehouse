"""Event data recorder dumps (`.edr`): fixed little-endian header, channel names, float32 samples
channel-interleaved at `hz`, `n_pre` samples before the trigger. Standard library only."""
from __future__ import annotations

import struct

MAGIC = b"EVXEDR1\x00"
HEADER = struct.Struct("<H17s24sBqHIIH")
TRIGGERS = {1: "thermal", 2: "fault", 3: "crash"}


class EdrError(ValueError):
    pass


def parse(data: bytes, samples: bool = True) -> dict:
    if data[:8] != MAGIC:
        raise EdrError("bad magic")
    if len(data) < 8 + HEADER.size:
        raise EdrError("truncated header")
    ver, vin, eid, trig, trig_ms, hz, n_pre, n_post, n_ch = HEADER.unpack_from(data, 8)
    pos = 8 + HEADER.size
    names = [data[pos + 12 * i:pos + 12 * (i + 1)].rstrip(b"\x00").decode("ascii") for i in range(n_ch)]
    pos += 12 * n_ch
    n = n_pre + n_post
    if len(data) - pos != n * n_ch * 4:
        raise EdrError(f"sample block {len(data) - pos} bytes, header says {n * n_ch * 4}")
    out = {"version": ver, "vin": vin.decode("ascii").strip(), "event_id": eid.decode("ascii").strip(),
           "trigger": TRIGGERS.get(trig, f"code{trig}"), "trigger_ms": trig_ms, "hz": hz, "n_pre": n_pre,
           "n_post": n_post, "channels": names}
    if samples:
        row = struct.Struct(f"<{n_ch}f")
        out["samples"] = [row.unpack_from(data, pos + i * row.size) for i in range(n)]
    return out
