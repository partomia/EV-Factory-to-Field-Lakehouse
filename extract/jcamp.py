"""JCAMP-DX 4.24 spectra with an (XY..XY) data table, as the incoming-inspection XRF writes them."""
from __future__ import annotations


class JcampError(ValueError):
    pass


def parse(text: str) -> dict:
    head, xs, ys, in_data, ended = {}, [], [], False, False
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("##"):
            key, _, val = line[2:].partition("=")
            key = key.strip().upper()
            if key == "END":
                ended = True
                break
            head[key] = val.strip()
            in_data = key == "XYDATA"
            continue
        if in_data:
            try:
                x, y = line.split(",")[:2]
                xs.append(float(x))
                ys.append(float(y))
            except ValueError as e:
                raise JcampError(f"bad data line {line[:40]!r}") from e
    if "TITLE" not in head or not ended:
        raise JcampError("missing ##TITLE or ##END")
    if head.get("NPOINTS") and int(head["NPOINTS"]) != len(xs):
        raise JcampError(f"NPOINTS {head['NPOINTS']} but {len(xs)} points")
    return {"header": head, "x": xs, "y": ys}
