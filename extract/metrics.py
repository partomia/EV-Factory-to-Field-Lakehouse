"""
Measurements from decoded objects (standard library; executors):

  xray_metrics   cell cross-section: bright blob at the can edge (electrode overhang), its size and intensity
  film_metrics   electrode film: streak count (bright near-vertical lines) and a bright defect patch
  xrf_peaks      incoming XRF spectrum: peak heights at the Ni / Mn / Co / Fe / Cu K-alpha lines
  edr_summary    EDR dump: trigger time, extremes of the pre / post buffers per channel
"""
from __future__ import annotations

from datetime import datetime, timezone

LINES_KEV = {"ni_ka": 7.48, "mn_ka": 5.90, "co_ka": 6.93, "fe_ka": 6.40, "cu_ka": 8.05}


def xray_metrics(w: int, h: int, px: bytes) -> dict:
    """The overhang shows as a dense saturated square right of the can wall (x >= 96). The wall arc and speckle
    reach the same gray level but only as isolated pixels, so a blob row needs a run of at least four."""
    xs, ys, n = [], [], 0
    for y in range(h):
        row = px[y * w:(y + 1) * w]
        hits = [x for x in range(96, w) if row[x] >= 245]
        n += len(hits)
        run, best = [], []
        for x in hits:
            run = run + [x] if run and x == run[-1] + 1 else [x]
            best = run if len(run) > len(best) else best
        if len(best) >= 4:
            xs += best
            ys.append(y)
    blob = len(ys) >= 3
    return {"mean_gray": round(sum(px) / len(px), 2), "edge_bright_px": n,
            "overhang_px": len(ys) if blob else 0,
            "overhang_box": f"{min(xs)},{min(ys)},{max(xs) + 1},{max(ys) + 1}" if blob else None}


def film_metrics(w: int, h: int, px: bytes) -> dict:
    """Columns where most rows are bright are streaks; a saturated 20x20 frame (streaks and speckle stay below 245)
    is a coating defect."""
    streak_cols = 0
    for x in range(w):
        bright = sum(1 for y in range(h) if px[y * w + x] >= 190)
        if bright >= h * 0.6:
            streak_cols += 1
    patch = sum(1 for y in range(20, 41) for x in range(60, 81) if px[y * w + x] >= 245)
    return {"mean_gray": round(sum(px) / len(px), 2), "streak_cols": streak_cols, "defect_patch": patch >= 60}


def xrf_peaks(xs: list[float], ys: list[float]) -> dict:
    base = sorted(ys)[len(ys) // 10]
    out = {}
    for name, kev in LINES_KEV.items():
        win = [y for x, y in zip(xs, ys) if abs(x - kev) <= 0.08]
        out[name] = round(max(win) - base, 1) if win else None
    out["fe_ni_ratio"] = round(out["fe_ka"] / out["ni_ka"], 5) if out.get("ni_ka") else None
    return out


def edr_summary(e: dict) -> dict:
    ch = {n: i for i, n in enumerate(e["channels"])}
    s, n_pre = e["samples"], e["n_pre"]

    def col(name, rows):
        return [r[ch[name]] for r in rows] if name in ch else []

    pre, post = s[:n_pre], s[n_pre:]
    t = datetime.fromtimestamp(e["trigger_ms"] / 1000, tz=timezone.utc)
    spread = [r[ch["cell_v_max"]] - r[ch["cell_v_min"]] for r in s] if {"cell_v_max", "cell_v_min"} <= ch.keys() else []
    return {"trigger_ts": t.strftime("%Y-%m-%d %H:%M:%S"), "duration_s": round(len(s) / e["hz"], 1),
            "cell_t_max_pre_c": round(max(col("cell_t_max", pre)), 2) if pre else None,
            "cell_t_max_post_c": round(max(col("cell_t_max", post)), 2) if post else None,
            "cell_v_min_v": round(min(col("cell_v_min", s)), 4) if s else None,
            "cell_dv_max_mv": round(1000 * max(spread), 1) if spread else None,
            "pack_i_max_a": round(max(abs(v) for v in col("pack_i", s)), 2) if s else None,
            "soc_at_trigger": round(s[n_pre][ch["soc"]], 2) if "soc" in ch and len(s) > n_pre else None,
            "speed_at_trigger_kph": round(s[n_pre][ch["speed_kph"]], 1) if "speed_kph" in ch and len(s) > n_pre else None}
