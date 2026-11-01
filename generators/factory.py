"""
Factory generator for one partition (cell line x pack-assembly month): cells and their 10 process steps
(cell MES shift exports), inline QC verdicts and images, formation curves, packs (pack-line MES documents,
wire-bond pulls, pack EOL) and vehicles (vehicle EOL documents). Standard library only.

Output records (consumed by generators/run.py):
  ("file", relpath, bytes)          relpath = <source>/<business_date>/<name>; unique within the whole run
  ("pack", dict)                    pack facts + truth, handed to generators/field.py in the same partition
  ("truth_batch", dict)             planted-cause truth per cell production batch

Step dates are fixed offsets from the production day (electrode, assembly, qc: +0; formation: +1;
grading incl. aging: +11; pack assembly: +12; vehicle EOL: +13), so files from different partitions never collide.
All values are synthetic.
"""
from __future__ import annotations

import io
import json
import math
from datetime import date, datetime, timedelta

from generators.core import IST, Batch, Plan, d, rng, shift_window
from generators.formats import circle, gray_canvas, line, png_gray, rect

AREAS = {
    "electrode": [("mixing", 0), ("coating", 0), ("calendering", 0)],
    "assembly": [("winding", 0), ("tab_welding", 0), ("canning", 0), ("electrolyte_fill", 0)],
    "formation": [("formation", 1)],
    "grading": [("aging", 11), ("grading", 11)],
}
STATIONS = {"mixing": "MX", "coating": "CT", "calendering": "CA", "winding": "WD", "tab_welding": "TW",
            "canning": "CN", "electrolyte_fill": "EF", "formation": "FM", "aging": "AG", "grading": "GR"}
MES_HEADER = "CELL_ID|BATCH_ID|LINE|STEP|STATION|START_TS|END_TS|RESULT|PARAMS"
QC_HEADER = "CELL_ID|BATCH_ID|LINE|INSPECTION|STATION|INSPECTED_TS|VERDICT|DEFECT_CODE|SCORE|BBOX|IMAGE_REF"


def ts(x: datetime) -> str:
    return x.astimezone(IST).isoformat(timespec="seconds")


class Cell:
    __slots__ = ("cell_id", "batch", "idx", "cap", "ir", "k", "overhang", "subthreshold", "xray_fail",
                 "ce", "dcir", "dqdv_v", "ocv_drop", "grade", "flagged", "sampled", "formation_line", "channel")


def make_cell(plan: Plan, b: Batch, i: int, film_dev: float) -> Cell:
    cc = plan.cfg["cell"]
    r = rng("cell", b.batch_id, i)
    c = Cell()
    c.cell_id, c.batch, c.idx = f"{b.batch_id}{i:04d}", b, i
    spread = 1.0 + 2.5 * b.drift
    c.cap = r.gauss(cc["capacity_ah"]["mean"] - 0.04 * b.drift - 0.02 * film_dev, cc["capacity_ah"]["sd"] * spread)
    c.ir = r.gauss(cc["ir_mohm"]["mean"] + (0.025 if b.contaminated else 0.0), cc["ir_mohm"]["sd"])
    k_med = cc["k_value_mv_day"]["median"] * (2.1 if b.contaminated else 1.0)
    c.k = k_med * math.exp(r.gauss(0, math.log(cc["k_value_mv_day"]["gsd"])))
    u = r.random()
    c.subthreshold = c.xray_fail = False
    thr = cc["xray_overhang_threshold"]
    if u < cc["xray_fail_rate"]:
        c.overhang, c.xray_fail = r.uniform(1.02, 1.6) * thr, True
    elif u < cc["xray_fail_rate"] + cc["subthreshold_overhang_rate"]:
        c.overhang, c.subthreshold = r.uniform(0.86, 0.99) * thr, True
    else:
        c.overhang = r.betavariate(2.0, 12.0) * 0.9 * thr
    c.ce = r.gauss(0.915 - (0.006 if b.contaminated else 0.0), 0.004)
    c.dcir = c.ir * r.uniform(1.9, 2.1)
    c.dqdv_v = r.gauss(3.62, 0.008)
    c.ocv_drop = c.k * 10 + r.gauss(0, 0.05)
    g = cc["grading"]
    if c.xray_fail or c.cap < g["capacity_min"] or c.ir > g["ir_max"] or c.k > g["k_max"]:
        c.grade = "REJECT"
    elif c.cap >= cc["capacity_ah"]["mean"] and c.k < 0.07:
        c.grade = "A"
    else:
        c.grade = "B"
    c.flagged = c.grade == "REJECT"
    c.sampled = r.random() < cc["image_sample_rate"]
    lines = plan.cfg["universe"]["formation_lines"][b.line]
    c.formation_line = lines[i % len(lines)]
    c.channel = r.randrange(plan.cfg["universe"]["formation_channels_per_line"])
    return c


def step_params(c: Cell, step: str, b: Batch, film_dev: float, r) -> tuple[str, str]:
    if step == "mixing":
        return "OK", f"viscosity_mpas={r.gauss(5200, 120):.0f};solids_pct={r.gauss(78.0, 0.3):.2f};powder_lot={b.powder_lot}"
    if step == "coating":
        return "OK", f"coat_weight_gsm={r.gauss(30.5, 0.25):.2f};dry_film=1"
    if step == "calendering":
        return "OK", f"thickness_um={118.0 + film_dev + r.gauss(0, 0.3):.2f};density_gcc={r.gauss(3.45, 0.02):.3f}"
    if step == "winding":
        return "OK", f"tension_n={r.gauss(12.0, 0.3):.2f};alignment_um={r.gauss(0, 15):.0f}"
    if step == "tab_welding":
        return "OK", f"weld_energy_j={r.gauss(42.0, 1.0):.1f}"
    if step == "canning":
        return "OK", f"crimp_height_mm={r.gauss(3.20, 0.02):.3f};can_lot={b.can_lot}"
    if step == "electrolyte_fill":
        return "OK", f"fill_g={r.gauss(98.0, 0.6):.2f};electrolyte_lot={b.electrolyte_lot}"
    if step == "formation":
        return "OK", (f"first_charge_ah={c.cap / c.ce:.3f};first_discharge_ah={c.cap:.3f};ce={c.ce:.4f};"
                      f"dcir_mohm={c.dcir:.3f};dqdv_peak_v={c.dqdv_v:.4f};channel={c.formation_line}-{c.channel:03d}")
    if step == "aging":
        return "OK", f"k_value_mv_day={c.k:.4f};ocv_drop_mv={c.ocv_drop:.3f};aging_days=10"
    if step == "grading":
        return ("NG" if c.grade == "REJECT" else "OK"), f"grade={c.grade};capacity_ah={c.cap:.3f};ir_mohm={c.ir:.3f}"
    raise KeyError(step)


def xray_png(c: Cell, r, corrupt: bool = False) -> tuple[bytes, str]:
    """128x128 cross-section: jelly-roll rings, can wall, overhang defect box at the edge if any."""
    w = h = 128
    px = gray_canvas(w, h)
    for i in range(len(px)):
        px[i] = 30
    circle(px, w, 64, 64, 60, ink=220)
    for rad in range(8, 56, 3):
        circle(px, w, 64, 64, rad, ink=140 + (rad % 2) * 40)
    bbox = ""
    if c.overhang > 0.7:
        size = int(4 + 10 * min(c.overhang, 1.6))
        x0, y0 = 100, 60 - size // 2
        for yy in range(y0, y0 + size):
            for xx in range(x0, min(x0 + size, w)):
                px[yy * w + xx] = 250
        bbox = f"{x0},{y0},{x0 + size},{y0 + size}"
    for _ in range(300):
        j = r.randrange(len(px))
        px[j] = min(255, px[j] + 30)
    data = png_gray(w, h, bytes(px))
    if corrupt:
        data = data[: len(data) // 3]
    return data, bbox


def film_png(segment_id: str, score: float, r) -> bytes:
    w, h = 160, 64
    px = gray_canvas(w, h)
    for i in range(len(px)):
        px[i] = 90
    n = int(score * 12)
    for _ in range(n):
        x, y = r.randrange(w), r.randrange(h)
        line(px, w, x, 0 if r.random() < 0.5 else y, x + r.randrange(-3, 4), h - 1, ink=200, thick=1)
    if score > 0.6:
        rect(px, w, 60, 20, 80, 40, ink=250)
    for _ in range(200):  # sensor noise: no two captures are byte-identical
        j = r.randrange(len(px))
        px[j] = min(255, px[j] + 25)
    return png_gray(w, h, bytes(px))


def formation_curve(c: Cell, points: int, r) -> list[tuple[float, float, float]]:
    """(minutes, voltage, current_a) for the first charge: CC then CV."""
    out = []
    for j in range(points):
        t = j * 3.0
        frac = j / (points - 1)
        v = 3.0 + 1.2 * (1 - math.exp(-3.2 * frac)) - (0.02 if c.batch.contaminated else 0.0) * frac + r.gauss(0, 0.002)
        i_a = 2.5 if frac < 0.75 else 2.5 * math.exp(-8 * (frac - 0.75))
        out.append((t, min(v, 4.2), i_a))
    return out


def generate_partition(plan: Plan, batches: list[Batch]):
    """Yields file, pack and truth records for one (line, assembly month) partition."""
    cfg = plan.cfg
    u, pk, cc = cfg["universe"], cfg["pack"], cfg["cell"]
    shifts = cfg["calendar"]["shifts"]
    faults = cfg.get("faults", {})
    corrupt_png_dates = {d(x) for x in faults.get("corrupt_xray_png", [])}
    orphan_dates = {d(x) for x in faults.get("genealogy_orphan_cell", [])}
    mes: dict[tuple, list[str]] = {}
    qc: dict[tuple, list[str]] = {}
    curves: dict[tuple, list[str]] = {}
    packs_by_shift: dict[tuple, list[str]] = {}
    vehicles_by_shift: dict[tuple, list[str]] = {}
    corrupt_done: set = set()
    pending: list[Cell] = []
    pack_seq: dict[date, int] = {}
    cpp = pk["cells_per_pack"]
    pack_line = u["pack_lines"][batches[0].line] if batches else None
    veh_line = u["vehicle_lines"][pack_line] if batches else None

    for b in sorted(batches, key=lambda x: (x.day, x.shift)):
        rb = rng("batch-film", b.batch_id)
        film_dev = rb.gauss(0, 0.4) + (2.0 * b.drift if b.drift else 0.0)
        yield ("truth_batch", {"batch_id": b.batch_id, "line": b.line, "production_date": b.day.isoformat(),
                               "shift": b.shift, "powder_lot": b.powder_lot, "contaminated": b.contaminated,
                               "drift": round(b.drift, 3), "film_dev_um": round(film_dev, 3), "n_cells": b.n_cells})
        s0, s1 = shift_window(b.day, b.shift, shifts)
        span = (s1 - s0).total_seconds()
        cells = [make_cell(plan, b, i, film_dev) for i in range(b.n_cells)]
        for c in cells:
            r = rng("steps", c.cell_id)
            for area, steps in AREAS.items():
                for k, (step, off) in enumerate(steps):
                    day = b.day + timedelta(days=off)
                    if day > plan.end:
                        continue
                    t0 = s0 + timedelta(days=off, seconds=span * (c.idx / b.n_cells) * 0.9 + k * 120)
                    t1 = t0 + timedelta(seconds=90 if off == 0 else 3600)
                    res, params = step_params(c, step, b, film_dev, r)
                    st = f"{STATIONS[step]}-{b.line}-{(c.idx % 4) + 1:02d}"
                    mes.setdefault((area, day, b.shift), []).append(
                        f"{c.cell_id}|{b.batch_id}|{b.line}|{step}|{st}|{ts(t0)}|{ts(t1)}|{res}|{params}")
            # inline X-ray at canning (+0)
            t_x = s0 + timedelta(seconds=span * (c.idx / b.n_cells) * 0.9 + 600)
            verdict = "FAIL" if c.xray_fail else "PASS"
            code = "XR-OVH" if c.xray_fail else ""
            img_ref, bbox = "", ""
            if c.flagged or c.sampled:
                corrupt = b.line == "CL1" and b.day in corrupt_png_dates and b.day not in corrupt_done
                corrupt_done.add(b.day) if corrupt else None
                data, bbox = xray_png(c, r, corrupt=corrupt)
                img_ref = f"qc_xray/{b.day}/{c.cell_id}.png"
                yield ("file", img_ref, data)
            qc.setdefault((b.day, b.shift), []).append(
                f"{c.cell_id}|{b.batch_id}|{b.line}|XRAY|XR-{b.line}-01|{ts(t_x)}|{verdict}|{code}|{c.overhang:.4f}|{bbox}|{img_ref}")
            if (c.flagged or c.sampled) and b.day + timedelta(days=1) <= plan.end:
                fday = b.day + timedelta(days=1)
                rows = curves.setdefault((c.formation_line, fday, b.shift), [])
                for t, v, i_a in formation_curve(c, cc["curve_points"], r):
                    rows.append(f"{c.cell_id},{c.formation_line}-{c.channel:03d},{t:.1f},{v:.4f},{i_a:.3f}")
        # electrode film vision: 6 segments per batch
        for sgi in range(6):
            r = rng("film", b.batch_id, sgi)
            score = min(1.0, max(0.0, r.betavariate(1.5, 8.0) + (0.35 * b.drift if b.drift else 0.0)))
            seg = f"{b.batch_id}S{sgi}"
            verdict = "FAIL" if score > 0.6 else "PASS"
            img_ref = ""
            if verdict == "FAIL" or r.random() < cc["image_sample_rate"] * 5:
                img_ref = f"qc_film/{b.day}/{seg}.png"
                yield ("file", img_ref, film_png(seg, score, r))
            t_f = s0 + timedelta(minutes=30 + 60 * sgi)
            qc.setdefault((b.day, b.shift), []).append(
                f"{seg}|{b.batch_id}|{b.line}|FILM|VF-{b.line}-01|{ts(t_f)}|{verdict}|{'FLM-COAT' if verdict == 'FAIL' else ''}|{score:.4f}||{img_ref}")

        # packs: graded cells in order; leftovers carry to the next batch on this line (mixed packs)
        if b.assembly_day > plan.end:
            continue
        pending.extend(c for c in cells if c.grade != "REJECT")
        while len(pending) >= cpp:
            group, pending = pending[:cpp], pending[cpp:]
            yield from _pack(plan, b, group, pack_line, veh_line, pack_seq, packs_by_shift, vehicles_by_shift,
                             orphan_dates)

    for (area, day, shift), rows in mes.items():
        body = MES_HEADER + "\n" + "\n".join(rows) + "\n"
        n = len(rows)
        if (batches[0].line == "CL1" and area == "formation" and shift == "B"
                and day.isoformat() in faults.get("mes_trailer_off_by_one", [])):
            n += 1
        yield ("file", f"cell_mes/{day}/{batches[0].line}_{area}_{shift}.csv", (body + f"TRL|{n}\n").encode())
    for (day, shift), rows in qc.items():
        yield ("file", f"cell_qc/{day}/{batches[0].line}_qc_{shift}.csv",
               (QC_HEADER + "\n" + "\n".join(rows) + f"\nTRL|{len(rows)}\n").encode())
    for (fl, day, shift), rows in curves.items():
        yield ("file", f"formation_curves/{day}/{fl}_{shift}.csv",
               ("cell_id,channel,minutes,voltage_v,current_a\n" + "\n".join(rows) + "\n").encode())
    for (day, shift), docs in packs_by_shift.items():
        yield ("file", f"pack_mes/{day}/{pack_line}_{shift}.ndjson", ("\n".join(docs) + "\n").encode())
    for (day, shift), docs in vehicles_by_shift.items():
        yield ("file", f"vehicle_eol/{day}/{veh_line}_{shift}.ndjson", ("\n".join(docs) + "\n").encode())


def _pack(plan, b: Batch, group: list[Cell], pack_line, veh_line, pack_seq, packs_by_shift, vehicles_by_shift,
          orphan_dates):
    cfg = plan.cfg
    u, pk, causes = cfg["universe"], cfg["pack"], cfg["causes"]
    shifts = cfg["calendar"]["shifts"]
    day = b.assembly_day
    seq = pack_seq.get(day, 0) + 1
    pack_seq[day] = seq
    pack_id = f"PK{pack_line}{day:%y%m%d}{seq:04d}"
    r = rng("pack", pack_id)
    station = u["wirebond_stations"][seq % len(u["wirebond_stations"])]
    weak = station == causes["weak_bond_station"] and plan.weak_bond_active(day)
    pulls = []
    for _ in range(len(group) * pk["bonds_per_cell"]):
        pulls.append(max(150.0, r.gauss(600, 70) if weak else r.gauss(720, 35)))
    caps = [c.cap for c in group]
    groups = [sum(caps[j::pk["series"]]) for j in range(pk["series"])]
    cap_pack = min(groups)
    cap_spread = (max(caps) - min(caps))
    ks = sorted(c.k for c in group)
    k_p95 = ks[int(0.95 * (len(ks) - 1))]
    v_spread_mv = 4.0 + 25.0 * cap_spread + 60.0 * (k_p95 - 0.05) + r.gauss(0, 1.0)
    batches = {}
    for c in group:
        batches[c.batch.batch_id] = batches.get(c.batch.batch_id, 0) + 1
    majority = max(batches, key=batches.get)
    cells = []
    for j, c in enumerate(group):
        cells.append({"cell_id": c.cell_id, "module": f"M{j // (len(group) // pk['modules']) + 1}",
                      "position": j + 1})
    if day in orphan_dates and seq == 1 and pack_line == "PL1":
        cells[7]["cell_id"] = f"{group[7].batch.batch_id}9{group[7].idx:03d}X"
    s0, s1 = shift_window(day, b.shift, shifts)
    t_eol = s0 + timedelta(minutes=(seq * 7) % int((s1 - s0).total_seconds() // 60))
    fw = "BMS-3.4.2" if day >= d("2026-07-01") else "BMS-3.4.1"
    insul = r.gauss(520, 40)
    eol_pass = cap_pack > 24.4 * pk["parallel"] and insul > 380 and min(pulls) > 300
    doc = {
        "doc_type": "PACK_BUILD", "synthetic": True, "pack_serial": pack_id, "pack_line": pack_line,
        "build_ts": ts(t_eol - timedelta(minutes=40)), "shift": b.shift,
        "config": f"{pk['series']}s{pk['parallel']}p",
        "cells": cells,
        "wire_bond": {"station": station, "bonds": len(pulls), "pull_min_g": round(min(pulls), 1),
                      "pull_mean_g": round(sum(pulls) / len(pulls), 1),
                      "pull_p05_g": round(sorted(pulls)[int(0.05 * len(pulls))], 1)},
        "eol": {"test_ts": ts(t_eol), "capacity_ah": round(cap_pack, 3), "insulation_mohm": round(insul, 1),
                "cell_v_spread_mv": round(v_spread_mv, 2), "bms_fw": fw, "result": "PASS" if eol_pass else "FAIL"},
    }
    packs_by_shift.setdefault((day, b.shift), []).append(json.dumps(doc, separators=(",", ":")))
    pack = {
        "pack_id": pack_id, "pack_line": pack_line, "assembly_date": day, "shift": b.shift,
        "majority_batch": majority, "n_batches": len(batches), "station": station, "weak_bond": weak,
        "contaminated": sum(1 for c in group if c.batch.contaminated) >= len(group) / 2,
        "drift": max(c.batch.drift for c in group),
        "n_subthreshold": sum(1 for c in group if c.subthreshold),
        "cap_spread": cap_spread, "k_p95": k_p95, "v_spread_mv": v_spread_mv, "eol_pass": eol_pass,
        "bms_fw": fw, "eol_ts": t_eol,
    }
    vday = day + timedelta(days=cfg["universe"]["pack_to_vehicle_eol_days"])
    if not eol_pass or vday > plan.end:
        yield ("pack", pack)
        return
    # vehicle EOL the next day, same shift
    vin = f"XEVS{pack_line[-1]}K{day:%y%m%d}{seq:05d}"
    t_v = t_eol + timedelta(days=1)
    rv = rng("vehicle", vin)
    vdoc = {
        "doc_type": "VEHICLE_EOL", "synthetic": True, "vin": vin, "vehicle_line": veh_line, "model": "S-4.7",
        "eol_ts": ts(t_v), "pack_serial": pack_id, "motor_serial": f"MT{vday:%y%m%d}{seq:04d}",
        "mcu_serial": f"MC{vday:%y%m%d}{seq:04d}", "vcu_fw": "VCU-2.8.0", "bms_fw": fw,
        "tests": {"functional": "PASS", "brake_n": round(rv.gauss(820, 20), 1),
                  "insulation_mohm": round(rv.gauss(480, 30), 1), "charge_handshake": "PASS"},
        "result": "PASS",
    }
    vehicles_by_shift.setdefault((vday, b.shift), []).append(json.dumps(vdoc, separators=(",", ":")))
    pack["vin"] = vin
    pack["vehicle_eol_date"] = vday
    pack["vehicle_eol_ts"] = t_v
    yield ("pack", pack)
