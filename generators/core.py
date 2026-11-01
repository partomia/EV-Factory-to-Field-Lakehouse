"""
Deterministic building blocks for the synthetic EV universe (standard library only).

  rng(*keys)       a random.Random seeded from the keys, so any partition of the work regenerates the same values
  Plan             the small driver-side plan: calendar, daily build rates, cell production batches (mixing lots),
                   powder and other material lots, planted cause episodes. Everything heavy is derived from it
                   per partition (generators/sources/*).

All values are synthetic. Nothing here describes the customer's real data.
"""
from __future__ import annotations

import hashlib
import json
import math
import random
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
IST = timezone(timedelta(hours=5, minutes=30))


def load_config(overlay: dict | None = None) -> dict:
    cfg = json.loads((ROOT / "config" / "lakehouse.json").read_text())
    if overlay:
        cfg = deep_merge(cfg, overlay)
    return cfg


def deep_merge(a: dict, b: dict) -> dict:
    out = dict(a)
    for k, v in b.items():
        out[k] = deep_merge(out[k], v) if isinstance(v, dict) and isinstance(out.get(k), dict) else v
    return out


def stable_int(*keys) -> int:
    h = hashlib.blake2b("|".join(str(k) for k in keys).encode(), digest_size=8)
    return int.from_bytes(h.digest(), "big")


def rng(*keys) -> random.Random:
    return random.Random(stable_int(*keys))


def d(s: str) -> date:
    return date.fromisoformat(s)


def days(a: date, b: date):
    """a .. b inclusive."""
    for i in range((b - a).days + 1):
        yield a + timedelta(days=i)


def shift_window(day: date, shift: str, shifts: dict) -> tuple[datetime, datetime]:
    h0, h1 = shifts[shift]
    start = datetime(day.year, day.month, day.day, tzinfo=IST) + timedelta(hours=h0)
    return start, start + timedelta(hours=h1 - h0)


def logistic(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


SHIFTS = ("A", "B", "C")


@dataclass
class Batch:
    """A cell production batch = one cathode mixing lot on one cell line and shift (the ML split unit)."""
    batch_id: str
    line: str
    day: date
    shift: str
    n_cells: int
    powder_lot: str
    anode_lot: str
    electrolyte_lot: str
    separator_lot: str
    can_lot: str
    contaminated: bool
    drift: float            # 0 = none, else calender drift magnitude for this batch
    assembly_day: date


@dataclass
class MaterialLot:
    lot_id: str             # SAP CHARG
    material: str           # SAP MATNR
    vendor: str             # SAP LIFNR
    line: str
    first_day: date
    last_day: date
    contaminated: bool = False


@dataclass
class Plan:
    cfg: dict
    scale: float = 1.0
    batches: list[Batch] = field(default_factory=list)
    lots: dict[str, MaterialLot] = field(default_factory=dict)

    @property
    def start(self) -> date:
        return d(self.cfg["calendar"]["history_start"])

    @property
    def history_end(self) -> date:
        return d(self.cfg["calendar"]["history_end"])

    @property
    def live_dates(self) -> list[date]:
        return [d(x) for x in self.cfg["calendar"]["live_dates"]]

    @property
    def end(self) -> date:
        return self.live_dates[-1]

    def business_dates(self) -> list[date]:
        return list(days(self.start, self.end))

    def packs_per_day(self, day: date) -> float:
        r = self.cfg["universe"]["ramp"]
        n = (self.history_end - self.start).days
        t = min(max((day - self.start).days / n, 0.0), 1.0)
        return self.scale * (r["packs_per_day_start"] + t * (r["packs_per_day_end"] - r["packs_per_day_start"]))

    def weak_bond_active(self, day: date) -> bool:
        return any(d(a) <= day <= d(b) for a, b in self.cfg["causes"]["weak_bond_periods"])


MATERIALS = {
    # MATNR: (description, vendors, lot length in days)
    "MAT-CATH-NMC811": ("Cathode active powder NMC811", ["V-CATH-01", "V-CATH-02"], 3),
    "MAT-ANOD-GR": ("Anode graphite powder", ["V-ANOD-01"], 4),
    "MAT-ELYT-LPF6": ("Electrolyte LiPF6 blend", ["V-ELYT-01"], 5),
    "MAT-SEP-PE12": ("Separator PE 12um", ["V-SEP-01"], 6),
    "MAT-CAN-4680": ("Cell can 4680 nickel-plated steel", ["V-CAN-01"], 7),
}
CATHODE = "MAT-CATH-NMC811"


def build_plan(cfg: dict, scale: float = 1.0) -> Plan:
    plan = Plan(cfg=cfg, scale=scale)
    u, c = cfg["universe"], cfg["causes"]
    lag = u["cell_to_pack_days"]
    cpp = cfg["pack"]["cells_per_pack"]
    grade_yield = 0.955
    contaminated = {(d(x["week_of"]), x["line"]) for x in c["contaminated_powder_lots"]}
    # cells are produced up to the last business date; later steps, packs and vehicles past it stay WIP
    prod_end = plan.end

    for line in u["cell_lines"]:
        # material lots per line, rotating vendors; lot boundaries are deterministic per material
        for matnr, (_, vendors, length) in MATERIALS.items():
            # lots run past the calendar so goods receipts (a few days ahead of use) continue to the end
            day, k, lots_end = plan.start, 0, prod_end + timedelta(days=14)
            phase = stable_int("lot-phase", matnr, line) % length
            while day <= lots_end:
                span = length - phase if k == 0 else length
                last = min(day + timedelta(days=span - 1), lots_end)
                vendor = vendors[stable_int(matnr, line, k) % len(vendors)] if len(vendors) > 1 else vendors[0]
                lot = MaterialLot(f"{matnr[4:8]}{line}{k:04d}", matnr, vendor, line, day, last)
                if matnr == CATHODE:
                    # the one lot in use on the episode's start day is the contaminated supplier batch
                    lot.contaminated = any(ln == line and day <= wk <= last for wk, ln in contaminated)
                plan.lots[lot.lot_id] = lot
                day, k = last + timedelta(days=1), k + 1

    by_line_mat: dict[tuple[str, str], list[MaterialLot]] = {}
    for lot in plan.lots.values():
        by_line_mat.setdefault((lot.line, lot.material), []).append(lot)

    def lot_for(line: str, matnr: str, day: date) -> MaterialLot:
        for lot in by_line_mat[(line, matnr)]:
            if lot.first_day <= day <= lot.last_day:
                return lot
        raise KeyError((line, matnr, day))

    drift_eps = []
    for ep in c["calender_drift"]:
        s0 = d(ep["start"])
        drift_eps.append((ep["line"], s0, s0 + timedelta(days=ep["days"] - 1), set(ep["shifts"])))

    for day in days(plan.start, prod_end):
        cells_day = plan.packs_per_day(day + timedelta(days=lag)) * cpp / grade_yield
        per_batch = cells_day / (len(u["cell_lines"]) * len(SHIFTS))
        for line in u["cell_lines"]:
            for shift in SHIFTS:
                r = rng("batch", line, day, shift)
                n = max(int(round(per_batch * r.uniform(0.92, 1.08))), cpp + 4)
                drift = 0.0
                for ln, a, b, sh in drift_eps:
                    if ln == line and a <= day <= b and shift in sh:
                        drift = 1.0 + 0.5 * ((day - a).days / max((b - a).days, 1))
                cath = lot_for(line, CATHODE, day)
                plan.batches.append(Batch(
                    batch_id=f"CB{line}{day:%y%m%d}{shift}",
                    line=line, day=day, shift=shift, n_cells=n,
                    powder_lot=cath.lot_id,
                    anode_lot=lot_for(line, "MAT-ANOD-GR", day).lot_id,
                    electrolyte_lot=lot_for(line, "MAT-ELYT-LPF6", day).lot_id,
                    separator_lot=lot_for(line, "MAT-SEP-PE12", day).lot_id,
                    can_lot=lot_for(line, "MAT-CAN-4680", day).lot_id,
                    contaminated=cath.contaminated, drift=drift,
                    assembly_day=day + timedelta(days=lag)))
    return plan


def partitions(plan: Plan) -> list[tuple[str, str]]:
    """(cell line, assembly month 'YYYY-MM'): disjoint production dates, pack dates and vehicle EOL dates."""
    keys = sorted({(b.line, f"{b.assembly_day:%Y-%m}") for b in plan.batches})
    return keys


def batches_of(plan: Plan, line: str, month: str) -> list[Batch]:
    return [b for b in plan.batches if b.line == line and f"{b.assembly_day:%Y-%m}" == month]
