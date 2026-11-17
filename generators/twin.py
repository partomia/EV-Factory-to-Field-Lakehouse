"""
Digital twin source (PLAN.md section 7, source 6): actual station cycle events from the line PLCs / MES and the
twin's weekly scenario outputs. Standard library only; config in config/twin.json, lines from config/lakehouse.json.

  station_cycle_events(cfg, day, scale)   every completed station cycle of one business date (the IST date of
                                          mes_ts, the MES clock); fields as INTERFACES.md section 2
  twin_scenarios(cfg, week_start, scale)  the twin's scenario documents for the week starting on that Monday
  generate_twin(cfg, days, scale)         generator records for generators/run.py:run_local and cde/jobs/generate.py:
                                            ("file", "line_event/<date>/<line>.csv", bytes)       header, no trailer
                                            ("file", "twin_scenario/<week start>/<line>.ndjson", bytes)
                                            ("truth_twin", dict)   designed bottleneck per scenario, actual drift
                                            ("truth_fault", dict)  the clock-skew instance, when generators.clock_skew
                                                                   is present and its date is generated

  python -m generators.twin --out data/generated --scale 0.05 [--from 2026-10-24 --to 2026-11-09]

Each line is a serial flow line: a unit enters station 1 and moves one station per pitch (the slowest design cycle
plus transfer). Cell lines run the real cell ids of the cell production batches (generators/core.py); pack and
vehicle lines use synthetic unit ids (the pack serials and VINs depend on cell grading, which is not replayed here).
Cycle times vary per cycle around each station's design value (shift, actual drift); FAULT, BLOCKED and STARVED
cycles take longer, BLOCKED mostly upstream of the bottleneck and STARVED downstream. Line events cover
config/twin.json line_events_from through the last live date; scenarios cover every week of the calendar.
Ids are unique by construction: event_id = station + date + sequence, scenario_id = line + week + index.
Every value is synthetic and seeded: the same (seed, scale, day) gives the same events.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import math
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from generators.core import IST, SHIFTS, build_plan, d, load_config, rng  # noqa: E402

try:
    from generators.clock_skew import apply as _skew
except ImportError:
    _skew = lambda e, cfg: e  # noqa: E731

LINE_EVENT_COLS = ["event_id", "station_id", "line_id", "area", "unit_id", "cycle_start_ts", "event_ts", "mes_ts",
                   "cycle_s", "status", "shift"]
UNIT_PREFIX = {"PACK": "PU", "VEHICLE": "VU"}
_PLANS: dict = {}


def twin_config(cfg: dict) -> dict:
    """cfg["twin_cfg"] when a caller merged it in, else config/twin.json."""
    return cfg.get("twin_cfg") or json.loads((ROOT / "config" / "twin.json").read_text())


def _plan(cfg: dict, scale: float):
    key = (json.dumps({k: v for k, v in cfg.items() if not k.startswith("_")}, sort_keys=True, default=str), scale)
    if key not in _PLANS:
        _PLANS.clear()
        plan = build_plan(cfg, scale)
        plan.by_key = {(b.line, b.day, b.shift): b for b in plan.batches}
        _PLANS[key] = plan
    return _PLANS[key]


def lines(cfg: dict) -> dict[str, str]:
    """line id -> area, from the lakehouse universe."""
    u = cfg["universe"]
    out = {ln: "CELL" for ln in u["cell_lines"]}
    out.update({ln: "PACK" for ln in sorted(set(u["pack_lines"].values()))})
    out.update({ln: "VEHICLE" for ln in sorted(set(u["vehicle_lines"].values()))})
    return out


def stations(cfg: dict, line: str) -> list[dict]:
    """The line's stations in flow order: station_id, code, step, design cycle_s, seq, bottleneck (slowest design)."""
    tc = twin_config(cfg)
    area = lines(cfg)[line]
    a = tc["areas"][area]
    slowest = max(s["cycle_s"] for s in a["stations"])
    return [{"station_id": f"{a['code']}-{s['code']}-{int(line[-1]):02d}", "code": s["code"], "step": s["step"],
             "cycle_s": s["cycle_s"], "seq": i + 1, "bottleneck": s["cycle_s"] == slowest}
            for i, s in enumerate(a["stations"])]


def _ts(x: datetime) -> str:
    return x.astimezone(IST).isoformat(timespec="seconds")


def _shift_of(x: datetime, shifts: dict) -> str:
    h = x.astimezone(IST).hour
    for name, (h0, h1) in shifts.items():
        if h0 <= h < h1 or h0 <= h + 24 < h1:
            return name
    raise ValueError(h)


def _drift(tc: dict, station_id: str, day: date) -> float:
    f = 1.0
    for x in tc.get("actual_drift", []):
        if x["station_id"] == station_id and day >= d(x["from"]):
            f *= x["factor"]
    return f


def _units(cfg: dict, plan, line: str, area: str, pday: date) -> list[tuple[str, datetime]]:
    """(unit id, entry time at the first station) for one production day (06:00 IST to 06:00 the next day)."""
    shifts = cfg["calendar"]["shifts"]
    if area == "CELL":
        out = []
        for sh in SHIFTS:
            b = plan.by_key.get((line, pday, sh))
            if b is None:
                continue
            h0, h1 = shifts[sh]
            s0 = datetime(pday.year, pday.month, pday.day, tzinfo=IST) + timedelta(hours=h0)
            span = (h1 - h0) * 3600
            out += [(f"{b.batch_id}{i:04d}", s0 + timedelta(seconds=span * i / b.n_cells)) for i in range(b.n_cells)]
        return out
    if pday < plan.start or pday > plan.end:
        return []
    n_lines = sum(1 for a in lines(cfg).values() if a == area)
    x = plan.packs_per_day(pday) / n_lines
    r = rng("twin-units", line, pday)
    n = max(1, int(x) + (1 if r.random() < x - int(x) else 0))
    day0 = datetime(pday.year, pday.month, pday.day, tzinfo=IST) + timedelta(hours=min(h for h, _ in shifts.values()))
    return [(f"{UNIT_PREFIX[area]}{line}{pday:%y%m%d}{k + 1:04d}",
             day0 + timedelta(seconds=86400 * (k + r.uniform(0.2, 0.8)) / n)) for k in range(n)]


def station_cycle_events(cfg: dict, day: date, scale: float) -> list[dict]:
    """Every station cycle whose MES receive time (mes_ts, IST) falls on `day`, deterministic for (seed, scale, day).
    Each event passes through generators.clock_skew.apply (lane D) after its id is assigned."""
    tc = twin_config(cfg)
    plan = _plan(cfg, scale)
    shifts = cfg["calendar"]["shifts"]
    lo, hi = tc["mes_latency_s"]
    rates, bias = tc["status_rates"], tc["status_bias"]
    day_start = datetime(day.year, day.month, day.day, tzinfo=IST)
    day_end = day_start + timedelta(days=1)
    out = []
    for line, area in lines(cfg).items():
        sts = stations(cfg, line)
        a = tc["areas"][area]
        pitch = max(s["cycle_s"] for s in sts) + a["transfer_s"]
        b_idx = next(i for i, s in enumerate(sts) if s["bottleneck"])
        reach = len(sts) * pitch + tc["fault_extra_s"][1] + 4 * pitch
        per_station = defaultdict(list)
        for pday in (day - timedelta(days=1), day):
            for unit, entry in _units(cfg, plan, line, area, pday):
                if entry + timedelta(seconds=reach) < day_start or entry >= day_end:
                    continue
                f_shift = tc["shift_factor"][_shift_of(entry, shifts)]
                for j, s in enumerate(sts):
                    r = rng("twin-cycle", s["station_id"], unit)
                    nominal = s["cycle_s"] * f_shift * _drift(tc, s["station_id"], pday)
                    cyc = nominal * math.exp(r.gauss(0.0, tc["cycle_sigma"]))
                    p_block = rates["BLOCKED"] * (bias if j < b_idx else 1 / bias)
                    p_starve = rates["STARVED"] * (bias if j > b_idx else 1 / bias)
                    u = r.random()
                    if u < rates["FAULT"]:
                        status, cyc = "FAULT", cyc + r.uniform(*tc["fault_extra_s"])
                    elif u < rates["FAULT"] + p_block:
                        status, cyc = "BLOCKED", cyc + nominal * r.uniform(*tc["wait_extra"])
                    elif u < rates["FAULT"] + p_block + p_starve:
                        status, cyc = "STARVED", cyc + nominal * r.uniform(*tc["wait_extra"])
                    else:
                        status = "OK"
                    start = entry + timedelta(seconds=int(j * pitch + r.uniform(0, a["transfer_s"])))
                    secs = max(1, int(round(cyc)))
                    end = start + timedelta(seconds=secs)
                    mes = end + timedelta(seconds=r.randint(lo, hi))
                    if not day_start <= mes < day_end:
                        continue
                    per_station[s["station_id"]].append((mes, unit, start, end, secs, status))
        for s in sts:
            for seq, (mes, unit, start, end, secs, status) in enumerate(sorted(per_station[s["station_id"]]), 1):
                ev = {"event_id": f"{s['station_id']}-{day:%Y%m%d}-{seq:06d}", "station_id": s["station_id"],
                      "line_id": line, "area": area, "unit_id": unit, "cycle_start_ts": _ts(start),
                      "event_ts": _ts(end), "mes_ts": _ts(mes), "cycle_s": float(secs), "status": status,
                      "shift": _shift_of(mes, shifts)}
                out.append(_skew(ev, cfg))
    return out


def week_starts(cfg: dict, scale: float = 1.0) -> list[date]:
    """Every twin week (Monday) from the first history week through the last live date."""
    plan = _plan(cfg, scale)
    first = plan.start - timedelta(days=plan.start.weekday())
    step = twin_config(cfg)["twin"]["cadence_days"]
    out, w = [], first
    while w <= plan.end:
        out.append(w)
        w += timedelta(days=step)
    return out


def _planned_units(cfg: dict, plan, line: str, area: str, week_start: date) -> float:
    days = [week_start + timedelta(days=i) for i in range(7)]
    if area == "CELL":
        per_day = [sum(b.n_cells for sh in SHIFTS if (b := plan.by_key.get((line, x, sh)))) for x in days]
        per_day = [n for n in per_day if n] or [0]
        return sum(per_day) / len(per_day)
    n_lines = sum(1 for a in lines(cfg).values() if a == area)
    return sum(plan.packs_per_day(x) for x in days) / 7 / n_lines


def _scenarios(cfg: dict, week_start: date, scale: float) -> list[tuple[dict, dict]]:
    """(scenario document, generator truth) for every line's scenarios of one week."""
    tc = twin_config(cfg)
    tw = tc["twin"]
    plan = _plan(cfg, scale)
    names = list(tw["scenarios"])
    extras = [n for n in names if n != "BASELINE"]
    out = []
    for line, area in lines(cfg).items():
        sts = stations(cfg, line)
        r = rng("twin-scenario", line, week_start)
        k = r.choices(range(len(tw["extras_weights"])), weights=tw["extras_weights"])[0]
        chosen = ["BASELINE"] + r.sample(extras, k)
        base_units = _planned_units(cfg, plan, line, area, week_start)
        generated = datetime(week_start.year, week_start.month, week_start.day, 18, tzinfo=IST) - timedelta(days=3)
        for i, name in enumerate(chosen):
            sc = tw["scenarios"][name]
            sid = f"TS-{line}-{week_start:%y%m%d}-{i:02d}"
            rs = rng("twin-scenario-design", sid)
            tool = rs.choice([s["station_id"] for s in sts if not s["bottleneck"]]) if "tool_factor" in sc else None
            units = base_units * sc.get("demand", 1.0)
            available = 86400 * tw["availability"] * sc.get("available", 1.0)
            preds = []
            for s in sts:
                f = sc.get("area_factor", 1.0) * (sc["tool_factor"] if s["station_id"] == tool else 1.0)
                rp = rng("twin-prediction", sid, s["station_id"])
                preds.append(round(s["cycle_s"] * f * math.exp(rp.gauss(0.0, tw["prediction_sigma"])), 1))
            top = max(range(len(sts)), key=lambda j: preds[j])
            doc = {"doc_type": "TWIN_SCENARIO", "synthetic": True, "scenario_id": sid,
                   "week_start": week_start.isoformat(), "line_id": line, "area": area, "scenario_name": name,
                   "model_version": tw["model_version"], "generated_ts": _ts(generated),
                   "planned_units_per_day": round(units, 1), "available_s": round(available, 1),
                   "n_stations": len(sts), "bottleneck_station": sts[top]["station_id"],
                   "stations": [{"station_id": s["station_id"], "seq": s["seq"], "predicted_cycle_s": p,
                                 "utilisation": round(p * units / available, 4), "bottleneck": j == top}
                                for j, (s, p) in enumerate(zip(sts, preds))]}
            truth = {"kind": "scenario", "scenario_id": sid, "week_start": week_start.isoformat(), "line_id": line,
                     "scenario_name": name, "designed_bottleneck": sts[top]["station_id"],
                     "design_bottleneck": next(s["station_id"] for s in sts if s["bottleneck"]),
                     "tool_change_station": tool}
            out.append((doc, truth))
    return out


def twin_scenarios(cfg: dict, week_start: date, scale: float) -> list[dict]:
    """The twin's scenario documents for the week starting `week_start` (a Monday): one BASELINE per line plus
    0-2 what-if scenarios; per station the predicted cycle s, utilisation and the predicted bottleneck."""
    return [doc for doc, _ in _scenarios(cfg, week_start, scale)]


def line_event_csv(events: list[dict]) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(LINE_EVENT_COLS)
    for e in sorted(events, key=lambda e: (e["mes_ts"], e["event_id"])):
        w.writerow([f"{e[c]:.1f}" if c == "cycle_s" else e[c] for c in LINE_EVENT_COLS])
    return buf.getvalue().encode()


def generate_twin(cfg: dict, days: list[date], scale: float):
    """Generator records for the given business dates (see the module docstring)."""
    tc = twin_config(cfg)
    plan = _plan(cfg, scale)
    first, end = d(tc["line_events_from"]), plan.end
    weeks = set(week_starts(cfg, scale))
    for day in sorted(days):
        if first <= day <= end:
            by_line = defaultdict(list)
            for e in station_cycle_events(cfg, day, scale):
                by_line[e["line_id"]].append(e)
            for line in sorted(by_line):
                yield ("file", f"line_event/{day}/{line}.csv", line_event_csv(by_line[line]))
        if day in weeks:
            by_line = defaultdict(list)
            for doc, truth in _scenarios(cfg, day, scale):
                by_line[doc["line_id"]].append(json.dumps(doc, separators=(",", ":")))
                yield ("truth_twin", truth)
            for line in sorted(by_line):
                yield ("file", f"twin_scenario/{day}/{line}.ndjson", ("\n".join(by_line[line]) + "\n").encode())
    wanted = {x.isoformat() for x in days}
    for x in tc.get("actual_drift", []):
        if x["from"] in wanted:
            yield ("truth_twin", {"kind": "actual_drift", "station_id": x["station_id"], "from": x["from"],
                                  "factor": x["factor"], "note": x.get("note", "")})
    skew = cfg.get("faults", {}).get("clock_skew", {})
    if skew.get("date") in wanted and first <= d(skew["date"]) <= end:
        try:
            from generators.clock_skew import truth_rows
        except ImportError:
            truth_rows = None
        for row in (truth_rows(cfg) if truth_rows else []):
            yield ("truth_fault", row)


def main(argv=None) -> int:
    from generators.run import summary, write_manifests

    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--out", default="data/generated")
    p.add_argument("--scale", type=float, default=0.05)
    p.add_argument("--from", dest="date_from", default=None, help="default: first history date")
    p.add_argument("--to", dest="date_to", default=None, help="default: last live date")
    args = p.parse_args(argv)
    cfg = load_config()
    plan = _plan(cfg, args.scale)
    a = d(args.date_from) if args.date_from else plan.start
    b = d(args.date_to) if args.date_to else plan.end
    out = Path(args.out)
    counts: dict[tuple, list] = defaultdict(lambda: [0, 0])
    truth: dict[str, list] = defaultdict(list)
    for rec in generate_twin(cfg, [a + timedelta(days=i) for i in range((b - a).days + 1)], args.scale):
        if rec[0] == "file":
            _, rel, data = rec
            f = out / rel
            f.parent.mkdir(parents=True, exist_ok=True)
            f.write_bytes(data)
            src, day = rel.split("/")[:2]
            counts[(src, day)][0] += 1
            counts[(src, day)][1] += len(data)
        else:
            truth[rec[0]].append(rec[1])
    for kind, recs in truth.items():
        f = out / "_truth" / kind / "part-twin.json"
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text("".join(json.dumps(r, default=str) + "\n" for r in recs))

    def write(rel, text):
        f = out / rel
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(text)

    write_manifests(write, counts)
    s = summary(counts)
    s["truth"] = {k: len(v) for k, v in truth.items()}
    print(json.dumps(s, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
