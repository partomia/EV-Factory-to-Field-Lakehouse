"""
Runs the synthetic source systems for the whole calendar (history + live dates) into a `generated/` area,
which the land stage then releases into landing one business date at a time.

  python -m generators.run --out data/generated --scale 0.05          local (CI scale)
  cde/jobs/generate.py                                                CDE (default scale), same records

Layout under <out>:
  <source>/<business_date>/<object>              objects (CSV, NDJSON, JSON pages, PNG, PDF, JDX, EDR)
  <table>/business_date=<date>/part-*.csv        legacy tabular exports (telematics_daily, dtc_history,
                                                 warranty_claims, dms_riders), header row, all strings
  <source>/<business_date>/_manifest.json        object / row counts per source and date
  _truth/<kind>/part-*.json                      generator truth (truth_pack, truth_batch) and fleet state:
                                                 never read by features (tests/test_no_truth_in_features.py)

Every value is synthetic and seeded: the same (seed, scale) gives the same bytes however the work is split.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from generators.core import batches_of, build_plan, d, load_config, partitions  # noqa: E402
from generators.factory import generate_partition  # noqa: E402
from generators.field import CLAIM_COLS, DTC_COLS, RIDER_COLS, TELEMATICS_COLS, generate_vehicles  # noqa: E402
from generators.supply import generate_supply  # noqa: E402

TABLES = {
    "telematics_daily": (TELEMATICS_COLS, "SUMMARY_DATE"),
    "dtc_history": (DTC_COLS, "EVENT_TS"),
    "warranty_claims": (CLAIM_COLS, "CLAIM_DATE"),
    "dms_riders": (RIDER_COLS, "DELIVERY_DATE"),
}
SUPPLY_KEY = ("SUPPLY", "-")


def prepare(cfg: dict, scale: float) -> tuple[dict, list[tuple[str, str]]]:
    """The config every partition sees (with the live-subset sampling rate) and the partition keys."""
    plan = build_plan(cfg, scale)
    lo, hi = (d(x) for x in cfg["universe"]["live_subset_delivered"])
    lag = cfg["universe"]["pack_to_vehicle_eol_days"] + 3 + 4.5
    expected = sum(plan.packs_per_day(lo + timedelta(days=i) - timedelta(days=int(lag)))
                   for i in range((hi - lo).days + 1)) * 0.99
    cfg = dict(cfg)
    cfg["_live_subset_p"] = min(1.0, cfg["universe"]["live_subset_vehicles"] / max(expected, 1.0))
    cfg["_scale"] = scale
    return cfg, [SUPPLY_KEY] + partitions(plan)


def partition_records(cfg: dict, key: tuple[str, str]):
    plan = build_plan(cfg, cfg.get("_scale", 1.0))
    if key == SUPPLY_KEY:
        yield from generate_supply(plan)
        return
    packs = []
    for rec in generate_partition(plan, batches_of(plan, *key)):
        if rec[0] == "pack":
            packs.append(rec[1])
        else:
            yield rec
    yield from generate_vehicles(plan, packs)


def row_date(table: str, row: tuple) -> str:
    cols, dcol = TABLES[table]
    return str(row[cols.index(dcol)])[:10]


def as_strings(row: tuple) -> tuple:
    return tuple(None if v is None else ("true" if v is True else "false" if v is False else str(v)) for v in row)


# ---------------------------------------------------------------- local mode


def run_local(out: Path, cfg: dict, keys) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    counts: dict[tuple, list] = defaultdict(lambda: [0, 0])
    rows: dict[tuple, list] = defaultdict(list)
    truth: dict[str, list] = defaultdict(list)
    for key in keys:
        for rec in partition_records(cfg, key):
            kind = rec[0]
            if kind == "file":
                _, rel, data = rec
                p = out / rel
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_bytes(data)
                src, day = rel.split("/")[:2]
                counts[(src, day)][0] += 1
                counts[(src, day)][1] += len(data)
            elif kind == "row":
                _, table, row = rec
                rows[(table, row_date(table, row))].append(as_strings(row))
            else:
                truth[kind].append(rec[1])
    for (table, day), rs in rows.items():
        p = out / table / f"business_date={day}" / "part-00000.csv"
        p.parent.mkdir(parents=True, exist_ok=True)
        buf = io.StringIO()
        w = csv.writer(buf, lineterminator="\n")
        w.writerow(TABLES[table][0])
        w.writerows(rs)
        p.write_text(buf.getvalue())
        counts[(table, day)][0] += 1
        counts[(table, day)][1] += len(rs)
    for kind, recs in truth.items():
        p = out / "_truth" / kind / "part-00000.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("".join(json.dumps(r, default=str) + "\n" for r in recs))
    write_manifests(lambda rel, text: _local_write(out / rel, text), counts)
    return summary(counts, truth)


def _local_write(p: Path, text: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)


def write_manifests(write, counts: dict) -> None:
    for (src, day), (n, size) in sorted(counts.items()):
        tabular = src in TABLES
        rel = f"{src}/business_date={day}/_manifest.json" if tabular else f"{src}/{day}/_manifest.json"
        body = {"source": src, "business_date": day, "synthetic": True,
                ("rows" if tabular else "objects"): (size if tabular else n)}
        if not tabular:
            body["bytes"] = size
        write(rel, json.dumps(body, sort_keys=True))


def summary(counts: dict, truth: dict | None = None) -> dict:
    by_source = defaultdict(lambda: [0, 0])
    for (src, _), (n, size) in counts.items():
        by_source[src][0] += n
        by_source[src][1] += size
    out = {"sources": {k: {"objects_or_files": v[0], "bytes_or_rows": v[1]} for k, v in sorted(by_source.items())}}
    if truth:
        packs = truth.get("truth_pack", [])
        out["truth"] = label_stats(packs)
    return out


def label_stats(packs: list[dict], history_end: str = "2026-11-05", label_days: int = 90) -> dict:
    cutoff = (d(history_end) - timedelta(days=label_days)).isoformat()
    delivered = [p for p in packs if p.get("delivery_date")]
    matured = [p for p in delivered if p["delivery_date"] <= cutoff]
    pos = [p for p in matured if p["positive"] and p["event_day"] <= label_days]
    return {"packs": len(packs), "delivered": len(delivered), "matured": len(matured), "positives": len(pos),
            "rate": round(len(pos) / max(len(matured), 1), 4),
            "live_subset": sum(1 for p in delivered if p.get("live_subset")),
            "by_cause": {c: sum(1 for p in pos if p["event_cause"] == c)
                         for c in ("contaminated", "drift", "overhang", "weak_bond", "usage")}}


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--out", default="data/generated")
    p.add_argument("--scale", type=float, default=0.05)
    p.add_argument("--only", default=None, help="comma list of partition keys line:month (debug)")
    args = p.parse_args(argv)
    cfg, keys = prepare(load_config(), args.scale)
    if args.only:
        want = {tuple(x.split(":")) for x in args.only.split(",")}
        keys = [k for k in keys if k in want]
    print(json.dumps(run_local(Path(args.out), cfg, keys), indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
