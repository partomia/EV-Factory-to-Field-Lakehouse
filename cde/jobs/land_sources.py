"""
Stage 0 - Landing: the synthetic source systems drop one business date's files into the landing zone.

  objects   <generated>/<source>/<date>/*                  -> <landing>/<source>/<date>/*
  exports   <generated>/<table>/business_date=<date>/*.csv -> <landing>/<table>/<date>/<table>_<date>.csv

Every <landing>/<source>/<date>/ folder gets _manifest.json: the source's own counts (objects or rows, as the
source reports them) plus name, size and sha256 per file, so bronze can prove what arrived. A re-run replaces the
folder. The EDR dumps sit under their arrival date, so late dumps land late.

Usage:
  spark-submit land_sources.py --business-date 2026-11-06
  spark-submit land_sources.py --from 2026-02-09 --to 2026-11-05          # initial load
  python cde/jobs/land_sources.py --from 2026-02-09 --to 2026-02-12 --generated data/generated --landing data/landing
"""
from __future__ import annotations

import hashlib
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import evx_common as C  # noqa: E402

from generators.run import TABLES  # noqa: E402


def land_date(fs, generated: str, landing: str, day: str, sources: list[str], pool) -> dict:
    out = {}
    for src in sources:
        tabular = src in TABLES
        g = f"{generated}/{src}/business_date={day}" if tabular else f"{generated}/{src}/{day}"
        files = [(n, s) for n, s in fs.list(g) if not n.startswith((".", "_SUCCESS"))]
        if not files:
            continue
        dst = f"{landing}/{src}/{day}"
        fs.delete(dst)
        manifest = json.loads(fs.read_text(f"{g}/_manifest.json")) if any(n == "_manifest.json" for n, _ in files) \
            else {"source": src, "business_date": day, "synthetic": True}
        data_files = [(n, s) for n, s in files if n != "_manifest.json"]
        if tabular:
            data_files = data_files[:1]
            name_of = {data_files[0][0]: f"{src}_{day}.csv"}
        else:
            name_of = {n: n for n, _ in data_files}

        def move(item):
            name, _ = item
            data = fs.read_bytes(f"{g}/{name}")
            fs.write_bytes(f"{dst}/{name_of[name]}", data)
            return {"name": name_of[name], "size": len(data), "sha256": hashlib.sha256(data).hexdigest()}

        manifest["files"] = sorted(pool.map(move, data_files), key=lambda f: f["name"])
        fs.write_text(f"{dst}/_manifest.json", json.dumps(manifest, sort_keys=True, indent=1))
        out[src] = (len(manifest["files"]), sum(f["size"] for f in manifest["files"]))
    return out


def main(argv=None, spark=None) -> int:
    p = C.base_parser(__doc__.split("\n\n")[0])
    p.add_argument("--generated", default=None, help="default: <landing>/../generated")
    p.add_argument("--sources", default="", help="comma-separated subset")
    p.add_argument("--threads", type=int, default=32)
    args = C.parse(p, argv)
    landing = C.as_uri(args.landing)
    generated = C.as_uri(args.generated or C.sibling(args.landing, "generated"))
    if spark is None and not landing.startswith("file:"):
        spark = C.get_spark("evx-land-sources")
    fs = C.filesystem(spark, landing)
    sources = [s for s in args.sources.split(",") if s] or \
        [s for s in fs.list_dirs(generated) if not s.startswith("_")]
    days = [d.isoformat() for d in C.date_range(args.date_from, args.date_to)]
    t0, total = time.time(), [0, 0]
    with ThreadPoolExecutor(max_workers=args.threads) as pool:
        for day in days:
            got = land_date(fs, generated, landing, day, sources, pool)
            n, size = sum(v[0] for v in got.values()), sum(v[1] for v in got.values())
            total[0] += n
            total[1] += size
            print(f"  {day} {len(got):>2} sources {n:>6} files {size / 1e6:8.2f} MB", flush=True)
    print(f"LAND_RESULT dates={len(days)} files={total[0]} mb={total[1] / 1e6:.1f} seconds={time.time() - t0:.0f}",
          flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
