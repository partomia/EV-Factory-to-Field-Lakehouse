"""
Inline QC verdicts onto evx.factory.qc: one message per inspection verdict row of the business date (X-ray per
cell, film vision per electrode segment), read from the same generator output the batch source carries
(<generated>/cell_qc/<date>/<line>_qc_<shift>.csv, landed as cell_qc and loaded into bronze.cell_qc_inspection).
So the stream and the batch reconcile one to one per business date: count(bronze.factory_qc_event) for a date =
the QC files' row counts = count(bronze.cell_qc_inspection) for that date, keyed on (unit, inspection) on both
sides.

Message: event_id (QC-<unit>-<inspection>), unit_id (cell id, or film segment id), batch_id, line_id,
inspection (XRAY / FILM), station_id, event_ts (inspection time as the file has it, +05:30), verdict, defect_code,
score, bbox, image_ref (null when empty), business_date, source_file, producer_run. Sent at inspection time plus
latency, with the late / out-of-order tails of config/streaming.json producers.qc; per-minute control totals on
evx.control. Live dates only. All data is synthetic.

  python -m stream.producer.qc --out data/stream --generated data/generated --dates 2026-11-06
"""
from __future__ import annotations

import argparse
import sys
import time
import uuid
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from generators.core import rng  # noqa: E402
from stream.producer import common as K  # noqa: E402

P = K.CFG["producers"]["qc"]
HEADER = "CELL_ID|BATCH_ID|LINE|INSPECTION|STATION|INSPECTED_TS|VERDICT|DEFECT_CODE|SCORE|BBOX|IMAGE_REF"


def parse_file(text: str, business_date: str, source_file: str, run_id: str) -> list[dict]:
    """The verdict rows of one QC export (header, rows, TRL|<n> trailer). The trailer is the file's own control
    and is checked by batch bronze; the stream sends the rows it has."""
    lines = [x for x in text.splitlines() if x.strip()]
    if not lines or lines[0] != HEADER:
        raise ValueError(f"{source_file}: unexpected header")
    rows = [x for x in lines[1:] if not x.startswith("TRL|")]
    out = []
    for row in rows:
        unit, batch, line, insp, station, ts, verdict, code, score, bbox, img = row.split("|")
        out.append({"event_id": f"QC-{unit}-{insp}", "unit_id": unit, "batch_id": batch, "line_id": line,
                    "inspection": insp, "station_id": station, "event_ts": ts, "verdict": verdict,
                    "defect_code": code or None, "score": float(score), "bbox": bbox or None, "image_ref": img or None,
                    "business_date": business_date, "source_file": source_file, "producer_run": run_id})
    return out


def records(fs, generated: str, day: date, run_id: str) -> list[dict]:
    """Every verdict row of the date's QC exports. fs: list(uri) -> [(name, size)], read_text(uri)."""
    folder = f"{generated.rstrip('/')}/cell_qc/{day.isoformat()}"
    out = []
    for name, _ in fs.list(folder):
        if name.endswith(".csv"):
            out += parse_file(fs.read_text(f"{folder}/{name}"), day.isoformat(), name, run_id)
    return out


def timed(msgs: list[dict]) -> list[tuple[float, dict]]:
    return [(K.parse_ts(m["event_ts"]).timestamp() + K.delay(rng("qc-stream", m["event_id"]), P), m) for m in msgs]


def backfill(sink, fs, generated: str, dates: list[date], run_id: str, log=print) -> int:
    ctl, sent = K.Control(sink, run_id, P["control_hold_s"], "event_ts"), 0
    for day in dates:
        K.refuse_history(day)
        msgs = records(fs, generated, day, run_id)
        if not msgs:
            raise FileNotFoundError(f"no QC exports for {day} under {generated}/cell_qc/{day}")
        sent += K.send_sorted(sink, ctl, K.TOPIC["qc"], timed(msgs))
        log(f"  {day} {len(msgs):,} QC verdicts ({sum(1 for m in msgs if m['verdict'] == 'FAIL'):,} FAIL) "
            f"from {len({m['source_file'] for m in msgs})} files")
    ctl.flush(0, final=True)
    return sent


class LocalFiles:
    @staticmethod
    def list(uri: str) -> list[tuple[str, int]]:
        p = Path(uri)
        return sorted((f.name, f.stat().st_size) for f in p.iterdir() if f.is_file()) if p.is_dir() else []

    @staticmethod
    def read_text(uri: str) -> str:
        return Path(uri).read_text("utf-8")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default=str(ROOT / "data" / "stream"))
    ap.add_argument("--generated", default=str(ROOT / "data" / "generated"))
    ap.add_argument("--dates", default=",".join(K.LH["calendar"]["live_dates"][:1]))
    args = ap.parse_args(argv)
    run_id = f"backfill-{uuid.uuid4().hex[:8]}"
    sink, t0 = K.FileSink(args.out, "qc"), time.time()
    try:
        backfill(sink, LocalFiles(), args.generated, [date.fromisoformat(x) for x in args.dates.split(",")], run_id)
    finally:
        sink.close()
    print(f"sent {sink.sent:,} messages in {time.time() - t0:.0f} s (run {run_id})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
