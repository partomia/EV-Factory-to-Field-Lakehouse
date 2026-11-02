"""
Publish a pack_risk_eol score parquet into the lakehouse.

The parquet is produced by usecases/uc01_pack_risk/score.py (the model never enters git).
Mount it as a files resource and pass its path. This job checks the feature-hash lock again,
then writes gold.uc01_pack_risk_score, gold.model_score and ref.guardrail_event.

  spark-submit cde/jobs/publish_uc01_scores.py --input /app/mount/scores/scores.parquet
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import evx_common as C  # noqa: E402
from usecases.uc01_pack_risk.score import ScoreRefused, publish_scores  # noqa: E402


def run(spark, argv=None) -> dict:
    p = C.base_parser(__doc__.split("\n\n")[0], dated=False)
    p.add_argument("--input", required=True)
    args = C.parse(p, argv)
    return publish_scores(spark, args.input, args.overlay)


def main() -> int:
    spark = C.get_spark("evx-uc01-score")
    try:
        run(spark, sys.argv[1:])
    except ScoreRefused as e:
        print(f"SCORE_REFUSED {e}", flush=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
