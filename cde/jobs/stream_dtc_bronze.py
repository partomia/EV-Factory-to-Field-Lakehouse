"""
Streaming bronze: Kafka topic evx.vehicle.dtc to bronze.vehicle_dtc_event (rejects to bronze.dtc_quarantine), as
a CDE Spark Structured Streaming job (rsingh-evx-stream-dtc); checkpoint <checkpoints>/dtc_bronze. The pattern
of stream_telemetry_bronze.py, through stream_common.py (contract validation, one Iceberg commit per micro-batch
with the evx.stream-batch marker, a replayed batch skipped).

  spark-submit stream_dtc_bronze.py [--source kafka|files] [--input DIR] [--checkpoints URI]
      [--trigger "60 seconds"|available-now] [--starting-offsets earliest|latest] [--await-seconds N]
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import evx_common as C  # noqa: E402
import stream_common as S  # noqa: E402

JOB = "rsingh-evx-stream-dtc"


def run(spark, argv=None):
    return S.run_bronze(spark, JOB, __doc__, argv)


def main() -> int:
    run(C.get_spark("evx-stream-dtc"), sys.argv[1:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
