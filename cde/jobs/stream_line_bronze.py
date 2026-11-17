"""
Streaming bronze: Kafka topic evx.factory.line to bronze.factory_line_event (rejects to bronze.line_quarantine),
as a CDE Spark Structured Streaming job (rsingh-evx-stream-line); checkpoint <checkpoints>/line_bronze. The
pattern of stream_telemetry_bronze.py, through stream_common.py; partitions and lateness come from mes_ts (the
trusted MES clock), so a station whose PLC clock is skewed lands in the right hour. stream_line_window.py reads
the table as a stream (maintenance must not expire snapshots past its checkpoint).

  spark-submit stream_line_bronze.py [--source kafka|files] [--input DIR] [--checkpoints URI]
      [--trigger "60 seconds"|available-now] [--starting-offsets earliest|latest] [--await-seconds N]
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import evx_common as C  # noqa: E402
import stream_common as S  # noqa: E402

JOB = "rsingh-evx-stream-line"


def run(spark, argv=None):
    return S.run_bronze(spark, JOB, __doc__, argv)


def main() -> int:
    run(C.get_spark("evx-stream-line"), sys.argv[1:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
