"""
Bronze Writer — consumes from StreamBus and writes partitioned Parquet to disk.

Why Parquet instead of JSON?
    JSON is human-readable but inefficient at scale:
    - No column pruning (must read entire row to get one field)
    - No compression metadata
    - Schema not enforced
    
    Parquet is columnar:
    - Read only columns you need (SELECT heart_rate reads 1 column, not 10)
    - Snappy compression reduces file size 60-80%
    - Schema embedded in file footer
    - Native support in Spark, Glue, Athena, Redshift COPY
    
    This is why every production data lake uses Parquet, not JSON.

Why partition by record_type/year/month/day?
    Partition pruning — when Athena queries:
        WHERE record_type = 'wearable_event' AND year = '2026' AND month = '05'
    It reads ONLY those folders, skipping everything else.
    Without partitioning, it scans the entire lake for every query.
    At scale this is the difference between $0.01 and $50 per query.

Why Bronze is immutable (never modified after write)?
    If your Silver ETL has a bug, you can fix the code and reprocess
    from Bronze. If Bronze was modified or deleted, that data is gone
    forever. Immutability is the foundation of replayable pipelines.
"""

import os
import sys
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from collections import defaultdict
from typing import List

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from version_local.producer.stream_bus import bus, TOPIC

# ── Config ────────────────────────────────────────────────────────────────────

BASE_PATH = os.path.join(
    os.path.dirname(__file__), '..', '..', 'data', 'bronze'
)
CONSUMER_GROUP = "bronze-writer"


# ── Schema Definitions ────────────────────────────────────────────────────────
# Explicit schemas prevent silent type coercion.
# If a field arrives as string instead of int, we catch it here
# rather than propagating corrupt data downstream.

SCHEMAS = {
    "wearable_event": pa.schema([
        pa.field("record_type",    pa.string()),
        pa.field("user_id",        pa.string()),
        pa.field("timestamp",      pa.string()),
        pa.field("heart_rate",     pa.int64()),
        pa.field("steps",          pa.int64()),
        pa.field("hrv",            pa.int64()),
        pa.field("calories_burned",pa.int64()),
        pa.field("recovery_score", pa.int64()),
        pa.field("stress_score",   pa.int64()),
        pa.field("device_type",    pa.string()),
    ]),
    "workout_log": pa.schema([
        pa.field("record_type",      pa.string()),
        pa.field("user_id",          pa.string()),
        pa.field("timestamp",        pa.string()),
        pa.field("workout_type",     pa.string()),
        pa.field("duration_minutes", pa.int64()),
        pa.field("rpe",              pa.int64()),
        pa.field("muscle_groups",    pa.string()),  # serialized list
        pa.field("completed",        pa.bool_()),
        pa.field("gym_id",           pa.string()),
    ]),
    "sleep_log": pa.schema([
        pa.field("record_type",        pa.string()),
        pa.field("user_id",            pa.string()),
        pa.field("timestamp",          pa.string()),
        pa.field("total_sleep_hours",  pa.float64()),
        pa.field("deep_sleep_hours",   pa.float64()),
        pa.field("rem_hours",          pa.float64()),
        pa.field("awakenings",         pa.int64()),
        pa.field("sleep_quality_score",pa.int64()),
    ]),
    "nutrition_snapshot": pa.schema([
        pa.field("record_type",  pa.string()),
        pa.field("user_id",      pa.string()),
        pa.field("timestamp",    pa.string()),
        pa.field("calories_in",  pa.int64()),
        pa.field("protein_g",    pa.int64()),
        pa.field("carbs_g",      pa.int64()),
        pa.field("fat_g",        pa.int64()),
        pa.field("hydration_ml", pa.int64()),
    ]),
    "user_profile": pa.schema([
        pa.field("record_type",    pa.string()),
        pa.field("user_id",        pa.string()),
        pa.field("timestamp",      pa.string()),
        pa.field("age",            pa.int64()),
        pa.field("gender",         pa.string()),
        pa.field("weight_kg",      pa.float64()),
        pa.field("height_cm",      pa.int64()),
        pa.field("medical_history",pa.string()),
        pa.field("job_type",       pa.string()),
        pa.field("fitness_goal",   pa.string()),
        pa.field("gym_id",         pa.string()),
    ]),
}


def _build_partition_path(record_type: str, timestamp_str: str) -> str:
    """
    Builds Hive-style partition path.
    
    Example output:
        data/bronze/record_type=wearable_event/year=2026/month=05/day=22/
    
    Hive-style partitioning (key=value) is the standard because:
    - Athena, Glue, Spark all auto-detect these partitions
    - No manual partition registration needed
    - Partition values are readable in the path itself
    """
    ts = datetime.fromisoformat(timestamp_str)
    return os.path.join(
        BASE_PATH,
        f"record_type={record_type}",
        f"year={ts.strftime('%Y')}",
        f"month={ts.strftime('%m')}",
        f"day={ts.strftime('%d')}"
    )


def _serialize_record(record: dict) -> dict:
    """Converts non-scalar types to strings for Parquet compatibility."""
    serialized = {}
    for k, v in record.items():
        if isinstance(v, list):
            serialized[k] = str(v)  # muscle_groups list → string
        else:
            serialized[k] = v
    return serialized


def write_bronze_batch(records: List[dict]) -> dict:
    """
    Groups records by type, writes each group as a Parquet file
    to its correct partition path.
    
    Returns summary of what was written.
    """
    if not records:
        return {"total_written": 0, "by_type": {}}

    now = datetime.utcnow()
    grouped = defaultdict(list)

    for envelope in records:
        record = envelope["data"]
        record_type = record.get("record_type", "unknown")
        grouped[record_type].append(_serialize_record(record))

    written_summary = {}

    for record_type, recs in grouped.items():
        partition_path = _build_partition_path(
            record_type,
            recs[0]["timestamp"]
        )
        os.makedirs(partition_path, exist_ok=True)

        df = pd.DataFrame(recs)

        # Apply schema if known, otherwise write as-is
        if record_type in SCHEMAS:
            try:
                table = pa.Table.from_pandas(df, schema=SCHEMAS[record_type])
            except Exception as e:
                print(f"Schema mismatch for {record_type}: {e} — writing without schema")
                table = pa.Table.from_pandas(df)
        else:
            table = pa.Table.from_pandas(df)

        filename = f"batch_{now.strftime('%Y%m%d_%H%M%S_%f')}.parquet"
        filepath = os.path.join(partition_path, filename)

        pq.write_table(
            table,
            filepath,
            compression="snappy"  # ~60% size reduction, fast decompress
        )

        written_summary[record_type] = len(recs)
        print(f"  ✓ Bronze: {len(recs):>4} {record_type} → {partition_path}")

    return {
        "total_written": sum(written_summary.values()),
        "by_type": written_summary
    }


def run_bronze_writer(
    poll_interval: float = 2.0,
    max_cycles: int = None,
    silent: bool = False
):
    """
    Continuously polls StreamBus and writes Bronze Parquet files.
    
    poll_interval : seconds between consume calls (mirrors Lambda trigger interval)
    max_cycles    : stop after N cycles (None = run until Ctrl+C)
    """
    if not silent:
        print(f"Bronze writer started | polling every {poll_interval}s")

    cycle = 0
    total_written = 0

    try:
        while True:
            records = bus.consume(
                topic=TOPIC,
                consumer_group=CONSUMER_GROUP,
                max_records=200
            )

            if records:
                if not silent:
                    print(f"\n[{datetime.utcnow().strftime('%H:%M:%S')}] "
                          f"Consumed {len(records)} records:")
                summary = write_bronze_batch(records)
                total_written += summary["total_written"]

            cycle += 1
            if max_cycles and cycle >= max_cycles:
                break

            import time
            time.sleep(poll_interval)

    except KeyboardInterrupt:
        pass

    if not silent:
        print(f"\nBronze writer stopped. Total written: {total_written}")

    return total_written


if __name__ == "__main__":
    run_bronze_writer(poll_interval=2.0)