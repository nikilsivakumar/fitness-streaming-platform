"""
lambda_function.py
=====================
AWS Phase 5 — Kinesis -> Lambda -> S3 Bronze
Fitness Streaming Analytics Platform — AWS Version

WHAT THIS REPLACES (local -> AWS mapping)
------------------------------------------
Local version_local/consumer/bronze_writer.py polls StreamBus every 2s
and writes partitioned Snappy Parquet to disk. This Lambda function does
the AWS-managed equivalent: it is triggered automatically by Kinesis
(via an event source mapping) whenever new records land on the shard,
and writes the same partitioned Snappy Parquet structure to S3 instead
of local disk.

WHY THE SCHEMAS AND PARTITIONING LOGIC ARE COPIED EXACTLY FROM
version_local/consumer/bronze_writer.py, NOT REDESIGNED
----------------------------------------------------------------
The whole point of the three-version comparison (per the project compact)
is proving the same Bronze layer contract holds regardless of *how* data
arrives (local queue vs managed stream). Changing the schema or partition
scheme here would break that comparison and make AWS Bronze incompatible
with the Glue Silver/Gold jobs that expect the same column names. The
PyArrow schemas, the muscle_groups list->string serialization, and the
Hive-style record_type=X/year=Y/month=M/day=D partitioning are all
copied verbatim from the verified local implementation.

KINESIS EVENT SHAPE
--------------------
Each Lambda invocation receives event["Records"], a list of Kinesis
records. Each record's actual payload is base64-encoded in
record["kinesis"]["data"] and must be decoded + JSON-parsed to recover
the original dict the producer sent. A single Lambda invocation can
contain a BATCH of Kinesis records (controlled by the event source
mapping's batch size), so this function groups the whole batch by
record_type before writing -- mirroring write_bronze_batch()'s grouping
behavior locally, and avoiding one-Parquet-file-per-record (which would
be a small-files problem at any real scale).

WHY A LAMBDA LAYER FOR pyarrow/pandas
---------------------------------------
pandas and pyarrow are not in the default Lambda Python runtime and
exceed the 250MB unzipped deployment package limit if bundled directly
for some platforms. They must be attached as a Lambda Layer (a zip of
the installed packages) built for the Lambda execution environment's
architecture (x86_64 or arm64) and Python version. The setup script in
this folder handles building that layer via a Docker-free, manylinux
wheel download approach suitable for Windows.

IDEMPOTENCY NOTE (interview-relevant)
----------------------------------------
Kinesis can redeliver a batch on Lambda retry (e.g. if the function
times out after partially succeeding). This function does NOT dedupe at
write time -- file names are timestamp+uuid-based, so retries create
additional (duplicate) Bronze files rather than overwriting or corrupting
existing ones. This mirrors the project's "never silently drop, audit
everything" philosophy: a retry-induced duplicate is a Bronze-layer
problem to catch via row-count monitoring or a downstream dedup step
(which Phase 3's bronze_to_silver.py already does --
.dropDuplicates(["user_id", "timestamp"]) -- so duplicates from Lambda
retries are naturally absorbed at the Silver layer, not silently lost,
but also not allowed to double-count).
"""

import base64
import json
import os
import uuid
from collections import defaultdict
from datetime import datetime

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

s3 = boto3.client("s3")

BUCKET = os.environ.get("BRONZE_BUCKET", "fitness-streaming-nikil-2026")
BRONZE_PREFIX = "bronze"

# ---------------------------------------------------------------------------
# SCHEMAS - copied verbatim from version_local/consumer/bronze_writer.py
# ---------------------------------------------------------------------------
SCHEMAS = {
    "wearable_event": pa.schema([
        pa.field("record_type",     pa.string()),
        pa.field("user_id",         pa.string()),
        pa.field("timestamp",       pa.string()),
        pa.field("heart_rate",      pa.int64()),
        pa.field("steps",           pa.int64()),
        pa.field("hrv",             pa.int64()),
        pa.field("calories_burned", pa.int64()),
        pa.field("recovery_score",  pa.int64()),
        pa.field("stress_score",    pa.int64()),
        pa.field("device_type",     pa.string()),
    ]),
    "workout_log": pa.schema([
        pa.field("record_type",      pa.string()),
        pa.field("user_id",          pa.string()),
        pa.field("timestamp",        pa.string()),
        pa.field("workout_type",     pa.string()),
        pa.field("duration_minutes", pa.int64()),
        pa.field("rpe",              pa.int64()),
        pa.field("muscle_groups",    pa.string()),  # serialized list, same as local
        pa.field("completed",        pa.bool_()),
        pa.field("gym_id",           pa.string()),
    ]),
    "sleep_log": pa.schema([
        pa.field("record_type",         pa.string()),
        pa.field("user_id",             pa.string()),
        pa.field("timestamp",           pa.string()),
        pa.field("total_sleep_hours",   pa.float64()),
        pa.field("deep_sleep_hours",    pa.float64()),
        pa.field("rem_hours",           pa.float64()),
        pa.field("awakenings",          pa.int64()),
        pa.field("sleep_quality_score", pa.int64()),
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
        pa.field("record_type",     pa.string()),
        pa.field("user_id",         pa.string()),
        pa.field("timestamp",       pa.string()),
        pa.field("age",             pa.int64()),
        pa.field("gender",          pa.string()),
        pa.field("weight_kg",       pa.float64()),
        pa.field("height_cm",       pa.int64()),
        pa.field("medical_history", pa.string()),
        pa.field("job_type",        pa.string()),
        pa.field("fitness_goal",    pa.string()),
        pa.field("gym_id",          pa.string()),
    ]),
}


def _serialize_record(record: dict) -> dict:
    """Converts non-scalar types (e.g. muscle_groups list) to strings,
    identical to the local _serialize_record in bronze_writer.py."""
    serialized = {}
    for k, v in record.items():
        if isinstance(v, list):
            serialized[k] = str(v)
        else:
            serialized[k] = v
    return serialized


def _build_partition_key(record_type: str, timestamp_str: str) -> str:
    """
    Hive-style S3 key prefix, matching the local partition path exactly:
        bronze/record_type=wearable_event/year=2026/month=05/day=22/
    """
    ts = datetime.fromisoformat(timestamp_str)
    return (
        f"{BRONZE_PREFIX}/record_type={record_type}/"
        f"year={ts.strftime('%Y')}/month={ts.strftime('%m')}/day={ts.strftime('%d')}"
    )


def lambda_handler(event, context):
    """
    Entry point. Triggered by the Kinesis event source mapping.

    event["Records"] is a list of Kinesis records in this invocation's
    batch. Each one is base64-decoded and JSON-parsed to recover the
    original producer dict, then grouped by record_type and written as
    one Parquet file per record_type per invocation -- mirroring
    write_bronze_batch()'s grouping behavior locally.
    """
    grouped = defaultdict(list)
    decode_errors = 0

    for kinesis_record in event.get("Records", []):
        try:
            payload_b64 = kinesis_record["kinesis"]["data"]
            payload_json = base64.b64decode(payload_b64).decode("utf-8")
            record = json.loads(payload_json)
        except Exception as e:
            # Malformed payload -- log and skip rather than crash the
            # whole batch. A poison-pill record should not block every
            # other valid record in the same invocation.
            print(f"WARNING: failed to decode/parse Kinesis record: {e}")
            decode_errors += 1
            continue

        record_type = record.get("record_type", "unknown")
        grouped[record_type].append(_serialize_record(record))

    written_summary = {}

    for record_type, recs in grouped.items():
        if not recs:
            continue

        partition_key = _build_partition_key(record_type, recs[0]["timestamp"])
        df = pd.DataFrame(recs)

        if record_type in SCHEMAS:
            try:
                table = pa.Table.from_pandas(df, schema=SCHEMAS[record_type])
            except Exception as e:
                print(f"Schema mismatch for {record_type}: {e} - writing without schema")
                table = pa.Table.from_pandas(df)
        else:
            table = pa.Table.from_pandas(df)

        # Write to /tmp first (Lambda's only writable local filesystem),
        # then upload to S3 -- there is no direct "write parquet to S3"
        # in pyarrow without s3fs, and avoiding that extra dependency
        # keeps the Lambda layer smaller.
        filename = f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}.parquet"
        local_path = f"/tmp/{filename}"

        pq.write_table(table, local_path, compression="snappy")

        s3_key = f"{partition_key}/{filename}"
        s3.upload_file(local_path, BUCKET, s3_key)
        os.remove(local_path)

        written_summary[record_type] = len(recs)
        print(f"  Bronze: {len(recs):>4} {record_type} -> s3://{BUCKET}/{s3_key}")

    result = {
        "total_written": sum(written_summary.values()),
        "by_type": written_summary,
        "decode_errors": decode_errors,
    }
    print(f"Invocation summary: {json.dumps(result)}")
    return result
