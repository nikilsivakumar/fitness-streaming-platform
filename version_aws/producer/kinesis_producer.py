"""
kinesis_producer.py
=======================
AWS Phase 5 — Wearable Simulator -> Kinesis adapter

WHY THIS FILE EXISTS SEPARATELY FROM wearable_simulator.py
-------------------------------------------------------------
The record GENERATION logic (what a wearable_event/workout_log/etc.
looks like) is infrastructure-independent -- it should not change
between local and AWS versions, since the whole point of the
comparative project structure is "same data, same logic, different
plumbing." This file imports the existing generator functions from
wearable_simulator.py unchanged, and only replaces the LAST step --
where the record gets sent -- swapping bus.produce(...) for a
Kinesis put_record / put_records call.

PARTITION KEY CHOICE: user_id
--------------------------------
Kinesis uses the partition key to decide which shard a record goes to,
and guarantees that records sharing a partition key arrive at the
consumer in the order they were sent. Using user_id as the partition
key means all of one user's events stay in relative order -- mirroring
the local StreamBus's _get_partition() behavior (also keyed on a
partition key) and matching real-world IoT/wearable architectures where
per-device event ordering usually matters more than global ordering.
With only 1 shard active right now, every key maps to the same shard
anyway, but writing the code with an explicit partition key (rather
than a random one) is the correct pattern to demonstrate even at
small scale -- it is what would need to change if/when more shards
are added.

WHY put_records (PLURAL, BATCHED) INSTEAD OF put_record IN A LOOP
----------------------------------------------------------------------
put_record sends one record per API call -- at any real volume this
is slow and burns through request-based cost faster than necessary.
put_records sends up to 500 records (max 5MB total) in a single API
call, which is the standard production pattern for batch-style
producers. This script batches records and flushes either when the
batch hits BATCH_SIZE or when the run completes.
"""

import sys
import os
import json
import time
import boto3

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from version_local.producer.wearable_simulator import (
    generate_wearable_event,
    generate_workout_log,
    generate_sleep_log,
    generate_nutrition_snapshot,
    generate_user_profile,
    USER_IDS,
)

STREAM_NAME = os.environ.get("KINESIS_STREAM_NAME", "fitness-wearable-stream")
REGION = os.environ.get("AWS_REGION", "ap-south-1")
BATCH_SIZE = 25  # well under the 500-record / 5MB put_records limit

kinesis = boto3.client("kinesis", region_name=REGION)


def _flush(batch):
    """Send one batch via put_records, retry any individual failures."""
    if not batch:
        return

    entries = [
        {"Data": json.dumps(record).encode("utf-8"), "PartitionKey": record["user_id"]}
        for record in batch
    ]

    response = kinesis.put_records(StreamName=STREAM_NAME, Records=entries)

    failed_count = response.get("FailedRecordCount", 0)
    if failed_count:
        # put_records can partially fail -- some records succeed, some
        # don't, even within one call. This is a normal Kinesis behavior
        # (not an exception), so failures must be checked explicitly
        # rather than relying on a try/except around the whole call.
        print(f"WARNING: {failed_count}/{len(batch)} records failed in this batch, retrying once")
        retry_entries = [
            entries[i] for i, r in enumerate(response["Records"]) if "ErrorCode" in r
        ]
        if retry_entries:
            kinesis.put_records(StreamName=STREAM_NAME, Records=retry_entries)

    print(f"  Sent batch of {len(batch)} records -> Kinesis stream '{STREAM_NAME}'")


def run_producer(num_cycles: int = 5, records_per_cycle: int = 20):
    """
    Generates and sends records to Kinesis for a fixed number of cycles.

    num_cycles, records_per_cycle: kept small and finite (not an infinite
    loop) deliberately -- this is meant for short verification runs given
    the per-shard-hour billing on Kinesis. Run it, verify Lambda picked
    the records up and wrote Bronze to S3, then delete the stream.
    """
    batch = []
    total_sent = 0

    for cycle in range(num_cycles):
        for _ in range(records_per_cycle):
            user_id = USER_IDS[cycle % len(USER_IDS)]
            # Round-robin across the 5 record types so Bronze gets a
            # realistic mix, same as the local simulator's distribution.
            generators = [
                generate_wearable_event,
                generate_workout_log,
                generate_sleep_log,
                generate_nutrition_snapshot,
            ]
            record = generators[_ % len(generators)](user_id)
            batch.append(record)

            if len(batch) >= BATCH_SIZE:
                _flush(batch)
                total_sent += len(batch)
                batch = []

        time.sleep(1)  # small pacing gap between cycles, not required but realistic

    _flush(batch)
    total_sent += len(batch)

    print(f"\nProducer finished. Total records sent: {total_sent}")
    return total_sent


if __name__ == "__main__":
    run_producer(num_cycles=5, records_per_cycle=20)
