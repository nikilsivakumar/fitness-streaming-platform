# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# dependencies = [
#   "faker",
# ]
# ///
# MAGIC %md
# MAGIC # DB-2 — Live Producer (Databricks version)
# MAGIC
# MAGIC ## Why this file exists separately from `wearable_simulator.py`
# MAGIC Same reasoning as `version_aws/producer/kinesis_producer.py`: record
# MAGIC GENERATION logic is infrastructure-independent and must not change
# MAGIC between Local / AWS / Databricks versions — that sameness is the whole
# MAGIC point of the comparative project. This notebook imports the existing
# MAGIC generator functions from `wearable_simulator.py` UNCHANGED, and only
# MAGIC replaces the last step — where the record gets sent — swapping
# MAGIC `bus.produce(...)` (local) / `kinesis.put_records(...)` (AWS) for
# MAGIC "write a Parquet file into a Unity Catalog Volume."
# MAGIC
# MAGIC ## Why this must run AS a Databricks notebook, not a local script
# MAGIC Unity Catalog Volume paths (`/Volumes/...`) only resolve inside
# MAGIC Databricks compute (notebooks, jobs, DLT pipelines). A local script
# MAGIC has no way to write to that path. This was the key architectural
# MAGIC decision flagged for DB-2: clone the GitHub repo as a Databricks Git
# MAGIC folder so the existing `version_local.producer.wearable_simulator`
# MAGIC import works unchanged inside the notebook's Python path.
# MAGIC
# MAGIC ## Why per-record-type subfolders, not one shared folder
# MAGIC Auto Loader (`cloudFiles`) watches one directory and enforces one
# MAGIC schema for it. Five record types have five different schemas — one
# MAGIC shared folder would break schema inference. Five subfolders → five
# MAGIC independent Auto Loader streams, each with its own schema location.
# MAGIC This is the Databricks-native equivalent of Lambda routing records
# MAGIC by `record_type` to separate S3 Bronze prefixes in the AWS version.
# MAGIC
# MAGIC ## Why bounded cycles, not an infinite loop
# MAGIC Free Edition is serverless-only and quota-limited; exceeding quota
# MAGIC suspends the workspace for the rest of the day/month. `kinesis_producer.py`
# MAGIC used a finite `num_cycles` for the identical cost-discipline reason on
# MAGIC AWS. Same pattern here — run it, verify Auto Loader picked the files up,
# MAGIC stop. Re-run the cell manually for more data later rather than leaving
# MAGIC a loop running unattended.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup — point this at your actual Databricks Git folder path
# MAGIC Find it in the Databricks UI under Workspace > Repos (or the new
# MAGIC "Git folders" UI) after cloning the GitHub repo. It typically looks like
# MAGIC `/Workspace/Repos/<your-email>/fitness-streaming-platform` or
# MAGIC `/Workspace/Users/<your-email>/fitness-streaming-platform`.
# MAGIC Update REPO_ROOT below to match — verify by running `%ls <path>` first.

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import sys
import os

REPO_ROOT = "/Workspace/Repos/nikildevikumarit@gmail.com/fitness-streaming-platform"
sys.path.append(REPO_ROOT)

from version_local.producer.wearable_simulator import (
    GENERATORS,
    USER_IDS,
    pick_generator,
)

# COMMAND ----------

import json
import random
import time
import uuid
from datetime import datetime
from collections import defaultdict
import pandas as pd

VOLUME_ROOT = "/Volumes/fitness_streaming/bronze/raw_events"
RECORD_TYPES = [
    "wearable_event",
    "workout_log",
    "sleep_log",
    "nutrition_snapshot",
    "user_profile",
]

# Fail loudly if a record type is missing a generator entry — this is
# exactly the bug class caught in AWS Phase 5 (user_profile silently
# never reaching the round-robin list). Verify the generator list
# covers all 5 types BEFORE running, not after checking Bronze.
_generator_names = {g.__name__.replace("generate_", "") for g, _ in GENERATORS}
_missing = set(RECORD_TYPES) - _generator_names
assert not _missing, f"Missing generators for record types: {_missing}"

# Ensure each record_type subfolder exists under the Volume
for rt in RECORD_TYPES:
    os.makedirs(os.path.join(VOLUME_ROOT, rt), exist_ok=True)

print("Generator coverage check passed:", sorted(_generator_names))
print("Volume subfolders ready under:", VOLUME_ROOT)

# COMMAND ----------

def _write_batch_to_volume(records_by_type: dict, batch_id: int):
    """
    Writes one Parquet file per record_type that had records this batch.
    Filename includes a UTC timestamp + batch_id + uuid suffix so every
    write is a genuinely new file -- required for Auto Loader's
    new-file-detection to have something incremental to detect.
    """
    written = {}
    for record_type, records in records_by_type.items():
        if not records:
            continue
        df = pd.DataFrame(records)
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S%f")
        filename = f"{record_type}_{ts}_batch{batch_id}_{uuid.uuid4().hex[:6]}.parquet"
        path = os.path.join(VOLUME_ROOT, record_type, filename)
        df.to_parquet(path, engine="pyarrow", index=False)
        written[record_type] = (path, len(records))
    return written


def run_producer(num_batches: int = 10, records_per_batch: int = 15, sleep_seconds: float = 2.0):
    """
    Bounded run -- mirrors kinesis_producer.run_producer's finite
    num_cycles design for the same Free-Edition quota-safety reason.

    Args:
        num_batches       : number of write cycles (NOT infinite)
        records_per_batch : records generated per cycle, split across
                             the 5 record types per their natural
                             GENERATORS weighting (same distribution
                             as Local and AWS -- 50/20/15/10/5)
        sleep_seconds      : pause between batches so files land
                             incrementally over real wall-clock time,
                             giving Auto Loader something genuinely
                             incremental to detect (not all-at-once)
    """
    total_written = 0
    type_totals = defaultdict(int)

    print("=" * 60)
    print("Databricks Live Producer")
    print(f"Volume root : {VOLUME_ROOT}")
    print(f"Batches     : {num_batches} x {records_per_batch} records, {sleep_seconds}s apart")
    print("=" * 60)

    for batch_id in range(1, num_batches + 1):
        records_by_type = defaultdict(list)

        for _ in range(records_per_batch):
            user_id = random.choice(USER_IDS)
            generator = pick_generator()
            record = generator(user_id)
            records_by_type[record["record_type"]].append(record)

        written = _write_batch_to_volume(records_by_type, batch_id)

        for rt, (path, count) in written.items():
            type_totals[rt] += count
            total_written += count

        summary = ", ".join(f"{rt}={count}" for rt, (_, count) in written.items())
        print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] Batch {batch_id}/{num_batches} -> {summary}")

        if batch_id < num_batches:
            time.sleep(sleep_seconds)

    print("\nProducer finished.")
    print(f"Total records written : {total_written}")
    for rt in RECORD_TYPES:
        print(f"  {rt:20s}: {type_totals.get(rt, 0)}")

    return total_written, dict(type_totals)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run it
# MAGIC Start small to verify end-to-end before scaling up — same discipline
# MAGIC as every prior phase: verify against real output, don't trust by reading.
# MAGIC After this runs, go check `/Volumes/fitness_streaming/bronze/raw_events/`
# MAGIC with `%fs ls` or `dbutils.fs.ls(...)` and confirm files actually landed
# MAGIC in all 5 subfolders before moving to the Auto Loader notebook.

# COMMAND ----------

total, breakdown = run_producer(num_batches=5, records_per_batch=15, sleep_seconds=2.0)

# COMMAND ----------

total, breakdown = run_producer(num_batches=15, records_per_batch=25, sleep_seconds=1.5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verification checkpoint — run this before declaring DB-2's producer done
# MAGIC List each subfolder and confirm file counts roughly match `breakdown`
# MAGIC above. Don't move to Auto Loader until this matches.

# COMMAND ----------

for rt in RECORD_TYPES:
    files = dbutils.fs.ls(f"{VOLUME_ROOT}/{rt}")
    print(f"{rt:20s}: {len(files)} file(s)")

# COMMAND ----------

import os
for rt in RECORD_TYPES:
    files = os.listdir(f"{VOLUME_ROOT}/{rt}")
    print(f"{rt:20s}: {len(files)} file(s)")
