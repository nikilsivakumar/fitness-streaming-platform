# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # DB-2 (part 2) — Auto Loader Ingestion: Volume -> Bronze Delta
# MAGIC
# MAGIC ## Layman recap of what this notebook does
# MAGIC The producer notebook is the delivery truck dropping boxes (Parquet
# MAGIC files) into 5 labeled drawers (Volume subfolders). THIS notebook is
# MAGIC the warehouse worker standing at the drawers: it watches each drawer,
# MAGIC opens every new box, and stacks the contents onto a conveyor belt
# MAGIC (a Delta table) — while remembering exactly which boxes it already
# MAGIC opened, so nothing gets double-counted even across separate runs.
# MAGIC That "remembering" is the checkpoint location below — it's the
# MAGIC single most important concept in this notebook.
# MAGIC
# MAGIC ## Why 5 separate streams instead of 1
# MAGIC Auto Loader infers and enforces ONE schema per stream. Five record
# MAGIC types have five different shapes (different columns). One shared
# MAGIC stream would either crash on the first schema mismatch or silently
# MAGIC merge incompatible columns. Five independent streams, each pointed
# MAGIC at its own subfolder, is the correct pattern — same reason the
# MAGIC producer wrote to 5 separate subfolders in the first place.
# MAGIC
# MAGIC ## Why checkpoint + schema locations matter (this is the new concept)
# MAGIC Two small folders per record type, neither of which holds your actual
# MAGIC data:
# MAGIC   - **checkpoint location** — Auto Loader's memory of which files it
# MAGIC     has already processed. Without this, every run would re-read
# MAGIC     every file from the beginning -> duplicate rows in Bronze.
# MAGIC   - **schema location** — where Auto Loader stores the schema it
# MAGIC     inferred on first run, so it doesn't have to re-scan and
# MAGIC     re-guess the schema every single run (slow + can drift).
# MAGIC Real production note: in your AWS version, "have I processed this
# MAGIC S3 object already" was handled implicitly by Lambda's event-trigger
# MAGIC model (each S3 PUT fires Lambda exactly once). Auto Loader achieves
# MAGIC the same "process each file exactly once" guarantee, but explicitly,
# MAGIC via this checkpoint -- worth naming directly in interviews as the
# MAGIC Databricks-native answer to the same problem AWS solved differently.
# MAGIC
# MAGIC ## Why `.trigger(availableNow=True)` instead of a continuously running stream
# MAGIC Structured Streaming can run forever, continuously watching for new
# MAGIC files (true for low-latency production use). `availableNow=True`
# MAGIC instead says: "process whatever files exist right now, then stop."
# MAGIC This is the batch-style equivalent of your Glue jobs running once
# MAGIC per session on AWS — appropriate here since Free Edition is
# MAGIC quota-limited and an always-on stream would burn quota for no reason
# MAGIC during development. In a real production job, you'd either run this
# MAGIC on a schedule (via Workflows) with availableNow=True, or remove the
# MAGIC trigger entirely for a truly continuous stream — that contrast is
# MAGIC itself a good interview talking point.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Config — paths and table names

# COMMAND ----------

CATALOG = "fitness_streaming"
VOLUME_ROOT = f"/Volumes/{CATALOG}/bronze/raw_events"

# Checkpoints and inferred-schema storage live INSIDE the existing
# raw_events Volume, as their own subfolders -- NOT as separate
# top-level Volumes. A Volume is a registered Unity Catalog object
# (created via CREATE VOLUME in SQL, like we did for raw_events in
# DB-1) -- os.makedirs() can create subfolders inside an already-
# registered Volume, but it cannot register a brand-new Volume itself.
# That's the exact cause of the "Operation not supported" error: the
# original path tried to treat "_checkpoints" as if it were its own
# Volume, which was never created via CREATE VOLUME.
CHECKPOINT_ROOT = f"/Volumes/{CATALOG}/bronze/raw_events/_checkpoints"
SCHEMA_ROOT = f"/Volumes/{CATALOG}/bronze/raw_events/_schemas"

RECORD_TYPES = [
    "wearable_event",
    "workout_log",
    "sleep_log",
    "nutrition_snapshot",
    "user_profile",
]

import os
for rt in RECORD_TYPES:
    os.makedirs(f"{CHECKPOINT_ROOT}/{rt}", exist_ok=True)
    os.makedirs(f"{SCHEMA_ROOT}/{rt}", exist_ok=True)

print("Checkpoint + schema folders ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run one Auto Loader stream per record type
# MAGIC
# MAGIC Each stream:
# MAGIC 1. Reads new Parquet files from `raw_events/<record_type>/`
# MAGIC 2. Infers schema automatically from the files themselves (`cloudFiles.inferColumnTypes`)
# MAGIC 3. Adds two audit columns -- `_ingested_at` and `_source_file` --
# MAGIC    things you'll want later for debugging ("which file did this
# MAGIC    row come from, and when did it land") but that the raw Parquet
# MAGIC    itself doesn't carry. This is a small but real production habit.
# MAGIC 4. Writes into a managed Delta table under `fitness_streaming.bronze`
# MAGIC
# MAGIC Run sequentially (not all 5 in parallel) -- at this small data volume
# MAGIC there's no performance reason to parallelize, and sequential output
# MAGIC is much easier to read and debug if one record type has an issue.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

results = {}

for rt in RECORD_TYPES:
    source_path = f"{VOLUME_ROOT}/{rt}"
    checkpoint_path = f"{CHECKPOINT_ROOT}/{rt}"
    schema_path = f"{SCHEMA_ROOT}/{rt}"
    target_table = f"{CATALOG}.bronze.{rt}"

    print(f"\n{'='*60}\nIngesting: {rt}\n{'='*60}")

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.inferColumnTypes", "true")
        .load(source_path)
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

    query = (
        df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable(target_table)
    )

    query.awaitTermination()  # blocks until this availableNow batch finishes

    count = spark.table(target_table).count()
    results[rt] = count
    print(f"  -> {target_table} now has {count} total row(s)")

print("\nAll 5 Auto Loader streams completed.")
for rt, count in results.items():
    print(f"  {rt:20s}: {count} rows in fitness_streaming.bronze.{rt}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verification checkpoint — same discipline as every prior phase
# MAGIC Don't trust the print statements above alone. Confirm independently:
# MAGIC 1. Row counts here should be >= what the producer notebook reported
# MAGIC    writing (>= not == is fine/expected if you run the producer again
# MAGIC    later and re-run this notebook -- Auto Loader will pick up only
# MAGIC    the NEW files thanks to the checkpoint, and counts will grow).
# MAGIC 2. Spot-check actual column names and a few rows per table -- this
# MAGIC    is exactly the kind of check that caught the gold_fatigue_recovery
# MAGIC    column-mismatch bug in the AWS version. Don't assume the schema
# MAGIC    matches your mental model -- look.

# COMMAND ----------

for rt in RECORD_TYPES:
    print(f"\n--- {rt} ---")
    spark.table(f"{CATALOG}.bronze.{rt}").printSchema()
    display(spark.table(f"{CATALOG}.bronze.{rt}").limit(3))
