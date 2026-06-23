# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # DB-3 — Bronze -> Silver: `wearable_event`
# MAGIC
# MAGIC ## Layman summary
# MAGIC Every wearable reading is its own independent fact (a heart-rate
# MAGIC ping at a moment in time) -- never an update to a past reading.
# MAGIC So unlike user_profile, there's no "find the existing row and
# MAGIC update it" step. We just: check each new reading makes physical
# MAGIC sense, sort the good ones into Silver and the bad ones into
# MAGIC Quarantine, and ADD them on -- never touching what's already there.
# MAGIC
# MAGIC ## Why append, not MERGE
# MAGIC MERGE answers "does this key already exist, and if so should I
# MAGIC update it?" -- a meaningful question for user_profile (one row
# MAGIC SHOULD exist per user). It's a meaningless question here: a new
# MAGIC heart-rate reading is never "the same event as" an older one, even
# MAGIC from the same user a second apart. Append is the honest model:
# MAGIC every valid incoming row is new information, full stop.
# MAGIC
# MAGIC ## Why this validation is the ONLY one with intentionally injected
# MAGIC ## bad data (per the original project spec, ported from Local)
# MAGIC wearable_event is the record type with deliberately injected
# MAGIC physiologically impossible values (~5%) -- heart_rate out of
# MAGIC range and non-positive HRV are the two documented failure modes.
# MAGIC Expect to actually SEE quarantined rows here, unlike user_profile
# MAGIC where quarantine was empty.
# MAGIC
# MAGIC ## Why dedup still happens, even though this is append-only
# MAGIC Dedup here guards against a DIFFERENT problem than user_profile's
# MAGIC CDC duplicates: a wearable device or its app can retry sending the
# MAGIC same reading after a flaky network call (at-least-once delivery,
# MAGIC a real and common streaming-systems behavior). Two rows with the
# MAGIC exact same (user_id, timestamp) are almost certainly the same
# MAGIC physical reading sent twice, not two different facts.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

CATALOG = "fitness_streaming"
RECORD_TYPE = "wearable_event"

bronze_df = spark.table(f"{CATALOG}.bronze.{RECORD_TYPE}")
print(f"Bronze rows read: {bronze_df.count()}")

# COMMAND ----------

# MAGIC %md ### Step 1 — Dedup on (user_id, timestamp): keep first occurrence

# COMMAND ----------

w = Window.partitionBy("user_id", "timestamp").orderBy(F.lit(1))
before = bronze_df.count()
df = (
    bronze_df
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)
after = df.count()
print(f"Removed {before - after} duplicate (user_id, timestamp) row(s)")

# COMMAND ----------

# MAGIC %md ### Step 2 — Cast types, sentinel-fill nulls

# COMMAND ----------

EXPECTED_SCHEMA = {
    "user_id": "string", "timestamp": "timestamp",
    "heart_rate": "int", "hrv": "double",
    "steps": "int", "recovery_score": "double",
}
SENTINEL_DEFAULTS = {"heart_rate": -1, "hrv": -1.0, "steps": -1, "recovery_score": -1.0}

for col_name, col_type in EXPECTED_SCHEMA.items():
    if col_name not in df.columns:
        print(f"WARNING: expected column '{col_name}' missing - creating as null")
        df = df.withColumn(col_name, F.lit(None).cast(col_type))
    else:
        df = df.withColumn(col_name, F.col(col_name).cast(col_type))

df = df.fillna(SENTINEL_DEFAULTS)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 — Validate (identical physiological bounds to validation_rules.py)
# MAGIC heart_rate 30-220 bpm | hrv > 0 | steps 0-50000 | recovery_score 0-100

# COMMAND ----------

df = df.withColumn("is_valid", F.lit(True)).withColumn("failure_reason", F.lit(None).cast("string"))

def apply_rule(df, condition_fails, reason_code):
    return df.withColumn(
        "is_valid", F.when(condition_fails, F.lit(False)).otherwise(F.col("is_valid"))
    ).withColumn(
        "failure_reason",
        F.when(condition_fails & F.col("failure_reason").isNull(), F.lit(reason_code))
         .otherwise(F.col("failure_reason"))
    )

df = apply_rule(df, ~F.col("heart_rate").between(30, 220) | F.col("heart_rate").isNull(), "heart_rate_out_of_range")
df = apply_rule(df, (F.col("hrv") <= 0) | F.col("hrv").isNull(), "hrv_negative_or_zero")
df = apply_rule(df, ~F.col("steps").between(0, 50000) | F.col("steps").isNull(), "steps_out_of_range")
df = apply_rule(
    df,
    F.col("recovery_score").isNotNull() & ~F.col("recovery_score").between(0, 100),
    "recovery_score_out_of_range"
)

valid_count = df.filter(F.col("is_valid")).count()
invalid_count = df.filter(~F.col("is_valid")).count()
print(f"Valid: {valid_count} | Quarantined: {invalid_count}")
display(df.filter(~F.col("is_valid")).groupBy("failure_reason").count())

# COMMAND ----------

# MAGIC %md ### Step 4 — Split, then APPEND (not merge) to Silver / Quarantine

# COMMAND ----------

df_silver_incoming = df.filter(F.col("is_valid") == True).drop("is_valid")
df_quarantine_incoming = df.filter(F.col("is_valid") == False).drop("is_valid")

silver_table = f"{CATALOG}.silver.{RECORD_TYPE}"
quarantine_table = f"{CATALOG}.silver.{RECORD_TYPE}_quarantine"

for table, incoming_df in [(silver_table, df_silver_incoming), (quarantine_table, df_quarantine_incoming)]:
    if incoming_df.count() == 0:
        print(f"No rows to write to {table} this run.")
        continue
    if spark.catalog.tableExists(table):
        incoming_df.write.format("delta").mode("append").saveAsTable(table)
    else:
        incoming_df.write.format("delta").saveAsTable(table)
    print(f"{table}: wrote {incoming_df.count()} row(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verification checkpoint
# MAGIC Silver + Quarantine row counts together should equal the deduped
# MAGIC Bronze count from Step 1 -- every row accounted for, none lost.

# COMMAND ----------

silver_total = spark.table(silver_table).count() if spark.catalog.tableExists(silver_table) else 0
quarantine_total = spark.table(quarantine_table).count() if spark.catalog.tableExists(quarantine_table) else 0
print(f"Silver total      : {silver_total}")
print(f"Quarantine total  : {quarantine_total}")
print(f"Combined          : {silver_total + quarantine_total}")
print(f"Deduped Bronze was: {after}")
