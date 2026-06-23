# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # DB-3 — Bronze -> Silver: `sleep_log`
# MAGIC
# MAGIC ## Layman summary
# MAGIC Same append-only pattern. The interesting validation rule here
# MAGIC isn't a fixed numeric range -- it's a LOGICAL relationship between
# MAGIC columns: a sleep stage (deep sleep, REM) can never be longer than
# MAGIC total sleep time. "The part can't exceed the whole."
# MAGIC
# MAGIC ## Bounds being checked (ported unchanged from validation_rules.py)
# MAGIC total_sleep_hours   0-16                       — beyond 16h continuous
# MAGIC                                                  sleep isn't typical
# MAGIC                                                  outside specific
# MAGIC                                                  medical conditions
# MAGIC deep_sleep_hours    <= total_sleep_hours        — component <= whole
# MAGIC rem_hours           <= total_sleep_hours        — component <= whole

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

CATALOG = "fitness_streaming"
RECORD_TYPE = "sleep_log"

bronze_df = spark.table(f"{CATALOG}.bronze.{RECORD_TYPE}")
print(f"Bronze rows read: {bronze_df.count()}")

# COMMAND ----------

# MAGIC %md ### Step 1 — Dedup on (user_id, timestamp)

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
    "total_sleep_hours": "double", "deep_sleep_hours": "double",
    "rem_hours": "double",
}
SENTINEL_DEFAULTS = {"total_sleep_hours": -1.0, "deep_sleep_hours": -1.0, "rem_hours": -1.0}

for col_name, col_type in EXPECTED_SCHEMA.items():
    if col_name not in df.columns:
        print(f"WARNING: expected column '{col_name}' missing - creating as null")
        df = df.withColumn(col_name, F.lit(None).cast(col_type))
    else:
        df = df.withColumn(col_name, F.col(col_name).cast(col_type))

df = df.fillna(SENTINEL_DEFAULTS)

# COMMAND ----------

# MAGIC %md ### Step 3 — Validate

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

df = apply_rule(df, ~F.col("total_sleep_hours").between(0, 16) | F.col("total_sleep_hours").isNull(), "total_sleep_out_of_range")
df = apply_rule(df, F.col("deep_sleep_hours") > F.col("total_sleep_hours"), "deep_sleep_exceeds_total")
df = apply_rule(df, F.col("rem_hours") > F.col("total_sleep_hours"), "rem_exceeds_total")

valid_count = df.filter(F.col("is_valid")).count()
invalid_count = df.filter(~F.col("is_valid")).count()
print(f"Valid: {valid_count} | Quarantined: {invalid_count}")
display(df.filter(~F.col("is_valid")).groupBy("failure_reason").count())

# COMMAND ----------

# MAGIC %md ### Step 4 — Split, then APPEND to Silver / Quarantine

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

# MAGIC %md ### Verification checkpoint

# COMMAND ----------

silver_total = spark.table(silver_table).count() if spark.catalog.tableExists(silver_table) else 0
quarantine_total = spark.table(quarantine_table).count() if spark.catalog.tableExists(quarantine_table) else 0
print(f"Silver total      : {silver_total}")
print(f"Quarantine total  : {quarantine_total}")
print(f"Combined          : {silver_total + quarantine_total}")
print(f"Deduped Bronze was: {after}")
