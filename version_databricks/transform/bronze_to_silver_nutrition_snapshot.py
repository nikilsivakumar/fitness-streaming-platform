# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # DB-3 — Bronze -> Silver: `nutrition_snapshot`
# MAGIC
# MAGIC ## Layman summary
# MAGIC Same append-only pattern as the other event-style record types.
# MAGIC Each meal/nutrition log is its own independent fact.
# MAGIC
# MAGIC ## Bounds being checked (ported unchanged from validation_rules.py)
# MAGIC calories_in   0-10000   — covers even extreme bulk/competitive eating
# MAGIC                          ranges without admitting clearly erroneous
# MAGIC                          values
# MAGIC protein_g     0-500
# MAGIC hydration_ml  0-10000

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

CATALOG = "fitness_streaming"
RECORD_TYPE = "nutrition_snapshot"

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
    "calories_in": "int", "protein_g": "double",
    "carbs_g": "double", "hydration_ml": "double",
}
SENTINEL_DEFAULTS = {"calories_in": -1, "protein_g": -1.0, "carbs_g": -1.0, "hydration_ml": -1.0}

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

df = apply_rule(df, ~F.col("calories_in").between(0, 10000) | F.col("calories_in").isNull(), "calories_out_of_range")
df = apply_rule(df, F.col("protein_g").isNotNull() & ~F.col("protein_g").between(0, 500), "protein_out_of_range")
df = apply_rule(df, F.col("hydration_ml").isNotNull() & ~F.col("hydration_ml").between(0, 10000), "hydration_out_of_range")

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
