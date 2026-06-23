# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # DB-3 — Bronze -> Silver: `user_profile` via MERGE INTO
# MAGIC
# MAGIC ## Layman recap of what this notebook does
# MAGIC Bronze has the same user appearing more than once, with different
# MAGIC details each time (CDC-style updates, by design). This notebook:
# MAGIC 1. Looks at all the rows for each user, keeps only the NEWEST one
# MAGIC 2. Checks that the data makes sense (age 13-100, weight 30-250kg)
# MAGIC    -- the SAME rule you already wrote for Local and AWS, ported
# MAGIC    unchanged, because validation logic shouldn't depend on the
# MAGIC    infrastructure underneath it
# MAGIC 3. Splits into "good" (Silver) and "bad" (Quarantine) -- nothing
# MAGIC    silently disappears, every row ends up somewhere explainable
# MAGIC 4. Instead of rewriting the whole Silver table every run (what
# MAGIC    Local/AWS do), uses MERGE INTO to update only the rows that
# MAGIC    actually changed and insert only the rows that are new
# MAGIC
# MAGIC ## Why MERGE INTO is the real new concept here, not just new syntax
# MAGIC Local & AWS Glue pattern: read ALL of Bronze -> transform -> write
# MAGIC mode("overwrite") -> the ENTIRE Silver table gets rewritten every
# MAGIC single run, even rows that didn't change. Fine at small/medium
# MAGIC scale, wasteful and slow at real scale.
# MAGIC
# MAGIC Delta MERGE INTO pattern: for each incoming row, ask "does a row
# MAGIC with this user_id already exist in Silver?"
# MAGIC   - YES -> UPDATE just that row's columns
# MAGIC   - NO  -> INSERT it as a new row
# MAGIC Untouched existing rows are never rewritten at all. This is only
# MAGIC possible because Delta tables support row-level transactions
# MAGIC (ACID) -- Parquet alone (what Glue writes) does not support this;
# MAGIC that's WHY Glue has to do full-partition overwrites instead.
# MAGIC
# MAGIC ## Why dedup happens BEFORE the merge, not instead of it
# MAGIC If we merged every Bronze row for U1041 one at a time, the merge
# MAGIC would apply the OLDER row, then immediately apply the NEWER row on
# MAGIC top of it in the same batch -- harmless here since the newer one
# MAGIC wins either way, but wasteful and, in trickier cases (e.g. if two
# MAGIC source rows for the same key conflict on a column merge logic can't
# MAGIC resolve), MERGE INTO will actually throw an error if a batch has
# MAGIC multiple matches for the same key against a single target row in
# MAGIC certain join conditions. Deduplicating the incoming batch down to
# MAGIC one row per user_id BEFORE merging avoids that entirely and matches
# MAGIC exactly the row_number() dedup already used in Local and AWS Glue.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 — Read Bronze, keep only each user's newest row
# MAGIC Same row_number()-over-timestamp pattern as Local's bronze_to_silver.py
# MAGIC and the AWS Glue gold_user_profile_enriched fix -- ranking each
# MAGIC user_id's rows by timestamp descending and keeping rank 1.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

CATALOG = "fitness_streaming"

bronze_df = spark.table(f"{CATALOG}.bronze.user_profile")
print(f"Bronze rows read: {bronze_df.count()}")

w = Window.partitionBy("user_id").orderBy(F.col("timestamp").desc())

bronze_latest = (
    bronze_df
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

print(f"After keeping latest-per-user: {bronze_latest.count()} rows")
display(bronze_latest.select("user_id", "timestamp", "age", "weight_kg"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 — Cast types, apply sentinel defaults
# MAGIC Same defensive philosophy as Local: a missing/null age or weight_kg
# MAGIC is still a row worth keeping (not dropping), so we fill nulls with
# MAGIC an out-of-range sentinel value (-1) -- the validation rule below
# MAGIC will then correctly catch it and route it to quarantine with a
# MAGIC clear reason, instead of the row silently vanishing during a cast.

# COMMAND ----------

EXPECTED_SCHEMA = {
    "user_id": "string", "timestamp": "timestamp",
    "age": "int", "weight_kg": "double",
    "medical_history": "string", "job_type": "string",
    "fitness_goal": "string",
}
SENTINEL_DEFAULTS = {"age": -1, "weight_kg": -1.0}

df = bronze_latest
for col_name, col_type in EXPECTED_SCHEMA.items():
    if col_name not in df.columns:
        print(f"WARNING: expected column '{col_name}' missing from Bronze - creating as null")
        df = df.withColumn(col_name, F.lit(None).cast(col_type))
    else:
        df = df.withColumn(col_name, F.col(col_name).cast(col_type))

df = df.fillna(SENTINEL_DEFAULTS)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 — Validate
# MAGIC IDENTICAL rule to validation_rules.py's validate_user_profile --
# MAGIC age must be 13-100, weight_kg must be 30-250 if present. Ported
# MAGIC unchanged on purpose: the rule describing "what a valid human looks
# MAGIC like" doesn't change because the infrastructure underneath changed.

# COMMAND ----------

df = (
    df
    .withColumn("is_valid", F.lit(True))
    .withColumn("failure_reason", F.lit(None).cast("string"))
)

df = df.withColumn(
    "is_valid",
    F.when(~F.col("age").between(13, 100) | F.col("age").isNull(), F.lit(False))
     .otherwise(F.col("is_valid"))
).withColumn(
    "failure_reason",
    F.when(
        (~F.col("age").between(13, 100) | F.col("age").isNull()) & F.col("failure_reason").isNull(),
        F.lit("age_out_of_range")
    ).otherwise(F.col("failure_reason"))
)

df = df.withColumn(
    "is_valid",
    F.when(
        F.col("weight_kg").isNotNull() & ~F.col("weight_kg").between(30, 250),
        F.lit(False)
    ).otherwise(F.col("is_valid"))
).withColumn(
    "failure_reason",
    F.when(
        (F.col("weight_kg").isNotNull() & ~F.col("weight_kg").between(30, 250)) & F.col("failure_reason").isNull(),
        F.lit("weight_out_of_range")
    ).otherwise(F.col("failure_reason"))
)

valid_count = df.filter(F.col("is_valid")).count()
invalid_count = df.filter(~F.col("is_valid")).count()
print(f"Valid: {valid_count} | Quarantined: {invalid_count}")
display(df.filter(~F.col("is_valid")).select("user_id", "age", "weight_kg", "failure_reason"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 — Split into Silver-bound and Quarantine-bound DataFrames

# COMMAND ----------

df_silver_incoming = df.filter(F.col("is_valid") == True).drop("is_valid")
df_quarantine_incoming = df.filter(F.col("is_valid") == False).drop("is_valid")

print(f"Silver-bound this run     : {df_silver_incoming.count()}")
print(f"Quarantine-bound this run : {df_quarantine_incoming.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 — MERGE INTO Silver (the new part)
# MAGIC First run: table doesn't exist yet, so we create it directly from
# MAGIC the incoming data. Every subsequent run: MERGE -- existing user_id
# MAGIC gets UPDATED in place, new user_id gets INSERTED. Re-running this
# MAGIC notebook again right now (no new Bronze data) should update 0 rows
# MAGIC and insert 0 rows -- nothing changed, so nothing should move. THAT
# MAGIC idempotency is itself worth checking and naming as a deliberate
# MAGIC verification, not just "it ran."

# COMMAND ----------

silver_table = f"{CATALOG}.silver.user_profile"
table_exists = spark.catalog.tableExists(silver_table)

if not table_exists:
    print(f"{silver_table} does not exist yet - creating from this run's data")
    df_silver_incoming.write.format("delta").saveAsTable(silver_table)
    print(f"Created {silver_table} with {df_silver_incoming.count()} rows")
else:
    from delta.tables import DeltaTable

    silver_dt = DeltaTable.forName(spark, silver_table)

    (
        silver_dt.alias("target")
        .merge(
            df_silver_incoming.alias("source"),
            "target.user_id = source.user_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    history = spark.sql(f"DESCRIBE HISTORY {silver_table} LIMIT 1")
    display(history.select("version", "timestamp", "operation", "operationMetrics"))

print(f"\n{silver_table} now has {spark.table(silver_table).count()} total row(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 — Write Quarantine (kept simple: overwrite, not merged)
# MAGIC Quarantine isn't a "current state per user" table the way Silver is
# MAGIC -- it's an audit log of bad records over time, so append makes more
# MAGIC sense than merge here (no natural "update in place" concept for a
# MAGIC quarantined record - it's a historical fact, not a current state).

# COMMAND ----------

quarantine_table = f"{CATALOG}.silver.user_profile_quarantine"

if df_quarantine_incoming.count() > 0:
    if spark.catalog.tableExists(quarantine_table):
        df_quarantine_incoming.write.format("delta").mode("append").saveAsTable(quarantine_table)
    else:
        df_quarantine_incoming.write.format("delta").saveAsTable(quarantine_table)
    print(f"Quarantine: {df_quarantine_incoming.count()} row(s) written to {quarantine_table}")
else:
    print("No quarantine records this run.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verification checkpoint
# MAGIC 1. Row count in Silver should equal number of DISTINCT user_id in
# MAGIC    Bronze that passed validation -- i.e. one row per user, no
# MAGIC    duplicates carried through. Check this explicitly, don't assume.
# MAGIC 2. Re-run this whole notebook a second time with no new Bronze data
# MAGIC    and confirm the MERGE step reports 0 updated / 0 inserted --
# MAGIC    proves the merge is idempotent, a real production concern.

# COMMAND ----------

print("Distinct users in Silver:", spark.table(silver_table).select("user_id").distinct().count())
print("Total rows in Silver:    ", spark.table(silver_table).count())
display(spark.table(silver_table).orderBy("user_id"))
