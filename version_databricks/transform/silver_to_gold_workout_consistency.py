# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # DB-4 — Silver -> Gold: `gold_workout_consistency`
# MAGIC
# MAGIC ## Layman recap
# MAGIC Per user: how many workouts, how many completed, average effort,
# MAGIC total training load, and a breakdown of which muscle groups got
# MAGIC trained how often. Identical aggregation logic to Local's
# MAGIC build_gold_workout_consistency.
# MAGIC
# MAGIC ## Databricks-specific adaptation: muscle_groups parsing
# MAGIC Verified against real Silver output before writing this (not assumed):
# MAGIC values look like "[arms]" / "[shoulders, core, chest]" -- Spark's
# MAGIC array-to-string format from DB-3's `.cast("string")` step, NOT Local's
# MAGIC Python-list-literal string ("['back', 'core']"). No quotes to swap,
# MAGIC so Local's regexp_replace(quotes)+from_json approach doesn't apply --
# MAGIC parsed here by stripping brackets and splitting on ", " instead.
# MAGIC Same end result (a real array column), different parse mechanics for
# MAGIC a different string format -- worth naming as its own interview point,
# MAGIC distinct from the reference-enrichment gap found earlier.
# MAGIC
# MAGIC ## Overwrite-on-recompute
# MAGIC No stable merge key for a per-user summary aggregate -- full recompute
# MAGIC from Silver every run, same pattern as the other 3 Gold tables.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "fitness_streaming"
gold_table = f"{CATALOG}.gold.workout_consistency"

# COMMAND ----------

# MAGIC %md ### Step 1 — Read Silver, parse muscle_groups into a real array

# COMMAND ----------

workout_df = spark.table(f"{CATALOG}.silver.workout_log")
print(f"Silver rows read: {workout_df.count()}")

df = (
    workout_df
    .withColumn("muscle_groups_clean", F.regexp_replace(F.col("muscle_groups"), r"[\[\]]", ""))
    .withColumn("muscle_groups_array", F.split(F.col("muscle_groups_clean"), ", "))
)

display(df.select("user_id", "muscle_groups", "muscle_groups_array").limit(5))

# COMMAND ----------

# MAGIC %md ### Step 2 — Per-user base aggregates (identical to Local)

# COMMAND ----------

df = df.withColumn("session_load", F.col("duration_minutes") * F.col("rpe"))

base = (
    df.groupBy("user_id")
    .agg(
        F.count("*").alias("total_sessions"),
        F.sum(F.col("completed").cast("int")).alias("completed_sessions"),
        F.round(F.avg("rpe"), 2).alias("avg_rpe"),
        F.round(F.avg("duration_minutes"), 2).alias("avg_duration_minutes"),
        F.round(F.sum("session_load"), 2).alias("total_training_load"),
    )
    .withColumn(
        "completion_rate_pct",
        F.round((F.col("completed_sessions") / F.col("total_sessions")) * 100, 2)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 — Muscle group breakdown (explode -> count -> re-collect)
# MAGIC Same explode + groupBy + collect_list(struct) pattern as Local --
# MAGIC unnest the array so each (user, muscle_group) pair is its own row
# MAGIC for counting, then fold back into one array-of-structs column per
# MAGIC user so the final table stays at user grain.

# COMMAND ----------

muscle_breakdown = (
    df.withColumn("muscle_group", F.explode("muscle_groups_array"))
    .groupBy("user_id", "muscle_group")
    .agg(F.count("*").alias("session_count"))
)

muscle_summary = (
    muscle_breakdown
    .groupBy("user_id")
    .agg(
        F.collect_list(
            F.struct(F.col("muscle_group"), F.col("session_count"))
        ).alias("muscle_group_breakdown")
    )
)

gold = base.join(muscle_summary, on="user_id", how="left")

display(gold.orderBy("user_id"))

# COMMAND ----------

# MAGIC %md ### Step 4 — Write Gold (overwrite-on-recompute)

# COMMAND ----------

(
    gold.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_table)
)

print(f"{gold_table} written: {spark.table(gold_table).count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verification checkpoint
# MAGIC Gold row count should equal distinct user_id count in Silver
# MAGIC workout_log -- one row per user who has at least one workout logged.

# COMMAND ----------

expected_users = workout_df.select("user_id").distinct().count()
actual_rows = spark.table(gold_table).count()

print(f"Distinct users in Silver workout_log: {expected_users}")
print(f"Rows in gold.workout_consistency     : {actual_rows}")
assert expected_users == actual_rows, "MISMATCH - investigate before proceeding"
print("Verified: row count matches distinct users.")

# Sanity check the muscle_groups parsing actually worked end-to-end
display(spark.table(gold_table).select("user_id", "total_sessions", "completion_rate_pct", "muscle_group_breakdown"))
