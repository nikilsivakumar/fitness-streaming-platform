# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # DB-4 — Silver -> Gold: `gold_community_analytics` (last, highest risk)
# MAGIC
# MAGIC ## Layman recap
# MAGIC Grain: one row per (job_type, fitness_goal) cohort. Does occupation
# MAGIC or stated fitness goal predict recovery, sleep, or training
# MAGIC consistency? Identical question and identical aggregation logic to
# MAGIC Local's build_gold_community_analytics.
# MAGIC
# MAGIC ## The cohort-lookup fix (3rd iteration, fixed at the root)
# MAGIC Local enriches job_type/fitness_goal onto wearable_event/sleep_log
# MAGIC natively in Phase 3 (CSV join), and only needed a lookup-trick for
# MAGIC workout_log. Databricks Silver never did that enrichment for ANY of
# MAGIC the three -- so this notebook derives ONE user_id -> (job_type,
# MAGIC fitness_goal) lookup from silver.user_profile (verified clean, one
# MAGIC row/user via DB-3's MERGE INTO) and applies it uniformly to all three
# MAGIC sources, rather than re-opening three Bronze->Silver notebooks to
# MAGIC port the same CSV join three separate times.
# MAGIC
# MAGIC ## Overwrite-on-recompute
# MAGIC Cohort-level aggregate, no stable merge key -- full recompute every
# MAGIC run, same as the other 3 Gold tables.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "fitness_streaming"
gold_table = f"{CATALOG}.gold.community_analytics"

# COMMAND ----------

# MAGIC %md ### Step 1 — Read Silver sources + the cohort lookup

# COMMAND ----------

wearable_df = spark.table(f"{CATALOG}.silver.wearable_event")
sleep_df = spark.table(f"{CATALOG}.silver.sleep_log")
workout_df = spark.table(f"{CATALOG}.silver.workout_log")

user_cohort_lookup = (
    spark.table(f"{CATALOG}.silver.user_profile")
    .select("user_id", "job_type", "fitness_goal")
)

print(f"wearable_event Silver rows : {wearable_df.count()}")
print(f"sleep_log Silver rows      : {sleep_df.count()}")
print(f"workout_log Silver rows    : {workout_df.count()}")
print(f"user_profile lookup rows   : {user_cohort_lookup.count()}  (should be 15, one per user)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 — Apply the cohort lookup uniformly to all three sources

# COMMAND ----------

wearable_with_cohort = wearable_df.join(user_cohort_lookup, on="user_id", how="left")
sleep_with_cohort = sleep_df.join(user_cohort_lookup, on="user_id", how="left")
workout_with_cohort = workout_df.join(user_cohort_lookup, on="user_id", how="left")

# Sanity check: cohort join should not introduce nulls if every user_id
# in these tables also exists in user_profile -- if this prints > 0,
# investigate before trusting the cohort aggregates below.
unmatched = wearable_with_cohort.filter(F.col("job_type").isNull()).select("user_id").distinct().count()
print(f"wearable_event users with NO cohort match: {unmatched}")

# COMMAND ----------

# MAGIC %md ### Step 3 — Per-source cohort aggregates (identical logic to Local)

# COMMAND ----------

wearable_cohort = (
    wearable_with_cohort
    .groupBy("job_type", "fitness_goal")
    .agg(
        F.round(F.avg("recovery_score"), 2).alias("avg_recovery_score"),
        F.round(F.avg("stress_score"), 2).alias("avg_stress_score"),
        F.round(F.avg("hrv"), 2).alias("avg_hrv"),
        F.countDistinct("user_id").alias("user_count_wearable"),
    )
)

sleep_cohort = (
    sleep_with_cohort
    .groupBy("job_type", "fitness_goal")
    .agg(F.round(F.avg("total_sleep_hours"), 2).alias("avg_sleep_hours"))
)

workout_cohort = (
    workout_with_cohort
    .withColumn("session_load", F.col("duration_minutes") * F.col("rpe"))
    .groupBy("job_type", "fitness_goal")
    .agg(
        F.round(F.avg("session_load"), 2).alias("avg_training_load"),
        F.round(F.avg(F.col("completed").cast("int")) * 100, 2).alias("avg_completion_rate_pct"),
    )
)

# COMMAND ----------

# MAGIC %md ### Step 4 — Join the three cohort aggregates together

# COMMAND ----------

gold = (
    wearable_cohort
    .join(sleep_cohort, on=["job_type", "fitness_goal"], how="left")
    .join(workout_cohort, on=["job_type", "fitness_goal"], how="left")
)

display(gold.orderBy("job_type", "fitness_goal"))

# COMMAND ----------

# MAGIC %md ### Step 5 — Write Gold (overwrite-on-recompute)

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
# MAGIC Gold row count should equal the number of distinct (job_type,
# MAGIC fitness_goal) combinations actually present in wearable_event after
# MAGIC the cohort join -- the anchor source, same reasoning as fatigue_recovery.

# COMMAND ----------

expected_cohorts = (
    wearable_with_cohort.select("job_type", "fitness_goal").distinct().count()
)
actual_rows = spark.table(gold_table).count()

print(f"Expected distinct cohorts (from wearable_event): {expected_cohorts}")
print(f"Actual rows in gold.community_analytics         : {actual_rows}")
assert expected_cohorts == actual_rows, "MISMATCH - investigate before proceeding"
print("Verified: row count matches expected cohort count.")

display(spark.table(gold_table))
