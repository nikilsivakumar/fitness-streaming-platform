# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # DB-4 — Silver -> Gold: `gold_fatigue_recovery`
# MAGIC
# MAGIC ## Layman recap
# MAGIC For every (user, day): blend recovery score, stress, and sleep debt
# MAGIC into one fatigue number, plus an early-warning overtraining flag.
# MAGIC Identical formula to Local/AWS -- see decisions.md / silver_to_gold.py
# MAGIC docstring (Local) for the full physiological sourcing of every weight
# MAGIC and threshold below (Foster 2001, Van Dongen 2003, Meeusen/ECSS 2013).
# MAGIC
# MAGIC ## Two genuine Databricks-specific adaptations (not bugs)
# MAGIC 1. Silver here has no precomputed `date` column (Local partitions
# MAGIC    Parquet by date; Delta tables don't need that) -- derived from
# MAGIC    `timestamp` at read time instead, same to_date() logic, just later
# MAGIC    in the pipeline.
# MAGIC 2. `stress_score` passed through Bronze->Silver untyped (DB-3's
# MAGIC    wearable_event notebook only explicitly casts heart_rate/hrv/
# MAGIC    steps/recovery_score) -- cast explicitly here for safety.
# MAGIC
# MAGIC ## Overwrite-on-recompute (the DB-4 architecture decision)
# MAGIC No stable merge key for a daily aggregate -- full recompute from
# MAGIC Silver every run, same pattern as Local (Parquet overwrite) and AWS
# MAGIC (Glue full-partition overwrite).

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "fitness_streaming"
gold_table = f"{CATALOG}.gold.fatigue_recovery"

SLEEP_BASELINE_HOURS      = 8.0   # Van Dongen et al. 2003
FATIGUE_SLEEP_WEIGHT      = 40.0
FATIGUE_RECOVERY_WEIGHT   = 35.0
FATIGUE_STRESS_WEIGHT     = 25.0
OVERTRAINING_FATIGUE_MIN  = 70.0
OVERTRAINING_RECOVERY_MAX = 40.0

# COMMAND ----------

# MAGIC %md ### Step 1 — Read Silver, derive `date`, cast `stress_score`

# COMMAND ----------

wearable_df = (
    spark.table(f"{CATALOG}.silver.wearable_event")
    .withColumn("date", F.to_date("timestamp"))
    .withColumn("stress_score", F.col("stress_score").cast("double"))
)
sleep_df = (
    spark.table(f"{CATALOG}.silver.sleep_log")
    .withColumn("date", F.to_date("timestamp"))
)
workout_df = (
    spark.table(f"{CATALOG}.silver.workout_log")
    .withColumn("date", F.to_date("timestamp"))
)

print(f"wearable_event Silver rows: {wearable_df.count()}")
print(f"sleep_log Silver rows     : {sleep_df.count()}")
print(f"workout_log Silver rows   : {workout_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 — Daily aggregates per source
# MAGIC Identical grouping/aggregation logic to Local's build_gold_fatigue_recovery.

# COMMAND ----------

wearable_daily = (
    wearable_df
    .groupBy("user_id", "date")
    .agg(
        F.avg("recovery_score").alias("recovery_score"),
        F.avg("stress_score").alias("stress_score"),
        F.avg("hrv").alias("hrv"),
    )
)

sleep_daily = (
    sleep_df
    .groupBy("user_id", "date")
    .agg(F.avg("total_sleep_hours").alias("total_sleep_hours"))
)

workout_daily = (
    workout_df
    .withColumn("session_load", F.col("duration_minutes") * F.col("rpe"))
    .groupBy("user_id", "date")
    .agg(F.sum("session_load").alias("training_load_today"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 — Join (wearable_event anchor, LEFT join the rest)
# MAGIC Same reasoning as Local: everyone wearing a device generates daily
# MAGIC readings; not everyone logs a workout or sleep entry every day, and
# MAGIC that absence is meaningful, not an error -- coalesced, not dropped.

# COMMAND ----------

gold = (
    wearable_daily
    .join(sleep_daily, on=["user_id", "date"], how="left")
    .join(workout_daily, on=["user_id", "date"], how="left")
    .fillna({"total_sleep_hours": SLEEP_BASELINE_HOURS, "training_load_today": 0.0})
)

# COMMAND ----------

# MAGIC %md ### Step 4 — Scoring formulas (unchanged from Local/AWS)

# COMMAND ----------

gold = gold.withColumn(
    "sleep_debt_hours",
    F.greatest(F.lit(0.0), F.lit(SLEEP_BASELINE_HOURS) - F.col("total_sleep_hours"))
)

gold = gold.withColumn(
    "fatigue_score",
    F.round(
        (F.col("sleep_debt_hours") / F.lit(SLEEP_BASELINE_HOURS)) * F.lit(FATIGUE_SLEEP_WEIGHT)
        + ((F.lit(100.0) - F.col("recovery_score")) / F.lit(100.0)) * F.lit(FATIGUE_RECOVERY_WEIGHT)
        + (F.col("stress_score") / F.lit(100.0)) * F.lit(FATIGUE_STRESS_WEIGHT),
        2
    )
)
gold = gold.withColumn(
    "fatigue_score",
    F.when(F.col("fatigue_score") > 100, F.lit(100.0))
     .when(F.col("fatigue_score") < 0, F.lit(0.0))
     .otherwise(F.col("fatigue_score"))
)

gold = gold.withColumn("recovery_readiness", F.round(F.lit(100.0) - F.col("fatigue_score"), 2))

gold = gold.withColumn(
    "overtraining_flag",
    (F.col("fatigue_score") > F.lit(OVERTRAINING_FATIGUE_MIN))
    & (F.col("training_load_today") > F.lit(0.0))
    & (F.col("recovery_score") < F.lit(OVERTRAINING_RECOVERY_MAX))
)

gold = gold.select(
    "user_id", "date",
    "recovery_score", "stress_score", "hrv",
    "total_sleep_hours", "sleep_debt_hours",
    "training_load_today",
    "fatigue_score", "recovery_readiness", "overtraining_flag",
)

display(gold.orderBy("user_id", "date"))

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
# MAGIC Gold row count should equal the number of distinct (user_id, date)
# MAGIC pairs in wearable_event Silver (the anchor source) -- the left joins
# MAGIC should never multiply or drop rows relative to that anchor.

# COMMAND ----------

expected_rows = wearable_df.select("user_id", "date").distinct().count()
actual_rows = spark.table(gold_table).count()

print(f"Expected (distinct user_id+date in wearable_event): {expected_rows}")
print(f"Actual rows in gold.fatigue_recovery               : {actual_rows}")
assert expected_rows == actual_rows, "MISMATCH - investigate before proceeding"
print("Verified: row count matches anchor source.")

display(spark.table(gold_table).filter("overtraining_flag = true"))

# COMMAND ----------

df = spark.table("fitness_streaming.silver.workout_log")
df.printSchema()
df.select("user_id", "muscle_groups").show(5, truncate=False)
