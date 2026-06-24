# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # DB-4 — Silver -> Gold: `gold_user_profile_enriched`
# MAGIC
# MAGIC ## Layman recap
# MAGIC Silver `user_profile` already has exactly one row per user (kept that
# MAGIC way by DB-3's MERGE INTO), and now also carries `medical_risk_modifier`
# MAGIC and `gym_type` after the reference-enrichment fix. This notebook's job
# MAGIC is small on purpose: select the columns Gold needs, and write them out.
# MAGIC
# MAGIC ## Why this is genuinely simpler than Local/AWS's version
# MAGIC Local and AWS both had to do their own row_number()-over-timestamp
# MAGIC dedup INSIDE the Gold build, because their Bronze->Silver step never
# MAGIC enforced "one row per user" as a Silver-layer guarantee -- Gold had to
# MAGIC defend against duplicates it had no control over (this is exactly the
# MAGIC bug that surfaced on AWS Phase 5, caught by inspecting real Gold output).
# MAGIC On Databricks, DB-3's MERGE INTO makes "one row per user_id" a Silver-
# MAGIC layer GUARANTEE, not a Gold-layer assumption. Gold here doesn't dedup
# MAGIC at all -- it just enriches/projects an already-correct base. This is a
# MAGIC real architectural improvement worth calling out explicitly in
# MAGIC decisions.md: pushing the correctness guarantee as far upstream as
# MAGIC possible (Silver) means every downstream consumer (Gold, dashboards,
# MAGIC future dbt models) inherits it for free, instead of every consumer
# MAGIC re-implementing the same defensive dedup.
# MAGIC
# MAGIC ## Why overwrite-on-recompute, not MERGE INTO, at Gold
# MAGIC Per the DB-4 architecture decision: Gold aggregates/dimensions have no
# MAGIC stable "what changed" key the way Silver's CDC rows do -- the correct
# MAGIC current state is just "recompute fully from Silver, every run." Same
# MAGIC pattern Local (Parquet overwrite) and AWS (Glue full-partition
# MAGIC overwrite) already use -- keeps all three stacks comparable at this
# MAGIC layer, which is the whole point of building this 3 times.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "fitness_streaming"
silver_table = f"{CATALOG}.silver.user_profile"
gold_table = f"{CATALOG}.gold.user_profile_enriched"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 — Read Silver, select Gold's columns
# MAGIC Identical column list to Local's build_gold_user_profile_enriched():
# MAGIC user_id, age, gender, weight_kg, medical_history,
# MAGIC medical_risk_modifier, job_type, fitness_goal, gym_id, gym_type.
# MAGIC No dedup step here -- see module docstring for why.

# COMMAND ----------

silver_df = spark.table(silver_table)
print(f"Silver rows read: {silver_df.count()}")
print(f"Silver columns: {silver_df.columns}")

gold_df = silver_df.select(
    "user_id", "age", "gender", "weight_kg",
    "medical_history", "medical_risk_modifier",
    "job_type", "fitness_goal", "gym_id", "gym_type",
)

display(gold_df.orderBy("user_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 — Write Gold (overwrite-on-recompute)
# MAGIC saveAsTable with mode("overwrite") fully replaces the table's contents
# MAGIC every run -- correct here because there's no merge key for a
# MAGIC dimension snapshot like this, only "the current, complete picture."

# COMMAND ----------

(
    gold_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_table)
)

print(f"{gold_table} written: {spark.table(gold_table).count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verification checkpoint
# MAGIC Gold row count MUST equal distinct user_id count in Silver -- if it
# MAGIC doesn't, something silently duplicated or dropped rows during the
# MAGIC select, which shouldn't be possible here but is exactly the kind of
# MAGIC thing this project checks rather than assumes.

# COMMAND ----------

silver_distinct_users = silver_df.select("user_id").distinct().count()
gold_row_count = spark.table(gold_table).count()

print(f"Distinct users in Silver : {silver_distinct_users}")
print(f"Rows in Gold              : {gold_row_count}")
assert silver_distinct_users == gold_row_count, "MISMATCH - investigate before proceeding"
print("Verified: Gold row count matches distinct Silver users.")

display(spark.table(gold_table).orderBy("user_id"))
