"""
glue_silver_to_gold.py
==========================
AWS Phase 5b — Silver to Gold ETL (Glue version)
Fitness Streaming Analytics Platform — AWS Version

Direct AWS Glue port of version_local/etl/silver_to_gold.py. All scoring
formulas, weights, and physiological sources are UNCHANGED from the
verified local version (Foster Session-RPE training load, Van Dongen
sleep debt baseline, Meeusen/ECSS overtraining proxy, from_json muscle
group parsing) - see the local script's docstring for the full
methodology writeup, which still applies identically here. Only the
infrastructure (GlueContext instead of local SparkSession, s3:// paths
instead of local disk, job arguments instead of hardcoded constants)
has changed, consistent with the project's "same logic, different
plumbing" comparative design.

THE COHORT-LOOKUP FIX FROM LOCAL PHASE 4 IS ALSO ALREADY APPLIED
---------------------------------------------------------------------
workout_log Silver does not carry job_type/fitness_goal natively (only
wearable_event/sleep_log/nutrition_snapshot get that enrichment join in
Phase 3/5b). gold_community_analytics derives a user_id -> cohort lookup
from wearable_event before joining workout_log against it, exactly as
fixed and verified in the local version.
"""

import sys
from datetime import datetime

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import ArrayType, StringType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])
BUCKET = args["BUCKET"]

SILVER_DIR = f"s3://{BUCKET}/silver"
GOLD_DIR   = f"s3://{BUCKET}/gold"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.sparkContext.setLogLevel("WARN")

SLEEP_BASELINE_HOURS      = 8.0
FATIGUE_SLEEP_WEIGHT      = 40.0
FATIGUE_RECOVERY_WEIGHT   = 35.0
FATIGUE_STRESS_WEIGHT     = 25.0
OVERTRAINING_FATIGUE_MIN  = 70.0
OVERTRAINING_RECOVERY_MAX = 40.0


def read_silver(record_type):
    path = f"{SILVER_DIR}/record_type={record_type}"
    try:
        df = spark.read.parquet(path)
    except Exception as e:
        print(f"WARNING: No Silver data found for {record_type} at {path} - skipping ({e})")
        return None

    count = df.count()
    print(f"[{record_type}] Silver rows read: {count}")
    if count == 0:
        return None
    return df


def parse_muscle_groups(df):
    json_safe = F.regexp_replace(F.col("muscle_groups"), "'", '"')
    df = df.withColumn("muscle_groups_array", F.from_json(json_safe, ArrayType(StringType())))
    return df


def build_gold_fatigue_recovery(df_wearable, df_sleep, df_workout):
    wearable_daily = (
        df_wearable.groupBy("user_id", "date").agg(
            F.avg("recovery_score").alias("recovery_score"),
            F.avg("stress_score").alias("stress_score"),
            F.avg("hrv").alias("hrv"),
        )
    )
    sleep_daily = (
        df_sleep.groupBy("user_id", "date").agg(F.avg("total_sleep_hours").alias("total_sleep_hours"))
    )
    workout_daily = (
        df_workout
        .withColumn("session_load", F.col("duration_minutes") * F.col("rpe"))
        .groupBy("user_id", "date")
        .agg(F.sum("session_load").alias("training_load_today"))
    )

    gold = (
        wearable_daily
        .join(sleep_daily, on=["user_id", "date"], how="left")
        .join(workout_daily, on=["user_id", "date"], how="left")
        .fillna({"total_sleep_hours": SLEEP_BASELINE_HOURS, "training_load_today": 0.0})
    )

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

    return gold.select(
        "user_id", "date",
        "recovery_score", "stress_score", "hrv",
        "total_sleep_hours", "sleep_debt_hours",
        "training_load_today",
        "fatigue_score", "recovery_readiness", "overtraining_flag",
    )


def build_gold_workout_consistency(df_workout):
    df = parse_muscle_groups(df_workout)
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
        .withColumn("completion_rate_pct", F.round((F.col("completed_sessions") / F.col("total_sessions")) * 100, 2))
    )

    muscle_breakdown = (
        df.withColumn("muscle_group", F.explode("muscle_groups_array"))
        .groupBy("user_id", "muscle_group")
        .agg(F.count("*").alias("session_count"))
    )

    muscle_summary = (
        muscle_breakdown.groupBy("user_id")
        .agg(F.collect_list(F.struct(F.col("muscle_group"), F.col("session_count"))).alias("muscle_group_breakdown"))
    )

    return base.join(muscle_summary, on="user_id", how="left")


def build_gold_community_analytics(df_wearable, df_sleep, df_workout):
    """job_type/fitness_goal cohort lookup derived from wearable_event,
    since workout_log Silver does not carry them natively (see module
    docstring and local Phase 4 bugfix commit for full explanation)."""
    wearable_cohort = (
        df_wearable.groupBy("job_type", "fitness_goal").agg(
            F.round(F.avg("recovery_score"), 2).alias("avg_recovery_score"),
            F.round(F.avg("stress_score"), 2).alias("avg_stress_score"),
            F.round(F.avg("hrv"), 2).alias("avg_hrv"),
            F.countDistinct("user_id").alias("user_count_wearable"),
        )
    )

    sleep_cohort = (
        df_sleep.groupBy("job_type", "fitness_goal").agg(F.round(F.avg("total_sleep_hours"), 2).alias("avg_sleep_hours"))
    )

    user_cohort_lookup = df_wearable.select("user_id", "job_type", "fitness_goal").dropDuplicates(["user_id"])
    workout_with_cohort = df_workout.join(user_cohort_lookup, on="user_id", how="left")

    workout_cohort = (
        workout_with_cohort
        .withColumn("session_load", F.col("duration_minutes") * F.col("rpe"))
        .groupBy("job_type", "fitness_goal")
        .agg(
            F.round(F.avg("session_load"), 2).alias("avg_training_load"),
            F.round(F.avg(F.col("completed").cast("int")) * 100, 2).alias("avg_completion_rate_pct"),
        )
    )

    return (
        wearable_cohort
        .join(sleep_cohort, on=["job_type", "fitness_goal"], how="left")
        .join(workout_cohort, on=["job_type", "fitness_goal"], how="left")
    )


def build_gold_user_profile_enriched(df_user_profile):
    """
    Grain: one row per user_id - the CURRENT (most recent) profile state.

    user_profile arrives as low-frequency CDC-style events (same user_id
    can legitimately repeat with different attribute values, simulating
    profile changes over time). BUG FOUND AND FIXED HERE (ported fix,
    same as version_local/etl/silver_to_gold.py): original version did a
    flat .select() with no dedup. Caught by inspecting real Gold output
    on the AWS run, where one user_id showed conflicting gym_id /
    medical_history values across rows after the producer fix that
    enabled user_profile to flow through Bronze at all.

    Type 1 (current-state-only) dimension: keep latest row per user_id.
    Full history remains in Silver for the future SCD Type 2 layer.
    """
    w = Window.partitionBy("user_id").orderBy(F.col("timestamp").desc())
    df_latest = (
        df_user_profile
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    return df_latest.select(
        "user_id", "age", "gender", "weight_kg",
        "medical_history", "medical_risk_modifier",
        "job_type", "fitness_goal", "gym_id", "gym_type",
    )


def write_gold(df, table_name):
    out_path = f"{GOLD_DIR}/{table_name}"
    (df.repartition(1).write.mode("overwrite").parquet(out_path))
    row_count = df.count()
    print(f"[{table_name}] Gold written -> {out_path} ({row_count} rows)")
    return row_count


def main():
    start = datetime.now()
    print("=" * 60)
    print("AWS PHASE 5b - GLUE SILVER TO GOLD ETL")
    print(f"Start time: {start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Bucket: {BUCKET}")
    print("=" * 60)

    df_wearable      = read_silver("wearable_event")
    df_workout       = read_silver("workout_log")
    df_sleep         = read_silver("sleep_log")
    df_nutrition     = read_silver("nutrition_snapshot")  # read for completeness, not yet used in Gold v1
    df_user_profile  = read_silver("user_profile")

    results = {}

    if df_wearable is not None and df_sleep is not None and df_workout is not None:
        results["gold_fatigue_recovery"] = write_gold(
            build_gold_fatigue_recovery(df_wearable, df_sleep, df_workout), "gold_fatigue_recovery"
        )
    else:
        print("Skipping gold_fatigue_recovery - missing one or more required Silver sources")

    if df_workout is not None:
        results["gold_workout_consistency"] = write_gold(
            build_gold_workout_consistency(df_workout), "gold_workout_consistency"
        )
    else:
        print("Skipping gold_workout_consistency - missing workout_log Silver")

    if df_wearable is not None and df_sleep is not None and df_workout is not None:
        results["gold_community_analytics"] = write_gold(
            build_gold_community_analytics(df_wearable, df_sleep, df_workout), "gold_community_analytics"
        )
    else:
        print("Skipping gold_community_analytics - missing one or more required Silver sources")

    if df_user_profile is not None:
        results["gold_user_profile_enriched"] = write_gold(
            build_gold_user_profile_enriched(df_user_profile), "gold_user_profile_enriched"
        )
    else:
        print("Skipping gold_user_profile_enriched - missing user_profile Silver")

    elapsed = (datetime.now() - start).total_seconds()

    print("=" * 60)
    print("PHASE 5b GOLD SUMMARY")
    for table, count in results.items():
        print(f"  {table}: {count} rows")
    print(f"Elapsed: {elapsed:.1f}s")
    print("=" * 60)


main()
job.commit()
