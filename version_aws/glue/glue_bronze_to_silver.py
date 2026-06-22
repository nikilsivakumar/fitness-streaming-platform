"""
glue_bronze_to_silver.py
===========================
AWS Phase 5b — Bronze to Silver ETL (Glue version)
Fitness Streaming Analytics Platform — AWS Version

WHAT THIS REPLACES (local -> AWS mapping)
------------------------------------------
Direct AWS Glue port of version_local/etl/bronze_to_silver.py and
version_local/etl/validation_rules.py, COMBINED into a single script
because Glue jobs are deployed as one script per job (no local package
imports across separate uploaded files without extra --extra-py-files
configuration, which adds deployment complexity not worth it for this
project's scope). The validation logic from validation_rules.py is
inlined below under the VALIDATORS dispatch dict, unchanged in
substance from the local version.

THE weight_kg FIX IS ALREADY APPLIED HERE
---------------------------------------------
The local validation_rules.py originally had a bug: validate_user_profile
checked F.col("weight") instead of F.col("weight_kg"), which does not
exist in the schema, causing every user_profile record to be incorrectly
quarantined. That bug was found, fixed, verified, and committed earlier
in this project (see git history). This Glue port uses the CORRECTED
weight_kg reference from the start - there is no reason to reintroduce
a bug that was already found and fixed, just because the transport
infrastructure changed.

WHAT ACTUALLY CHANGED FROM LOCAL TO GLUE (the real point of comparison)
----------------------------------------------------------------------------
1. SparkSession creation: GlueContext wraps the SparkContext that AWS
   provides to the job at runtime, rather than building one with
   .master("local[*]"). Glue manages cluster provisioning; you never
   specify "local[*]" because there is no "local" - it's always a
   managed Spark cluster (even at the smallest worker configuration).
2. Paths: every local filesystem path (os.path.join(...)) becomes an
   s3:// URI. Spark's S3 connector handles this natively - no code
   changes needed to the actual read/write calls, only the path strings.
3. Job arguments: Glue jobs receive parameters via getResolvedOptions
   instead of hardcoded constants, so the same script can be pointed at
   different buckets/prefixes without editing code - this is what
   makes a Glue job reusable across environments (dev/staging/prod)
   without a redeploy, a real production pattern.
4. Job bookmarking (not used here, but worth naming in interviews):
   Glue can track which S3 files it has already processed across runs,
   so re-running the job doesn't reprocess old data. This project's
   actual pipeline doesn't enable bookmarking because Bronze is meant to
   be fully reprocessed on each Silver rebuild (mirroring the local
   version's overwrite-mode write behavior) - bookmarking would actually
   work AGAINST the "Bronze is immutable, reprocess freely" design
   decision documented in the local version.
"""

import sys
from datetime import datetime

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import Window

# -------------------------------------------------
# JOB ARGUMENTS
# -------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])
BUCKET = args["BUCKET"]

BRONZE_DIR     = f"s3://{BUCKET}/bronze"
SILVER_DIR     = f"s3://{BUCKET}/silver"
QUARANTINE_DIR = f"s3://{BUCKET}/quarantine"
REFERENCE_DIR  = f"s3://{BUCKET}/reference"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.sparkContext.setLogLevel("WARN")

RECORD_TYPES = [
    "wearable_event",
    "workout_log",
    "sleep_log",
    "nutrition_snapshot",
    "user_profile",
]

SENTINEL_DEFAULTS = {
    "wearable_event": {
        "heart_rate": -1, "hrv": -1.0, "steps": -1, "recovery_score": -1.0
    },
    "workout_log": {
        "duration_minutes": -1, "rpe": -1
    },
    "sleep_log": {
        "total_sleep_hours": -1.0, "deep_sleep_hours": -1.0, "rem_hours": -1.0
    },
    "nutrition_snapshot": {
        "calories_in": -1, "protein_g": -1.0, "carbs_g": -1.0, "hydration_ml": -1.0
    },
    "user_profile": {
        "age": -1, "weight_kg": -1.0
    },
}

EXPECTED_SCHEMA = {
    "wearable_event": {
        "user_id": "string", "timestamp": "timestamp",
        "heart_rate": "int", "hrv": "double",
        "steps": "int", "recovery_score": "double",
    },
    "workout_log": {
        "user_id": "string", "timestamp": "timestamp",
        "workout_type": "string", "duration_minutes": "int",
        "rpe": "int", "muscle_groups": "string",
    },
    "sleep_log": {
        "user_id": "string", "timestamp": "timestamp",
        "total_sleep_hours": "double", "deep_sleep_hours": "double",
        "rem_hours": "double",
    },
    "nutrition_snapshot": {
        "user_id": "string", "timestamp": "timestamp",
        "calories_in": "int", "protein_g": "double",
        "carbs_g": "double", "hydration_ml": "double",
    },
    "user_profile": {
        "user_id": "string", "timestamp": "timestamp",
        "age": "int", "weight_kg": "double",
        "medical_history": "string", "job_type": "string",
        "fitness_goal": "string",
    },
}


# -------------------------------------------------
# VALIDATION RULES - inlined from validation_rules.py, weight_kg fix applied
# -------------------------------------------------

def _base_columns(df):
    return df.withColumn("is_valid", F.lit(True)).withColumn("failure_reason", F.lit(None).cast("string"))


def _apply_rule(df, condition_fails, reason_code):
    return df.withColumn(
        "is_valid",
        F.when(condition_fails, F.lit(False)).otherwise(F.col("is_valid"))
    ).withColumn(
        "failure_reason",
        F.when(condition_fails & F.col("failure_reason").isNull(), F.lit(reason_code))
         .otherwise(F.col("failure_reason"))
    )


def validate_wearable_event(df):
    df = _base_columns(df)
    df = _apply_rule(df, ~F.col("heart_rate").between(30, 220) | F.col("heart_rate").isNull(), "heart_rate_out_of_range")
    df = _apply_rule(df, (F.col("hrv") <= 0) | F.col("hrv").isNull(), "hrv_negative_or_zero")
    df = _apply_rule(df, ~F.col("steps").between(0, 50000) | F.col("steps").isNull(), "steps_out_of_range")
    df = _apply_rule(df, F.col("recovery_score").isNotNull() & ~F.col("recovery_score").between(0, 100), "recovery_score_out_of_range")
    return df


def validate_workout_log(df):
    df = _base_columns(df)
    df = _apply_rule(df, ~F.col("duration_minutes").between(1, 300) | F.col("duration_minutes").isNull(), "duration_out_of_range")
    df = _apply_rule(df, ~F.col("rpe").between(1, 10) | F.col("rpe").isNull(), "rpe_out_of_range")
    return df


def validate_sleep_log(df):
    df = _base_columns(df)
    df = _apply_rule(df, ~F.col("total_sleep_hours").between(0, 16) | F.col("total_sleep_hours").isNull(), "total_sleep_out_of_range")
    df = _apply_rule(df, F.col("deep_sleep_hours") > F.col("total_sleep_hours"), "deep_sleep_exceeds_total")
    df = _apply_rule(df, F.col("rem_hours") > F.col("total_sleep_hours"), "rem_exceeds_total")
    return df


def validate_nutrition_snapshot(df):
    df = _base_columns(df)
    df = _apply_rule(df, ~F.col("calories_in").between(0, 10000) | F.col("calories_in").isNull(), "calories_out_of_range")
    df = _apply_rule(df, F.col("protein_g").isNotNull() & ~F.col("protein_g").between(0, 500), "protein_out_of_range")
    df = _apply_rule(df, F.col("hydration_ml").isNotNull() & ~F.col("hydration_ml").between(0, 10000), "hydration_out_of_range")
    return df


def validate_user_profile(df):
    """weight_kg fix applied here from the start - see module docstring."""
    df = _base_columns(df)
    df = _apply_rule(df, ~F.col("age").between(13, 100) | F.col("age").isNull(), "age_out_of_range")
    df = _apply_rule(
        df,
        F.col("weight_kg").isNotNull() & ~F.col("weight_kg").between(30, 250),
        "weight_out_of_range"
    )
    return df


VALIDATORS = {
    "wearable_event":      validate_wearable_event,
    "workout_log":         validate_workout_log,
    "sleep_log":           validate_sleep_log,
    "nutrition_snapshot":  validate_nutrition_snapshot,
    "user_profile":        validate_user_profile,
}


# -------------------------------------------------
# PIPELINE STEPS - same logic as local bronze_to_silver.py
# -------------------------------------------------

def read_bronze(record_type):
    path = f"{BRONZE_DIR}/record_type={record_type}"
    try:
        df = spark.read.parquet(path)
    except Exception as e:
        print(f"WARNING: No Bronze data found for {record_type} at {path} - skipping ({e})")
        return None

    count = df.count()
    print(f"[{record_type}] Bronze rows read: {count}")
    if count == 0:
        return None
    return df


def deduplicate(df, record_type):
    if "user_id" not in df.columns or "timestamp" not in df.columns:
        print(f"[{record_type}] Missing user_id or timestamp - skipping dedup")
        return df

    before = df.count()
    w = Window.partitionBy("user_id", "timestamp").orderBy(F.lit(1))
    df_deduped = (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    after = df_deduped.count()
    if before != after:
        print(f"[{record_type}] Deduplication removed {before - after} duplicate rows")
    return df_deduped


def cast_and_fill(df, record_type):
    schema = EXPECTED_SCHEMA.get(record_type, {})
    for col_name, col_type in schema.items():
        if col_name not in df.columns:
            print(f"[{record_type}] Expected column '{col_name}' missing from Bronze - creating as null")
            df = df.withColumn(col_name, F.lit(None).cast(col_type))
        else:
            df = df.withColumn(col_name, F.col(col_name).cast(col_type))

    sentinels = SENTINEL_DEFAULTS.get(record_type, {})
    if sentinels:
        df = df.fillna(sentinels)
    return df


def validate(df, record_type):
    validator_fn = VALIDATORS.get(record_type)
    if validator_fn is None:
        print(f"[{record_type}] No validator defined - marking all rows valid")
        return df.withColumn("is_valid", F.lit(True)).withColumn("failure_reason", F.lit(None).cast("string"))
    return validator_fn(df)


def split_silver_quarantine(df, record_type):
    df_silver = df.filter(F.col("is_valid") == True).drop("is_valid")
    df_quarantine = df.filter(F.col("is_valid") == False).drop("is_valid")

    silver_count = df_silver.count()
    quarantine_count = df_quarantine.count()
    total = silver_count + quarantine_count
    pct_quarantined = (quarantine_count / total * 100) if total > 0 else 0.0

    print(f"[{record_type}] Silver: {silver_count} | Quarantine: {quarantine_count} ({pct_quarantined:.1f}%)")
    return df_silver, df_quarantine


def load_reference_tables():
    refs = {}
    for name in ["user_profiles", "gym_master", "workout_catalog", "medical_conditions"]:
        path = f"{REFERENCE_DIR}/{name}.csv"
        try:
            df = spark.read.option("header", True).option("inferSchema", True).csv(path)
            refs[name] = df
            print(f"Loaded reference: {name}.csv ({df.count()} rows)")
        except Exception as e:
            print(f"WARNING: could not load {name}.csv - {e}")
            refs[name] = None
    return refs


def enrich(df_silver, record_type, refs):
    user_profiles = refs.get("user_profiles")
    gym_master = refs.get("gym_master")
    workout_catalog = refs.get("workout_catalog")
    medical_conditions = refs.get("medical_conditions")

    if record_type == "user_profile":
        if medical_conditions is not None and "medical_history" in df_silver.columns:
            df_silver = df_silver.join(
                medical_conditions.select(
                    F.col("condition").alias("medical_history"),
                    F.col("risk_modifier").alias("medical_risk_modifier")
                ),
                on="medical_history",
                how="left"
            )
            df_silver = df_silver.fillna({"medical_risk_modifier": 1.0})
        else:
            df_silver = df_silver.withColumn("medical_risk_modifier", F.lit(1.0))

        if gym_master is not None and "gym_id" in df_silver.columns:
            df_silver = df_silver.join(
                gym_master.select(F.col("gym_id"), F.col("gym_type")),
                on="gym_id",
                how="left"
            )
        return df_silver

    if record_type == "workout_log":
        if workout_catalog is not None and "workout_type" in df_silver.columns:
            df_silver = df_silver.join(
                workout_catalog.select("workout_type", "met_value", "recovery_days_needed"),
                on="workout_type",
                how="left"
            )
        return df_silver

    if user_profiles is not None and "user_id" in df_silver.columns:
        select_cols = ["user_id"]
        for col_candidate in ["job_type", "fitness_goal", "gym_id"]:
            if col_candidate in user_profiles.columns:
                select_cols.append(col_candidate)
        df_silver = df_silver.join(
            user_profiles.select(*select_cols),
            on="user_id",
            how="left"
        )
    return df_silver


def add_partition_columns(df):
    if "timestamp" in df.columns:
        df = df.withColumn("date", F.to_date("timestamp"))
    return df


def write_silver(df, record_type):
    out_path = f"{SILVER_DIR}/record_type={record_type}"
    df = add_partition_columns(df)
    (
        df.repartition(1)
        .write.mode("overwrite")
        .partitionBy("date")
        .parquet(out_path)
    )
    print(f"[{record_type}] Silver written -> {out_path}")


def write_quarantine(df, record_type):
    out_path = f"{QUARANTINE_DIR}/record_type={record_type}"
    if df.count() == 0:
        print(f"[{record_type}] No quarantine records - skipping write")
        return
    df = add_partition_columns(df)
    (
        df.repartition(1)
        .write.mode("overwrite")
        .partitionBy("date")
        .parquet(out_path)
    )
    print(f"[{record_type}] Quarantine written -> {out_path}")


def process_record_type(record_type, refs):
    print("=" * 60)
    print(f"Processing record_type = {record_type}")
    print("=" * 60)

    df = read_bronze(record_type)
    if df is None:
        return {"record_type": record_type, "status": "skipped_no_data"}

    df = deduplicate(df, record_type)
    df = cast_and_fill(df, record_type)
    df = validate(df, record_type)
    df_silver, df_quarantine = split_silver_quarantine(df, record_type)
    df_silver = enrich(df_silver, record_type, refs)

    write_silver(df_silver, record_type)
    write_quarantine(df_quarantine, record_type)

    return {
        "record_type": record_type,
        "status": "success",
        "silver_rows": df_silver.count(),
        "quarantine_rows": df_quarantine.count(),
    }


def main():
    start = datetime.now()
    print("=" * 60)
    print("AWS PHASE 5b - GLUE BRONZE TO SILVER ETL")
    print(f"Start time: {start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Bucket: {BUCKET}")
    print("=" * 60)

    refs = load_reference_tables()

    results = []
    for record_type in RECORD_TYPES:
        try:
            result = process_record_type(record_type, refs)
        except Exception as e:
            print(f"[{record_type}] FAILED with error: {e}")
            result = {"record_type": record_type, "status": "error", "error": str(e)}
        results.append(result)

    elapsed = (datetime.now() - start).total_seconds()

    print("=" * 60)
    print("PHASE 5b SUMMARY")
    for r in results:
        print(f"  {r}")
    print(f"Elapsed: {elapsed:.1f}s")
    print("=" * 60)


main()
job.commit()
