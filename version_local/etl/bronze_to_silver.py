"""
bronze_to_silver.py
=====================
Phase 3 — Bronze to Silver ETL
Fitness Streaming Analytics Platform — Local Version

WHAT THIS SCRIPT DOES
-----------------------
For each of the 5 record types:
  1. Read raw Bronze Parquet (Hive-partitioned by record_type/year/month/day)
  2. Deduplicate on (user_id, timestamp)
  3. Cast types and handle nulls using sentinel values (never silent drops)
  4. Validate using the rules in validation_rules.py
  5. Split into Silver (valid) and Quarantine (invalid, with failure_reason)
  6. Enrich Silver records by joining reference data:
       - user_profile.csv  -> user demographics
       - gym_master.csv    -> gym details
       - workout_catalog.csv -> MET values, recovery days (workout_log only)
       - medical_conditions.csv -> risk modifiers (joined via user_profile)
  7. Write Silver Parquet, Hive-partitioned by record_type/date
  8. Write Quarantine Parquet, same partitioning, same schema + failure_reason

WHY BRONZE IS NEVER MODIFIED
--------------------------------
This script only READS from Bronze. It never writes back. If a bug is found
in this script, Bronze remains the source of truth and you simply re-run
this script after fixing the bug. This is what "Bronze immutability" means
in practice, not just in theory.

WHY WE PROCESS RECORD TYPES SEPARATELY
-------------------------------------------
Each record type has a different validation rule set and a different
reference data enrichment join. Processing them as 5 separate DataFrames
(rather than one giant DataFrame with sparse columns) keeps the schema
clean and the validation logic readable. This mirrors how Glue ETL jobs
are typically organised on AWS — one job per logical entity, or one job
with clearly separated branches per type.
"""

import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

from validation_rules import VALIDATORS

# -------------------------------------------------
# LOGGING
# -------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SILVER] %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)

# -------------------------------------------------
# PATHS
# -------------------------------------------------
BASE_DIR        = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "data")
BRONZE_DIR      = os.path.join(BASE_DIR, "bronze")
SILVER_DIR      = os.path.join(BASE_DIR, "silver")
QUARANTINE_DIR  = os.path.join(BASE_DIR, "quarantine")
REFERENCE_DIR   = os.path.join(BASE_DIR, "reference")

RECORD_TYPES = [
    "wearable_event",
    "workout_log",
    "sleep_log",
    "nutrition_snapshot",
    "user_profile",
]

# Sentinel defaults applied per record type BEFORE validation.
# WHY SENTINELS, NOT DROPS: a null heart_rate is still a row worth keeping
# in the audit trail. We replace nulls with an out-of-range sentinel value
# so the validation rule catches it and routes it to quarantine with a
# clear reason, rather than the row silently disappearing during a cast.
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

# Expected schema per record type — column name -> Spark type string.
# WHY EXPLICIT SCHEMAS: Bronze was written by a separate process (the
# wearable simulator). If that process changes a column name or type later,
# this script should fail LOUDLY at the cast step rather than silently
# producing garbage downstream. Explicit casting is a defensive pattern.
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


def build_spark():
    """
    Local SparkSession — same configuration philosophy as Phase 4:
    local[*] uses all available cores, no cluster manager needed.
    """
    spark = (
        SparkSession.builder
        .appName("FitnessStreamingPlatform_BronzeToSilver")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession initialized - local[*]")
    return spark


# -------------------------------------------------
# STEP 1 - READ BRONZE FOR A GIVEN RECORD TYPE
# -------------------------------------------------

def read_bronze(spark, record_type):
    """
    Read all Bronze partitions for one record_type.
    Bronze is Hive-partitioned as record_type=<type>/year=YYYY/month=MM/day=DD/

    Returns None if no Bronze data exists for this type yet (defensive —
    lets the caller skip cleanly rather than crash the whole pipeline run).
    """
    path = os.path.join(BRONZE_DIR, f"record_type={record_type}")
    if not os.path.exists(path):
        log.warning(f"No Bronze data found for {record_type} at {path} - skipping")
        return None

    df = spark.read.parquet(path)
    count = df.count()
    log.info(f"[{record_type}] Bronze rows read: {count}")
    if count == 0:
        return None
    return df


# -------------------------------------------------
# STEP 2 - DEDUPLICATE
# -------------------------------------------------

def deduplicate(df, record_type):
    """
    Deduplicate on (user_id, timestamp) - the natural key for an event stream.

    WHY THIS KEY: a wearable could theoretically send the same reading twice
    due to a network retry on the producer side (this happens in real
    streaming systems constantly - at-least-once delivery semantics).
    We keep the FIRST occurrence (arbitrary but consistent) using row_number.
    """
    if "user_id" not in df.columns or "timestamp" not in df.columns:
        log.warning(f"[{record_type}] Missing user_id or timestamp - skipping dedup")
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
        log.info(f"[{record_type}] Deduplication removed {before - after} duplicate rows")
    return df_deduped


# -------------------------------------------------
# STEP 3 - CAST TYPES AND APPLY SENTINEL DEFAULTS
# -------------------------------------------------

def cast_and_fill(df, record_type):
    """
    Cast each expected column to its correct type, then fill nulls with
    the sentinel defaults defined above.

    DEFENSIVE BEHAVIOUR: if an expected column is missing entirely from
    Bronze (e.g. producer schema drifted), we create it as a null column
    of the correct type rather than crashing. This keeps the pipeline
    running and lets validation catch the resulting sentinel-filled nulls
    downstream, which surfaces the issue via quarantine counts rather
    than an unhandled exception.
    """
    schema = EXPECTED_SCHEMA.get(record_type, {})

    for col_name, col_type in schema.items():
        if col_name not in df.columns:
            log.warning(f"[{record_type}] Expected column '{col_name}' missing from Bronze - creating as null")
            df = df.withColumn(col_name, F.lit(None).cast(col_type))
        else:
            df = df.withColumn(col_name, F.col(col_name).cast(col_type))

    sentinels = SENTINEL_DEFAULTS.get(record_type, {})
    if sentinels:
        df = df.fillna(sentinels)

    return df


# -------------------------------------------------
# STEP 4 - VALIDATE
# -------------------------------------------------

def validate(df, record_type):
    """
    Apply the record-type-specific validation function from validation_rules.py.
    Adds is_valid and failure_reason columns.
    """
    validator_fn = VALIDATORS.get(record_type)
    if validator_fn is None:
        log.warning(f"[{record_type}] No validator defined - marking all rows valid")
        return df.withColumn("is_valid", F.lit(True)).withColumn("failure_reason", F.lit(None).cast("string"))

    return validator_fn(df)


# -------------------------------------------------
# STEP 5 - SPLIT SILVER / QUARANTINE
# -------------------------------------------------

def split_silver_quarantine(df, record_type):
    """
    Split validated DataFrame into Silver (valid rows) and Quarantine
    (invalid rows, retaining failure_reason).

    Silver drops is_valid (no longer needed once split) but KEEPS
    failure_reason as null - this keeps schema identical to quarantine
    for easier downstream UNION if ever needed for audit purposes.
    """
    df_silver = df.filter(F.col("is_valid") == True).drop("is_valid")
    df_quarantine = df.filter(F.col("is_valid") == False).drop("is_valid")

    silver_count = df_silver.count()
    quarantine_count = df_quarantine.count()
    total = silver_count + quarantine_count
    pct_quarantined = (quarantine_count / total * 100) if total > 0 else 0.0

    log.info(f"[{record_type}] Silver: {silver_count} | Quarantine: {quarantine_count} ({pct_quarantined:.1f}%)")

    return df_silver, df_quarantine


# -------------------------------------------------
# STEP 6 - ENRICHMENT (REFERENCE DATA JOINS)
# -------------------------------------------------

def load_reference_tables(spark):
    """
    Load all 5 reference CSVs into Spark DataFrames once, reused across
    all record type enrichment joins.

    WHY LOAD ONCE: these are small static files (50, 5, 9, 3, 365 rows).
    Loading once and reusing avoids redundant disk reads per record type.
    """
    refs = {}

    user_profiles_path = os.path.join(REFERENCE_DIR, "user_profiles.csv")
    if os.path.exists(user_profiles_path):
        refs["user_profiles"] = spark.read.option("header", True).option("inferSchema", True).csv(user_profiles_path)
        log.info(f"Loaded reference: user_profiles.csv ({refs['user_profiles'].count()} rows)")
    else:
        log.warning("user_profiles.csv not found in reference data")
        refs["user_profiles"] = None

    gym_master_path = os.path.join(REFERENCE_DIR, "gym_master.csv")
    if os.path.exists(gym_master_path):
        refs["gym_master"] = spark.read.option("header", True).option("inferSchema", True).csv(gym_master_path)
        log.info(f"Loaded reference: gym_master.csv ({refs['gym_master'].count()} rows)")
    else:
        refs["gym_master"] = None

    workout_catalog_path = os.path.join(REFERENCE_DIR, "workout_catalog.csv")
    if os.path.exists(workout_catalog_path):
        refs["workout_catalog"] = spark.read.option("header", True).option("inferSchema", True).csv(workout_catalog_path)
        log.info(f"Loaded reference: workout_catalog.csv ({refs['workout_catalog'].count()} rows)")
    else:
        refs["workout_catalog"] = None

    medical_conditions_path = os.path.join(REFERENCE_DIR, "medical_conditions.csv")
    if os.path.exists(medical_conditions_path):
        refs["medical_conditions"] = spark.read.option("header", True).option("inferSchema", True).csv(medical_conditions_path)
        log.info(f"Loaded reference: medical_conditions.csv ({refs['medical_conditions'].count()} rows)")
    else:
        refs["medical_conditions"] = None

    return refs


def enrich(df_silver, record_type, refs):
    """
    Join Silver records with relevant reference data.

    WHY ENRICHMENT HAPPENS IN SILVER, NOT GOLD
    -----------------------------------------------
    Per the project's architecture decisions table: "Enrichment layer:
    Silver, not Gold - Gold only aggregates, join logic stays in one place."
    This means Gold never needs to know about gym_master.csv or
    workout_catalog.csv at all - by the time Gold reads Silver, every
    record already carries its full context.

    user_profile records get demographic context from user_profiles.csv
    plus a medical_risk_modifier derived from medical_conditions.csv.

    workout_log records get MET value and recovery_days_required from
    workout_catalog.csv, matched on workout_type.

    wearable_event, sleep_log, and nutrition_snapshot all get the user's
    gym_type via a join through user_profiles -> gym_master, since gym
    context is relevant for community analytics segmentation later.
    """
    user_profiles = refs.get("user_profiles")
    gym_master = refs.get("gym_master")
    workout_catalog = refs.get("workout_catalog")
    medical_conditions = refs.get("medical_conditions")

    if record_type == "user_profile":
        # user_profile records ARE the dimension being enriched - join
        # medical_conditions to compute a risk modifier directly.
        if medical_conditions is not None and "medical_history" in df_silver.columns:
            # medical_conditions.csv expected to have: condition_name, risk_modifier
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
                gym_master.select(
                    F.col("gym_id"),
                    F.col("gym_type")
                ),
                on="gym_id",
                how="left"
            )
        return df_silver

    if record_type == "workout_log":
        if workout_catalog is not None and "workout_type" in df_silver.columns:
            # workout_catalog.csv expected to have: workout_type, met_value, recovery_days_required
            df_silver = df_silver.join(
                workout_catalog.select("workout_type", "met_value", "recovery_days_needed"),
                on="workout_type",
                how="left"
            )
        return df_silver

    # wearable_event, sleep_log, nutrition_snapshot:
    # join user_profiles for job_type/fitness_goal context, useful for
    # community analytics segmentation in Gold later.
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


# -------------------------------------------------
# STEP 7/8 - WRITE SILVER AND QUARANTINE
# -------------------------------------------------

def add_partition_columns(df):
    """
    Add a 'date' column derived from timestamp, used for Hive partitioning.
    record_type is already known per-DataFrame so it's added as a literal
    column before writing.
    """
    if "timestamp" in df.columns:
        df = df.withColumn("date", F.to_date("timestamp"))
    return df


def write_silver(df, record_type):
    out_path = os.path.join(SILVER_DIR, f"record_type={record_type}")
    os.makedirs(SILVER_DIR, exist_ok=True)

    df = add_partition_columns(df)
    (
        df.repartition(1)
        .write.mode("overwrite")
        .partitionBy("date")
        .parquet(out_path)
    )
    log.info(f"[{record_type}] Silver written -> {out_path}")


def write_quarantine(df, record_type):
    out_path = os.path.join(QUARANTINE_DIR, f"record_type={record_type}")
    os.makedirs(QUARANTINE_DIR, exist_ok=True)

    if df.count() == 0:
        log.info(f"[{record_type}] No quarantine records - skipping write")
        return

    df = add_partition_columns(df)
    (
        df.repartition(1)
        .write.mode("overwrite")
        .partitionBy("date")
        .parquet(out_path)
    )
    log.info(f"[{record_type}] Quarantine written -> {out_path}")


# -------------------------------------------------
# MAIN - PROCESS ONE RECORD TYPE END TO END
# -------------------------------------------------

def process_record_type(spark, record_type, refs):
    """
    Full pipeline for a single record type: read, dedupe, cast, validate,
    split, enrich, write. Returns a summary dict for the final report.
    """
    log.info("=" * 60)
    log.info(f"Processing record_type = {record_type}")
    log.info("=" * 60)

    df = read_bronze(spark, record_type)
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
    log.info("=" * 60)
    log.info("PHASE 3 - BRONZE TO SILVER ETL")
    log.info(f"Start time: {start.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    spark = build_spark()
    refs = load_reference_tables(spark)

    results = []
    for record_type in RECORD_TYPES:
        try:
            result = process_record_type(spark, record_type, refs)
        except Exception as e:
            log.error(f"[{record_type}] FAILED with error: {e}")
            result = {"record_type": record_type, "status": "error", "error": str(e)}
        results.append(result)

    elapsed = (datetime.now() - start).total_seconds()

    log.info("=" * 60)
    log.info("PHASE 3 SUMMARY")
    for r in results:
        log.info(f"  {r}")
    log.info(f"Elapsed: {elapsed:.1f}s")
    log.info("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()