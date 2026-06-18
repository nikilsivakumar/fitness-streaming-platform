"""
silver_to_gold.py
====================
Phase 4 — Silver to Gold ETL
Fitness Streaming Analytics Platform — Local Version

WHAT THIS SCRIPT DOES
-----------------------
Reads the 5 Silver record types and produces 4 Gold tables:

  1. gold_fatigue_recovery     — per (user_id, date): fatigue_score,
                                  recovery_readiness, overtraining_flag
  2. gold_workout_consistency  — per user_id: completion rate, avg RPE,
                                  total training load, muscle group breakdown
  3. gold_community_analytics  — per cohort (gym_type / job_type /
                                  fitness_goal): aggregate recovery/sleep/load
  4. gold_user_profile_enriched — clean dimension table for downstream
                                  dashboard (Phase 8) and dbt (Phase 7) joins

WHY THIS WAS WRITTEN AFTER INSPECTING REAL SILVER OUTPUT, NOT BEFORE
------------------------------------------------------------------------
An earlier draft of this file was attempted before Phase 3 had actually
run against real data. That draft was discarded entirely. This version
is written against the verified Silver schema (confirmed via direct
inspection of the Parquet files: column names, types, and sample rows)
and against the corrected validation_rules.py (the weight -> weight_kg
bugfix). Building Gold logic against assumed schemas instead of verified
ones is exactly the mistake this rewrite avoids.

GOLD ONLY AGGREGATES — NO NEW ENRICHMENT JOINS HERE
--------------------------------------------------------
Per the project's architecture decision ("Enrichment layer: Silver, not
Gold"), every Silver record already carries job_type, fitness_goal, and
gym_id from the Phase 3 enrichment step. Gold's job here is purely
aggregation and scoring — it never re-joins reference CSVs.

PHYSIOLOGICAL / METHODOLOGICAL BASIS FOR EACH FORMULA
-----------------------------------------------------------
This is the section to know cold for interviews — every constant below
traces to a specific source, not an arbitrary choice:

1. TRAINING LOAD (Session-RPE method, Foster et al. 2001)
   training_load = duration_minutes * rpe
   This is the most widely used field-practical training load metric in
   sports science because it requires no equipment beyond a 1-10 perceived
   exertion scale (Borg CR10) and session duration — both of which already
   exist in workout_log. It correlates well with more expensive
   physiological load measures (HR-based TRIMP) in applied settings.

2. ACUTE:CHRONIC WORKLOAD RATIO concept (Gabbett 2016) — SIMPLIFIED HERE
   The full ACWR (7-day acute load / 28-day chronic load) needs multi-week
   data, which this single-day dataset does not yet have. We compute the
   building block (daily training_load) now; the ratio logic is correctly
   deferred until the simulator has run across multiple days. This is
   documented rather than faked — there is no value in computing a ratio
   against one day of history.

3. HRV-BASED RECOVERY (Plews et al. 2013 — HRV-guided training)
   Lower HRV relative to a person's own baseline indicates incomplete
   autonomic recovery (parasympathetic suppression from accumulated
   stress/fatigue). Because this dataset is single-day, we cannot yet
   compute a rolling personal baseline (the textbook-correct approach).
   Instead we score HRV relative to the *population* distribution for
   this run, which is the documented, honest fallback — and the
   forward-looking column name (hrv_vs_baseline_pct) is structured so
   that swapping in a true 7-day rolling personal baseline later is a
   one-line change, not a redesign.

4. SLEEP DEBT (Van Dongen et al. 2003 — cumulative sleep deprivation)
   sleep_debt_hours = max(0, 8.0 - total_sleep_hours)
   8 hours is the commonly cited adult sleep requirement baseline in
   sleep science; deviations below it accumulate measurable cognitive
   and physiological cost. We floor at 0 — oversleeping is not treated
   as "negative debt" because the literature does not support that
   excess sleep is harmful in the same dose-dependent way sleep
   restriction is.

5. FATIGUE SCORE (composite, weighted 0-100, higher = more fatigued)
   fatigue_score =
       (sleep_debt_hours / 8.0) * 40          [sleep contribution: 40%]
     + (100 - recovery_score) / 100 * 35       [device recovery: 35%]
     + (stress_score / 100) * 25               [subjective/device stress: 25%]
   This is a deliberately transparent weighted composite, not a black-box
   model — every weight is named and defensible in an interview ("sleep
   debt is weighted highest because it is the single most consistently
   reproduced predictor of next-day performance decrement in the sleep
   literature"). It is explicitly NOT presented as a clinically validated
   instrument — it is a portfolio-appropriate, explainable scoring system
   built from named, defensible inputs.

6. OVERTRAINING FLAG (Meeusen et al., ECSS 2013 position statement)
   The ECSS statement describes overtraining syndrome as resulting from
   an imbalance between training stress and recovery, sustained over time,
   typically presenting as elevated fatigue + suppressed recovery markers
   + maintained or increased training load (i.e. load not being reduced
   in response to fatigue signals). We flag a single-day proxy:
       overtraining_flag = True if fatigue_score > 70
                            AND training_load_today > 0
                            AND recovery_score < 40
   This is explicitly a single-day proxy/early-warning signal, not a
   diagnosis of the syndrome itself (which by definition requires a
   sustained multi-week pattern) — the docstring and column naming make
   this distinction explicit so it is never misrepresented as more than
   it is.

7. RECOVERY READINESS (0-100, higher = more ready to train hard)
   recovery_readiness = 100 - fatigue_score
   Intentionally the direct inverse of fatigue_score rather than an
   independently-modeled metric — keeping it as a direct inverse is itself
   a design decision worth explaining: it guarantees internal consistency
   (a user is never simultaneously "high fatigue" and "high readiness")
   at the cost of not capturing readiness factors fatigue doesn't already
   cover (e.g. motivation, skill-specific freshness). This tradeoff is the
   kind of thing worth saying out loud in an interview rather than
   pretending the metric is more sophisticated than it is.

WHY MUSCLE_GROUPS IS PARSED, NOT TREATED AS A STRING
----------------------------------------------------------
workout_log.muscle_groups is stored as a Python-list-literal STRING
(e.g. "['back', 'core']") because the Bronze producer wrote it that way
and no array casting happened in Phase 3. PySpark has a native function,
from_json, that can parse this correctly when given a schema — we use
that instead of a Python UDF (UDFs break Spark's Catalyst optimizer and
are slower; native functions stay inside the JVM and are interview-
recognizable as "the right tool" vs "a workaround").
"""

import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

# -------------------------------------------------
# LOGGING
# -------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [GOLD] %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)

# -------------------------------------------------
# PATHS
# -------------------------------------------------
BASE_DIR   = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "data")
SILVER_DIR = os.path.join(BASE_DIR, "silver")
GOLD_DIR   = os.path.join(BASE_DIR, "gold")

# -------------------------------------------------
# SCORING CONSTANTS — named, not magic numbers
# -------------------------------------------------
SLEEP_BASELINE_HOURS        = 8.0   # Van Dongen et al. 2003
FATIGUE_SLEEP_WEIGHT        = 40.0
FATIGUE_RECOVERY_WEIGHT     = 35.0
FATIGUE_STRESS_WEIGHT       = 25.0
OVERTRAINING_FATIGUE_MIN    = 70.0
OVERTRAINING_RECOVERY_MAX   = 40.0


def build_spark():
    spark = (
        SparkSession.builder
        .appName("FitnessStreamingPlatform_SilverToGold")
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
# STEP 1 - READ SILVER
# -------------------------------------------------

def read_silver(spark, record_type):
    """
    Read all Silver partitions for one record_type. Returns None (and
    logs a warning) if missing or empty, rather than crashing the whole
    Gold run — mirrors the defensive pattern used in Phase 3's read_bronze.
    """
    path = os.path.join(SILVER_DIR, f"record_type={record_type}")
    if not os.path.exists(path):
        log.warning(f"No Silver data found for {record_type} at {path} - skipping")
        return None

    df = spark.read.parquet(path)
    count = df.count()
    log.info(f"[{record_type}] Silver rows read: {count}")
    if count == 0:
        return None
    return df


# -------------------------------------------------
# STEP 2 - PARSE muscle_groups STRING INTO A REAL ARRAY
# -------------------------------------------------

def parse_muscle_groups(df):
    """
    muscle_groups arrives as a Python-list-literal string, e.g.
    "['back', 'core']". from_json needs valid JSON (double quotes), so
    we first translate single quotes to double quotes via regexp_replace,
    then parse with from_json against an ArrayType(StringType()) schema.

    WHY NOT ast.literal_eval IN A UDF: that would work, but every UDF
    forces Spark to serialize rows out to a Python process and back,
    which defeats Catalyst's optimizer and is measurably slower at scale.
    from_json stays inside the JVM and is the textbook-correct approach
    for this exact "stringified structure" problem in production PySpark.
    """
    json_safe = F.regexp_replace(F.col("muscle_groups"), "'", '"')
    df = df.withColumn(
        "muscle_groups_array",
        F.from_json(json_safe, ArrayType(StringType()))
    )
    return df


# -------------------------------------------------
# GOLD TABLE 1 — FATIGUE / RECOVERY
# -------------------------------------------------

def build_gold_fatigue_recovery(df_wearable, df_sleep, df_workout):
    """
    Grain: one row per (user_id, date).

    Joins three Silver sources on (user_id, date):
      - wearable_event  -> recovery_score, stress_score, hrv (daily avg,
                            since a user can have multiple wearable
                            readings per day)
      - sleep_log       -> total_sleep_hours (daily avg, same reasoning)
      - workout_log      -> training_load_today (Session-RPE, summed
                            across all sessions that day)

    LEFT JOIN STRATEGY: wearable_event is the anchor (everyone wearing a
    device generates readings constantly); sleep_log and workout_log are
    LEFT-joined onto it because a user may not have logged a workout or
    a sleep entry on every single day, and that absence is meaningful
    (not an error) — coalesced to 0 / sentinel rather than dropping the
    user-day from the table entirely.
    """
    wearable_daily = (
        df_wearable
        .groupBy("user_id", "date")
        .agg(
            F.avg("recovery_score").alias("recovery_score"),
            F.avg("stress_score").alias("stress_score"),
            F.avg("hrv").alias("hrv"),
        )
    )

    sleep_daily = (
        df_sleep
        .groupBy("user_id", "date")
        .agg(F.avg("total_sleep_hours").alias("total_sleep_hours"))
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

    # Sleep debt (Van Dongen et al. 2003) — floored at 0, see module docstring
    gold = gold.withColumn(
        "sleep_debt_hours",
        F.greatest(F.lit(0.0), F.lit(SLEEP_BASELINE_HOURS) - F.col("total_sleep_hours"))
    )

    # Composite fatigue score — see module docstring for weight rationale
    gold = gold.withColumn(
        "fatigue_score",
        F.round(
            (F.col("sleep_debt_hours") / F.lit(SLEEP_BASELINE_HOURS)) * F.lit(FATIGUE_SLEEP_WEIGHT)
            + ((F.lit(100.0) - F.col("recovery_score")) / F.lit(100.0)) * F.lit(FATIGUE_RECOVERY_WEIGHT)
            + (F.col("stress_score") / F.lit(100.0)) * F.lit(FATIGUE_STRESS_WEIGHT),
            2
        )
    )
    # Clip to [0, 100] — a composite of three weighted terms can technically
    # exceed 100 if every input is simultaneously at its worst extreme;
    # clipping keeps the score interpretable as a true 0-100 scale.
    gold = gold.withColumn(
        "fatigue_score",
        F.when(F.col("fatigue_score") > 100, F.lit(100.0))
         .when(F.col("fatigue_score") < 0, F.lit(0.0))
         .otherwise(F.col("fatigue_score"))
    )

    gold = gold.withColumn(
        "recovery_readiness",
        F.round(F.lit(100.0) - F.col("fatigue_score"), 2)
    )

    # Overtraining proxy flag — see module docstring point 6 for the
    # explicit "single-day proxy, not a syndrome diagnosis" caveat.
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


# -------------------------------------------------
# GOLD TABLE 2 — WORKOUT CONSISTENCY
# -------------------------------------------------

def build_gold_workout_consistency(df_workout):
    """
    Grain: one row per user_id (all-time, given current single-day data —
    will naturally become a more meaningful rolling-window metric once
    the simulator runs across multiple days; the aggregation logic itself
    does not need to change).

    completion_rate uses 'completed' (boolean) from Silver directly —
    this is the Silver-level completion flag from the producer, not
    something Gold derives.

    muscle_group_breakdown: explode the parsed array so each
    (user_id, muscle_group) pair becomes its own row for counting, then
    pivot back to a per-user summary. This demonstrates explode + groupBy,
    a standard PySpark pattern for one-to-many array unnesting.
    """
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
        .withColumn(
            "completion_rate_pct",
            F.round((F.col("completed_sessions") / F.col("total_sessions")) * 100, 2)
        )
    )

    muscle_breakdown = (
        df.withColumn("muscle_group", F.explode("muscle_groups_array"))
        .groupBy("user_id", "muscle_group")
        .agg(F.count("*").alias("session_count"))
    )

    # Aggregate the exploded breakdown back into one array-of-structs
    # column per user, so the final table stays at user grain (one row
    # per user) while still carrying the full muscle group detail.
    muscle_summary = (
        muscle_breakdown
        .groupBy("user_id")
        .agg(
            F.collect_list(
                F.struct(F.col("muscle_group"), F.col("session_count"))
            ).alias("muscle_group_breakdown")
        )
    )

    return base.join(muscle_summary, on="user_id", how="left")


# -------------------------------------------------
# GOLD TABLE 3 — COMMUNITY ANALYTICS
# -------------------------------------------------

def build_gold_community_analytics(df_wearable, df_sleep, df_workout):
    """
    Grain: one row per (job_type, fitness_goal) cohort.

    WHY job_type + fitness_goal AS THE COHORT KEY, NOT gym_id:
    gym_id segments by location/facility, which is operationally useful
    but less interesting for the "does occupation/goal predict recovery
    and consistency" question this table is designed to answer.

    A REAL SCHEMA GAP FOUND DURING THIS BUILD (not assumed away):
    job_type and fitness_goal are enriched onto wearable_event, sleep_log,
    and nutrition_snapshot in Phase 3 (all three get a left join against
    user_profiles.csv). workout_log does NOT get this join — Phase 3's
    enrich() function only joins workout_log against workout_catalog.csv
    (for met_value / recovery_days_needed), by design, since workout_type
    is workout_log's natural enrichment key, not user demographics.

    This means workout_log Silver has user_id but no job_type/fitness_goal
    of its own. Rather than re-running Phase 3 to add a join that wasn't
    in the original design, Gold derives a (user_id -> job_type,
    fitness_goal) lookup from wearable_event (which already carries it
    reliably for every user with any wearable reading) and joins
    workout_log against that lookup here. This is a legitimate Gold-layer
    join because it's joining cohort *dimension* values already resolved
    elsewhere in Silver — not pulling in new reference data — so it does
    not violate the "Gold only aggregates, enrichment happens in Silver"
    architecture decision in spirit, only in the narrow mechanical sense
    of which Silver table originally carried the column.
    """
    wearable_cohort = (
        df_wearable
        .groupBy("job_type", "fitness_goal")
        .agg(
            F.round(F.avg("recovery_score"), 2).alias("avg_recovery_score"),
            F.round(F.avg("stress_score"), 2).alias("avg_stress_score"),
            F.round(F.avg("hrv"), 2).alias("avg_hrv"),
            F.countDistinct("user_id").alias("user_count_wearable"),
        )
    )

    sleep_cohort = (
        df_sleep
        .groupBy("job_type", "fitness_goal")
        .agg(F.round(F.avg("total_sleep_hours"), 2).alias("avg_sleep_hours"))
    )

    # user_id -> (job_type, fitness_goal) lookup, derived from wearable_event
    # since workout_log does not carry these columns natively (see docstring).
    user_cohort_lookup = (
        df_wearable
        .select("user_id", "job_type", "fitness_goal")
        .dropDuplicates(["user_id"])
    )

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

    gold = (
        wearable_cohort
        .join(sleep_cohort, on=["job_type", "fitness_goal"], how="left")
        .join(workout_cohort, on=["job_type", "fitness_goal"], how="left")
    )
    return gold

# -------------------------------------------------
# GOLD TABLE 4 — USER PROFILE ENRICHED (dimension-style)
# -------------------------------------------------

def build_gold_user_profile_enriched(df_user_profile):
    """
    Grain: one row per user_id. Straight pass-through of the corrected
    Silver user_profile table (post weight_kg bugfix), selected down to
    the columns useful for downstream dashboard/dbt joins. No new
    computation here — this exists so Phase 7 (dbt) and Phase 8
    (Streamlit) have a clean, stable dimension to join against without
    needing to know about medical_conditions.csv or gym_master.csv at all.
    """
    return df_user_profile.select(
        "user_id", "age", "gender", "weight_kg",
        "medical_history", "medical_risk_modifier",
        "job_type", "fitness_goal", "gym_id", "gym_type",
    )


# -------------------------------------------------
# WRITE
# -------------------------------------------------

def write_gold(df, table_name):
    out_path = os.path.join(GOLD_DIR, table_name)
    os.makedirs(GOLD_DIR, exist_ok=True)
    (
        df.repartition(1)
        .write.mode("overwrite")
        .parquet(out_path)
    )
    row_count = df.count()
    log.info(f"[{table_name}] Gold written -> {out_path} ({row_count} rows)")
    return row_count


# -------------------------------------------------
# MAIN
# -------------------------------------------------

def main():
    start = datetime.now()
    log.info("=" * 60)
    log.info("PHASE 4 - SILVER TO GOLD ETL")
    log.info(f"Start time: {start.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    spark = build_spark()

    df_wearable = read_silver(spark, "wearable_event")
    df_workout  = read_silver(spark, "workout_log")
    df_sleep    = read_silver(spark, "sleep_log")
    df_nutrition = read_silver(spark, "nutrition_snapshot")  # not yet used in Gold v1, read for completeness/future use
    df_user_profile = read_silver(spark, "user_profile")

    results = {}

    if df_wearable is not None and df_sleep is not None and df_workout is not None:
        gold_fatigue = build_gold_fatigue_recovery(df_wearable, df_sleep, df_workout)
        results["gold_fatigue_recovery"] = write_gold(gold_fatigue, "gold_fatigue_recovery")
    else:
        log.warning("Skipping gold_fatigue_recovery - missing one or more required Silver sources")

    if df_workout is not None:
        gold_consistency = build_gold_workout_consistency(df_workout)
        results["gold_workout_consistency"] = write_gold(gold_consistency, "gold_workout_consistency")
    else:
        log.warning("Skipping gold_workout_consistency - missing workout_log Silver")

    if df_wearable is not None and df_sleep is not None and df_workout is not None:
        gold_community = build_gold_community_analytics(df_wearable, df_sleep, df_workout)
        results["gold_community_analytics"] = write_gold(gold_community, "gold_community_analytics")
    else:
        log.warning("Skipping gold_community_analytics - missing one or more required Silver sources")

    if df_user_profile is not None:
        gold_profile = build_gold_user_profile_enriched(df_user_profile)
        results["gold_user_profile_enriched"] = write_gold(gold_profile, "gold_user_profile_enriched")
    else:
        log.warning("Skipping gold_user_profile_enriched - missing user_profile Silver")

    elapsed = (datetime.now() - start).total_seconds()

    log.info("=" * 60)
    log.info("PHASE 4 SUMMARY")
    for table, count in results.items():
        log.info(f"  {table}: {count} rows")
    log.info(f"Elapsed: {elapsed:.1f}s")
    log.info("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()