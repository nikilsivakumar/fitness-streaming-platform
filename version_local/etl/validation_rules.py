"""
validation_rules.py
====================
Phase 3 — Bronze to Silver ETL
Fitness Streaming Analytics Platform — Local Version

WHAT THIS MODULE DOES
----------------------
Defines physiologically grounded validation rules for each of the 5 record
types. Each rule function takes a Spark DataFrame and returns it with two
new columns added:

  is_valid        (boolean)  — True if the record passes all checks
  failure_reason  (string)   — Null if valid, otherwise a short code
                                explaining which rule failed

WHY VALIDATION RULES LIVE IN A SEPARATE FILE
-----------------------------------------------
bronze_to_silver.py orchestrates reading, validating, quarantining, and
writing. Keeping the actual validation logic separate means:
  - Rules can be unit tested in isolation
  - The same rule set can be reused if we ever validate at Bronze write time
  - Interview answer: "validation logic is decoupled from orchestration —
    single responsibility principle applied to ETL code"

PHYSIOLOGICAL BASIS FOR EACH RULE
------------------------------------
wearable_event:
  heart_rate    30-220 bpm   — below 30 is incompatible with consciousness,
                               above 220 exceeds the theoretical max HR
                               (220 - age) for any realistic adult
  hrv           > 0 ms       — HRV is a time-domain measurement (RMSSD),
                               physically cannot be negative or zero
  steps         0-50000/day  — upper bound covers ultra-marathon territory;
                               anything beyond is almost certainly a sensor
                               glitch or unit error
  recovery_score 0-100       — this is a normalised score by definition

workout_log:
  duration_minutes  1-300     — a workout under 1 minute or beyond 5 hours
                                is not a realistic single session
  rpe               1-10      — Borg CR10 scale, hard bounds by definition

sleep_log:
  total_sleep_hours  0-16     — beyond 16 hours of continuous sleep in a
                                24h period is not physiologically typical
                                outside of specific medical conditions
  deep_sleep_hours   <= total_sleep_hours  — a component cannot exceed
                                the whole
  rem_hours          <= total_sleep_hours  — same logic

nutrition_snapshot:
  calories_in   0-10000   — covers even extreme bulk/competitive eating
                            ranges without admitting clearly erroneous values
  protein_g     0-500
  hydration_ml  0-10000

user_profile:
  age      13-100   — reasonable bounds for a fitness app user base
  weight   30-250 kg
"""

from pyspark.sql import functions as F


def _base_columns(df):
    """
    Ensure is_valid starts True and failure_reason starts Null for every row.
    Every validation function builds on top of this base.
    """
    return df.withColumn("is_valid", F.lit(True)).withColumn("failure_reason", F.lit(None).cast("string"))


def _apply_rule(df, condition_fails, reason_code):
    """
    Apply one validation rule on top of existing is_valid / failure_reason.

    WHY WE CHAIN RULES THIS WAY
    -----------------------------
    A record can fail multiple rules. We only want to record the FIRST
    failure reason (so we don't overwrite a meaningful reason with a less
    important one), and a record already marked invalid stays invalid.

    condition_fails: a boolean Spark Column expression that is True when
                      the rule is VIOLATED (not when it passes)
    """
    return df.withColumn(
        "is_valid",
        F.when(condition_fails, F.lit(False)).otherwise(F.col("is_valid"))
    ).withColumn(
        "failure_reason",
        F.when(condition_fails & F.col("failure_reason").isNull(), F.lit(reason_code))
         .otherwise(F.col("failure_reason"))
    )


def validate_wearable_event(df):
    """
    Validate wearable_event records.
    This is the ONLY record type with intentionally injected bad data
    (5% per the project spec) — heart_rate out of range and negative HRV
    are the two documented failure modes.
    """
    df = _base_columns(df)

    df = _apply_rule(
        df,
        ~F.col("heart_rate").between(30, 220) | F.col("heart_rate").isNull(),
        "heart_rate_out_of_range"
    )
    df = _apply_rule(
        df,
        (F.col("hrv") <= 0) | F.col("hrv").isNull(),
        "hrv_negative_or_zero"
    )
    df = _apply_rule(
        df,
        ~F.col("steps").between(0, 50000) | F.col("steps").isNull(),
        "steps_out_of_range"
    )
    df = _apply_rule(
        df,
        F.col("recovery_score").isNotNull() & ~F.col("recovery_score").between(0, 100),
        "recovery_score_out_of_range"
    )
    return df


def validate_workout_log(df):
    """Validate workout_log records."""
    df = _base_columns(df)

    df = _apply_rule(
        df,
        ~F.col("duration_minutes").between(1, 300) | F.col("duration_minutes").isNull(),
        "duration_out_of_range"
    )
    df = _apply_rule(
        df,
        ~F.col("rpe").between(1, 10) | F.col("rpe").isNull(),
        "rpe_out_of_range"
    )
    return df


def validate_sleep_log(df):
    """Validate sleep_log records."""
    df = _base_columns(df)

    df = _apply_rule(
        df,
        ~F.col("total_sleep_hours").between(0, 16) | F.col("total_sleep_hours").isNull(),
        "total_sleep_out_of_range"
    )
    df = _apply_rule(
        df,
        F.col("deep_sleep_hours") > F.col("total_sleep_hours"),
        "deep_sleep_exceeds_total"
    )
    df = _apply_rule(
        df,
        F.col("rem_hours") > F.col("total_sleep_hours"),
        "rem_exceeds_total"
    )
    return df


def validate_nutrition_snapshot(df):
    """Validate nutrition_snapshot records."""
    df = _base_columns(df)

    df = _apply_rule(
        df,
        ~F.col("calories_in").between(0, 10000) | F.col("calories_in").isNull(),
        "calories_out_of_range"
    )
    df = _apply_rule(
        df,
        F.col("protein_g").isNotNull() & ~F.col("protein_g").between(0, 500),
        "protein_out_of_range"
    )
    df = _apply_rule(
        df,
        F.col("hydration_ml").isNotNull() & ~F.col("hydration_ml").between(0, 10000),
        "hydration_out_of_range"
    )
    return df


def validate_user_profile(df):
    """Validate user_profile records."""
    df = _base_columns(df)

    df = _apply_rule(
        df,
        ~F.col("age").between(13, 100) | F.col("age").isNull(),
        "age_out_of_range"
    )
    df = _apply_rule(
        df,
        F.col("weight_kg").isNotNull() & ~F.col("weight_kg").between(30, 250),
        "weight_out_of_range"
    )
    return df


# Dispatch table — maps record_type string to its validation function.
# WHY A DISPATCH DICT: bronze_to_silver.py needs to call the right function
# per partition without a long if/elif chain. This is the cleaner pattern
# and makes adding a 6th record type later a one-line change.
VALIDATORS = {
    "wearable_event":      validate_wearable_event,
    "workout_log":         validate_workout_log,
    "sleep_log":           validate_sleep_log,
    "nutrition_snapshot":  validate_nutrition_snapshot,
    "user_profile":        validate_user_profile,
}