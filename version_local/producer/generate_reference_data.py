"""
Reference Data Generator — creates static dimensional data files.

Why static files instead of streaming?
    These are slowly-changing dimensions (SCDs). User demographics
    and gym details don't change every second — they change occasionally.
    Loading them as static CSVs into a reference zone, then joining
    at ETL time, is the standard pattern in every production data lake.

    In Phase 6 (Redshift), user_profile becomes a Type 2 SCD table —
    meaning when a user's job_type or medical_history changes, we keep
    the old record AND insert a new one with validity dates. This lets
    you answer: "what was this user's profile when they logged this workout
    6 months ago?" — which matters for longitudinal health analytics.

Files generated:
    1. user_profiles.csv     — 50 users, demographic + health data
    2. gym_master.csv        — 5 gyms, location + capacity
    3. workout_catalog.csv   — reference table of workout types + attributes
    4. medical_conditions.csv— reference for condition severity scoring
    5. date_dimension.csv    — pre-built date table (standard DW pattern)
"""

import os
import sys
import pandas as pd
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker('en_IN')  # Indian locale for realistic Chennai-area data
random.seed(42)        # Reproducible data — same file every run

OUTPUT_PATH = os.path.join(
    os.path.dirname(__file__), '..', '..', 'data', 'reference'
)

TOTAL_USERS = 50
USER_IDS    = [f"U{str(i).zfill(4)}" for i in range(1001, 1001 + TOTAL_USERS)]
GYM_IDS     = [f"GYM{i}" for i in range(1, 6)]


# ── 1. User Profiles ──────────────────────────────────────────────────────────

def generate_user_profiles() -> pd.DataFrame:
    """
    Master user dimension table.

    Columns designed for analytics depth:
        - age + medical_history → risk stratification
        - job_type → sedentary behavior correlation with fitness metrics
        - fitness_goal → personalization layer in recommendation engine
        - member_since → cohort analysis (how do 2024 members vs 2025 members differ?)
        - bmi → computed field, demonstrates derived column pattern

    BMI formula: weight(kg) / height(m)^2
    WHO classifications used: Underweight <18.5, Normal 18.5-24.9,
    Overweight 25-29.9, Obese ≥30
    """
    records = []

    for user_id in USER_IDS:
        weight_kg  = round(random.uniform(48, 112), 1)
        height_cm  = random.randint(152, 198)
        height_m   = height_cm / 100
        bmi        = round(weight_kg / (height_m ** 2), 1)

        if bmi < 18.5:
            bmi_category = "underweight"
        elif bmi < 25:
            bmi_category = "normal"
        elif bmi < 30:
            bmi_category = "overweight"
        else:
            bmi_category = "obese"

        # Member since: random date in last 3 years
        days_ago     = random.randint(30, 1095)
        member_since = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')

        records.append({
            "user_id":          user_id,
            "name":             fake.name(),
            "age":              random.randint(18, 65),
            "gender":           random.choice(["male", "female", "other"]),
            "weight_kg":        weight_kg,
            "height_cm":        height_cm,
            "bmi":              bmi,
            "bmi_category":     bmi_category,
            "medical_history":  random.choice([
                                    "none", "none", "none",
                                    "hypertension", "diabetes"
                                ]),
            "job_type":         random.choice(["sedentary", "active", "mixed"]),
            "fitness_goal":     random.choice([
                                    "strength", "endurance",
                                    "weight_loss", "longevity"
                                ]),
            "gym_id":           random.choice(GYM_IDS),
            "member_since":     member_since,
            "is_active":        random.choices([True, False], weights=[85, 15])[0],
            "city":             random.choice([
                                    "Chennai", "Bengaluru", "Mumbai",
                                    "Hyderabad", "Coimbatore"
                                ]),
        })

    return pd.DataFrame(records)


# ── 2. Gym Master ─────────────────────────────────────────────────────────────

def generate_gym_master() -> pd.DataFrame:
    """
    Gym dimension table.

    gym_type drives different expected workout patterns:
        - crossfit   → high RPE, short duration, compound movements
        - commercial → varied, moderate intensity
        - yoga_studio→ low RPE, flexibility focus
        - functional → moderate-high RPE, mobility emphasis
        - performance→ highest RPE, sport-specific

    This enables community analytics like:
        "Do CrossFit members show higher fatigue scores than yoga members?"
    """
    gyms = [
        {
            "gym_id":       "GYM1",
            "name":         "Iron Temple Fitness",
            "city":         "Chennai",
            "area":         "Tambaram",
            "gym_type":     "functional",
            "capacity":     80,
            "monthly_fee":  1500,
            "established":  2019,
            "has_pool":     False,
            "has_sauna":    False,
        },
        {
            "gym_id":       "GYM2",
            "name":         "CrossFit Velachery",
            "city":         "Chennai",
            "area":         "Velachery",
            "gym_type":     "crossfit",
            "capacity":     45,
            "monthly_fee":  3500,
            "established":  2021,
            "has_pool":     False,
            "has_sauna":    False,
        },
        {
            "gym_id":       "GYM3",
            "name":         "Gold's Gym Adyar",
            "city":         "Chennai",
            "area":         "Adyar",
            "gym_type":     "commercial",
            "capacity":     200,
            "monthly_fee":  2500,
            "established":  2015,
            "has_pool":     True,
            "has_sauna":    True,
        },
        {
            "gym_id":       "GYM4",
            "name":         "Zen Yoga Studio",
            "city":         "Chennai",
            "area":         "Anna Nagar",
            "gym_type":     "yoga_studio",
            "capacity":     30,
            "monthly_fee":  2000,
            "established":  2020,
            "has_pool":     False,
            "has_sauna":    False,
        },
        {
            "gym_id":       "GYM5",
            "name":         "Performance Lab",
            "city":         "Chennai",
            "area":         "OMR",
            "gym_type":     "performance",
            "capacity":     60,
            "monthly_fee":  4500,
            "established":  2022,
            "has_pool":     False,
            "has_sauna":    True,
        },
    ]
    return pd.DataFrame(gyms)


# ── 3. Workout Catalog ────────────────────────────────────────────────────────

def generate_workout_catalog() -> pd.DataFrame:
    """
    Reference table for workout types with physiological attributes.

    met_value (Metabolic Equivalent of Task):
        Standardized measure of exercise intensity from exercise physiology.
        1 MET = energy at rest. Running = ~8 MET. Yoga = ~2.5 MET.
        Used in Gold layer to compute estimated caloric expenditure:
            calories = MET × weight_kg × duration_hours

    This makes your calorie estimates scientifically defensible —
    important for any health-tech interview.
    """
    workouts = [
        {"workout_type": "Strength",  "category": "resistance", "met_value": 5.0,
         "primary_benefit": "muscle_gain",    "recovery_days_needed": 2},
        {"workout_type": "CrossFit",  "category": "mixed",      "met_value": 8.0,
         "primary_benefit": "conditioning",   "recovery_days_needed": 2},
        {"workout_type": "HIIT",      "category": "cardio",     "met_value": 8.5,
         "primary_benefit": "fat_loss",       "recovery_days_needed": 1},
        {"workout_type": "Yoga",      "category": "flexibility","met_value": 2.5,
         "primary_benefit": "mobility",       "recovery_days_needed": 0},
        {"workout_type": "Running",   "category": "cardio",     "met_value": 8.0,
         "primary_benefit": "endurance",      "recovery_days_needed": 1},
        {"workout_type": "Cycling",   "category": "cardio",     "met_value": 7.5,
         "primary_benefit": "endurance",      "recovery_days_needed": 1},
        {"workout_type": "Swimming",  "category": "cardio",     "met_value": 7.0,
         "primary_benefit": "full_body",      "recovery_days_needed": 1},
        {"workout_type": "Mobility",  "category": "flexibility","met_value": 2.0,
         "primary_benefit": "injury_prevention","recovery_days_needed": 0},
        {"workout_type": "Rest",      "category": "recovery",   "met_value": 1.0,
         "primary_benefit": "recovery",       "recovery_days_needed": 0},
    ]
    return pd.DataFrame(workouts)


# ── 4. Medical Conditions Reference ──────────────────────────────────────────

def generate_medical_conditions() -> pd.DataFrame:
    """
    Reference table for medical condition risk modifiers.

    risk_modifier is applied in Gold layer fatigue scoring:
        A diabetic user with the same HRV and sleep as a healthy user
        should receive a higher risk flag — their physiological
        margin for error is smaller.

    This is what separates a generic fitness app from a
    health-aware analytics platform. Critical differentiator
    for your project narrative.
    """
    conditions = [
        {"condition":     "none",
         "risk_modifier": 1.0,
         "hr_max_factor": 1.0,
         "notes":         "Standard calculations apply"},

        {"condition":     "hypertension",
         "risk_modifier": 1.3,
         "hr_max_factor": 0.85,
         "notes":         "Target HR zones reduced; fatigue threshold lower"},

        {"condition":     "diabetes",
         "risk_modifier": 1.25,
         "hr_max_factor": 0.90,
         "notes":         "Monitor for hypoglycemia during exercise"},
    ]
    return pd.DataFrame(conditions)


# ── 5. Date Dimension ─────────────────────────────────────────────────────────

def generate_date_dimension(
    start_date: str = "2026-01-01",
    end_date:   str = "2026-12-31"
) -> pd.DataFrame:
    """
    Pre-built date dimension table — standard in every data warehouse.

    Why a date dimension?
        Instead of computing DATEPART(month, timestamp) in every query,
        you join to dim_date and filter on month_name = 'May'.
        This is faster, more readable, and enables business-friendly
        labels (week_of_year, is_weekend, quarter) without repeated logic.

        Every DE interview at a company with a DW will assume you know this.
    """
    dates = pd.date_range(start=start_date, end=end_date, freq='D')

    records = []
    for d in dates:
        records.append({
            "date_id":        d.strftime('%Y%m%d'),
            "full_date":      d.strftime('%Y-%m-%d'),
            "year":           d.year,
            "quarter":        d.quarter,
            "month":          d.month,
            "month_name":     d.strftime('%B'),
            "week_of_year":   d.isocalendar()[1],
            "day_of_week":    d.dayofweek,
            "day_name":       d.strftime('%A'),
            "is_weekend":     d.dayofweek >= 5,
            "day_of_month":   d.day,
            "day_of_year":    d.dayofyear,
        })

    return pd.DataFrame(records)


# ── Main Generator ────────────────────────────────────────────────────────────

def generate_all():
    os.makedirs(OUTPUT_PATH, exist_ok=True)

    files = {
        "user_profiles.csv":        generate_user_profiles(),
        "gym_master.csv":           generate_gym_master(),
        "workout_catalog.csv":      generate_workout_catalog(),
        "medical_conditions.csv":   generate_medical_conditions(),
        "date_dimension.csv":       generate_date_dimension(),
    }

    print("Generating reference data files...\n")

    for filename, df in files.items():
        filepath = os.path.join(OUTPUT_PATH, filename)
        df.to_csv(filepath, index=False)
        print(f"  ✓ {filename:<30} {len(df):>4} rows  →  {filepath}")

    print(f"\n✅ All reference files written to {OUTPUT_PATH}")
    return files


if __name__ == "__main__":
    generate_all()