"""
Wearable Simulator — generates realistic fitness data for all 5 record types.

Data sources simulated:
    1. wearable_event     — real-time biometrics (HR, HRV, steps) from device
    2. workout_log        — app event logged after each session
    3. sleep_log          — nightly device sync
    4. nutrition_snapshot — daily manual entry
    5. user_profile       — slow-changing demographic + health data

Why 5 separate record types on one stream?
    Real fitness platforms receive data from multiple sources that
    eventually need to be joined. Keeping them on one stream with
    a record_type field mirrors how Kinesis/Kafka handle multiplexed
    events. The consumer routes by record_type — same pattern as
    Lambda event routing in the AWS version.

Why inject bad data (~5%)?
    Production sensors fail. Heart rate of 0, sleep hours of 25,
    negative HRV — these happen constantly. Your quarantine layer
    needs real bad data to prove it works. If all test data is
    clean, you haven't tested your pipeline.
"""

import random
import time
import sys
import os
from datetime import datetime
from faker import Faker

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from version_local.producer.stream_bus import bus, TOPIC

fake = Faker()

# ── Constants ─────────────────────────────────────────────────────────────────

TOTAL_USERS = 50
USER_IDS = [f"U{str(i).zfill(4)}" for i in range(1001, 1001 + TOTAL_USERS)]

WORKOUT_TYPES = [
    "Strength", "CrossFit", "HIIT", "Yoga",
    "Running", "Cycling", "Swimming", "Mobility", "Rest"
]
MUSCLE_GROUPS = ["chest", "back", "legs", "shoulders", "core", "arms"]
MEDICAL_CONDITIONS = ["none", "none", "none", "hypertension", "diabetes"]
JOB_TYPES = ["sedentary", "active", "mixed"]
FITNESS_GOALS = ["strength", "endurance", "weight_loss", "longevity"]
DEVICES = ["AppleWatch", "Garmin", "Fitbit", "Whoop", "Samsung"]
GYM_IDS = [f"GYM{i}" for i in range(1, 6)]


# ── Record Generators ──────────────────────────────────────────────────────────

def generate_wearable_event(user_id: str) -> dict:
    """
    Real-time biometric stream.
    Intentionally injects ~5% invalid records to test quarantine layer.
    
    Valid ranges (based on exercise physiology):
        heart_rate : 30–220 bpm (resting to max effort)
        hrv        : 10–150 ms  (poor to excellent recovery)
        sleep_hours: not applicable here (separate record type)
    """
    is_bad = random.random() < 0.05  # 5% bad record injection rate

    return {
        "record_type": "wearable_event",
        "user_id": user_id,
        "timestamp": datetime.utcnow().isoformat(),
        "heart_rate": random.randint(220, 280) if is_bad else random.randint(45, 185),
        "steps": random.randint(0, 900),
        "hrv": -1 if is_bad else random.randint(15, 110),
        "calories_burned": random.randint(50, 650),
        "recovery_score": random.randint(25, 100),
        "stress_score": random.randint(10, 90),
        "device_type": random.choice(DEVICES)
    }


def generate_workout_log(user_id: str) -> dict:
    """
    App event triggered after workout completion.
    RPE (Rate of Perceived Exertion) is a validated psychophysical scale
    used in sports science — 1 = very easy, 10 = maximal effort.
    Including it makes your data model scientifically grounded,
    which you can discuss in interviews with health-tech companies.
    """
    return {
        "record_type": "workout_log",
        "user_id": user_id,
        "timestamp": datetime.utcnow().isoformat(),
        "workout_type": random.choice(WORKOUT_TYPES),
        "duration_minutes": random.randint(15, 90),
        "rpe": random.randint(1, 10),
        "muscle_groups": random.sample(MUSCLE_GROUPS, k=random.randint(1, 3)),
        "completed": random.choices([True, False], weights=[75, 25])[0],
        "gym_id": random.choice(GYM_IDS)
    }


def generate_sleep_log(user_id: str) -> dict:
    """
    Nightly device sync — sleep architecture data.
    
    Sleep stage proportions based on sleep science:
        Deep sleep : 15–25% of total (physical recovery)
        REM sleep  : 20–25% of total (cognitive recovery, memory)
    These proportions are used later to compute recovery scores —
    a technically defensible metric you can explain in interviews.
    """
    total = round(random.uniform(4.0, 9.5), 1)
    deep  = round(total * random.uniform(0.15, 0.25), 1)
    rem   = round(total * random.uniform(0.20, 0.25), 1)

    return {
        "record_type": "sleep_log",
        "user_id": user_id,
        "timestamp": datetime.utcnow().isoformat(),
        "total_sleep_hours": total,
        "deep_sleep_hours": deep,
        "rem_hours": rem,
        "awakenings": random.randint(0, 7),
        "sleep_quality_score": random.randint(35, 100)
    }


def generate_nutrition_snapshot(user_id: str) -> dict:
    """
    Daily macro tracking entry.
    Calorie and macro ranges reflect realistic adult intake variation.
    Hydration data enables dehydration-fatigue correlation in Gold layer.
    """
    return {
        "record_type": "nutrition_snapshot",
        "user_id": user_id,
        "timestamp": datetime.utcnow().isoformat(),
        "calories_in": random.randint(1200, 3800),
        "protein_g": random.randint(50, 220),
        "carbs_g": random.randint(80, 450),
        "fat_g": random.randint(30, 130),
        "hydration_ml": random.randint(800, 4500)
    }


def generate_user_profile(user_id: str) -> dict:
    """
    Slow-changing dimensional data (SCD).
    
    Why this is on the stream at low frequency (5%):
        In production, profile updates come from a registration/CRM system
        as change events. Putting them on the same stream at low frequency
        simulates CDC (Change Data Capture) — a pattern you'll be asked
        about in senior DE interviews.
        
        The dbt snapshot layer later handles SCD Type 2 — preserving
        history when a user's job_type or medical_history changes.
    """
    return {
        "record_type": "user_profile",
        "user_id": user_id,
        "timestamp": datetime.utcnow().isoformat(),
        "age": random.randint(18, 65),
        "gender": random.choice(["male", "female", "other"]),
        "weight_kg": round(random.uniform(45, 115), 1),
        "height_cm": random.randint(150, 200),
        "medical_history": random.choice(MEDICAL_CONDITIONS),
        "job_type": random.choice(JOB_TYPES),
        "fitness_goal": random.choice(FITNESS_GOALS),
        "gym_id": random.choice(GYM_IDS)
    }


# ── Weighted Record Type Distribution ─────────────────────────────────────────
# Mirrors real-world event frequency:
# Biometrics are continuous → highest volume
# Profile updates are rare → lowest volume

GENERATORS = [
    (generate_wearable_event,    0.50),
    (generate_workout_log,       0.20),
    (generate_sleep_log,         0.15),
    (generate_nutrition_snapshot,0.10),
    (generate_user_profile,      0.05),
]


def pick_generator():
    """Weighted random selection — mirrors Kinesis partition key distribution."""
    roll = random.random()
    cumulative = 0.0
    for generator, weight in GENERATORS:
        cumulative += weight
        if roll < cumulative:
            return generator
    return GENERATORS[0][0]


# ── Main Simulator ─────────────────────────────────────────────────────────────

def run_simulator(
    records_per_batch: int = 10,
    sleep_seconds: float = 1.0,
    max_batches: int = None,
    silent: bool = False
):
    """
    Pushes batches of records to StreamBus continuously.
    
    Args:
        records_per_batch : records per cycle (mirrors Kinesis batch size)
        sleep_seconds     : pause between batches (controls throughput)
        max_batches       : stop after N batches (None = run until Ctrl+C)
        silent            : suppress print output (for orchestration use)
    """
    if not silent:
        print("=" * 60)
        print("Fitness Wearable Simulator")
        print(f"Topic   : {TOPIC}")
        print(f"Users   : {TOTAL_USERS}")
        print(f"Batch   : {records_per_batch} records every {sleep_seconds}s")
        print("Press Ctrl+C to stop" if max_batches is None else f"Running {max_batches} batches")
        print("=" * 60)

    batch_count = 0
    total_pushed = 0

    try:
        while True:
            for _ in range(records_per_batch):
                user_id   = random.choice(USER_IDS)
                generator = pick_generator()
                record    = generator(user_id)

                bus.produce(
                    topic=TOPIC,
                    record=record,
                    partition_key=user_id  # same user → same partition → ordered
                )
                total_pushed += 1

            batch_count += 1

            if not silent:
                stats = bus.get_stats(TOPIC)
                print(
                    f"[{datetime.utcnow().strftime('%H:%M:%S')}] "
                    f"Batch {batch_count} | "
                    f"Pushed {records_per_batch} | "
                    f"Total in stream: {stats['total_records']}"
                )

            if max_batches and batch_count >= max_batches:
                break

            time.sleep(sleep_seconds)

    except KeyboardInterrupt:
        pass

    if not silent:
        print(f"\nSimulator stopped. Total pushed: {total_pushed}")

    return total_pushed


if __name__ == "__main__":
    run_simulator(records_per_batch=10, sleep_seconds=1.0)