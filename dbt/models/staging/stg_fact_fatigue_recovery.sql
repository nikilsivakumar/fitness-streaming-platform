-- Thin 1:1 view over the daily fatigue/recovery fact. One row per
-- user per day. Grounded in Foster Session-RPE (2001), Van Dongen (2003)
-- sleep-debt research, and Meeusen/ECSS (2013) overtraining guidelines —
-- see docs/decisions.md for the scoring methodology.

select
    fact_id,
    user_id,
    event_date,
    recovery_score,
    stress_score,
    hrv,
    total_sleep_hours,
    sleep_debt_hours,
    training_load_today,
    fatigue_score,
    recovery_readiness,
    overtraining_flag
from {{ source('fitness_star_schema', 'fact_fatigue_recovery') }}
