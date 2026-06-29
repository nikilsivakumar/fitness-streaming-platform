-- Thin 1:1 view over the cumulative workout-consistency fact. One row
-- per user (no date grain) — this table answers "how has this user
-- trained overall," not "how did this user train on a given day."
-- muscle_group_breakdown is Redshift SUPER (semi-structured) and is
-- passed through untouched; flattening it is a mart-layer decision,
-- not a staging-layer one.

select
    fact_id,
    user_id,
    total_sessions,
    completed_sessions,
    avg_rpe,
    avg_duration_minutes,
    total_training_load,
    completion_rate_pct,
    muscle_group_breakdown
from {{ source('fitness_star_schema', 'fact_workout_consistency') }}
