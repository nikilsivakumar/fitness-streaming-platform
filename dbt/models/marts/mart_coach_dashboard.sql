-- mart_coach_dashboard
--
-- Grain: one row per user (cumulative, no date dimension).
--
-- DESIGN NOTE — different join from mart_user_health_score on purpose:
-- fact_workout_consistency has no event_date; it's a running cumulative
-- total per user, not a daily snapshot. There is no "point in time" to
-- look up here, so joining to dim_user WHERE is_current = true is the
-- correct choice for THIS fact — not a shortcut. Contrast with
-- mart_user_health_score, which must NOT take this shortcut because
-- that fact does have a real date grain. Both joins are deliberately
-- different, for reasons specific to each fact's grain.

with workouts as (
    select * from {{ ref('stg_fact_workout_consistency') }}
),

current_users as (
    select * from {{ ref('stg_dim_user') }}
    where is_current = true
)

select
    w.fact_id,
    w.user_id,
    u.gym_id,
    u.gym_type,
    u.fitness_goal,
    u.job_type,
    w.total_sessions,
    w.completed_sessions,
    w.avg_rpe,
    w.avg_duration_minutes,
    w.total_training_load,
    w.completion_rate_pct,
    w.muscle_group_breakdown
from workouts w
left join current_users u
    on w.user_id = u.user_id
