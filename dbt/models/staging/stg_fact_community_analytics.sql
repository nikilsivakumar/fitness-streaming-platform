-- Thin 1:1 view over the cohort-level community analytics fact. Already
-- aggregated by job_type/fitness_goal upstream in the Glue Silver→Gold
-- job (including the "Unprofiled" fallback for users with no matching
-- dim_user row at the time of aggregation — see decisions.md §4.5).
-- No user-level grain, no join to dim_user needed or possible here.

select
    fact_id,
    job_type,
    fitness_goal,
    avg_recovery_score,
    avg_stress_score,
    avg_hrv,
    user_count_wearable,
    avg_sleep_hours,
    avg_training_load,
    avg_completion_rate_pct
from {{ source('fitness_star_schema', 'fact_community_analytics') }}
