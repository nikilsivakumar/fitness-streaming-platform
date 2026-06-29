-- mart_community_analytics
--
-- Grain: one row per (job_type, fitness_goal) cohort.
--
-- This is intentionally close to a pass-through. The real aggregation
-- work already happened upstream in the Glue Silver→Gold job, including
-- the "Unprofiled" fallback for users with wearable/sleep/workout
-- activity but no matching dim_user row at aggregation time (see
-- decisions.md §4.5 — same root-cause fix as the Databricks track,
-- ported deliberately rather than re-solved from scratch).
--
-- The only thing added here is a readiness label for dashboard
-- consumption, so a BI tool doesn't need its own thresholding logic.
--
-- PLACEHOLDER THRESHOLDS (70 / 40): these are illustrative, not derived
-- from the Foster/Van Dongen/Meeusen literature backing the fatigue
-- score itself. Replace with validated cutoffs (or remove the label
-- entirely) before this mart is treated as something a coach actually
-- acts on — do not let an invented threshold be mistaken for a
-- scientifically grounded one.

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
    avg_completion_rate_pct,
    case
        when avg_recovery_score >= 70 then 'high_readiness'
        when avg_recovery_score >= 40 then 'moderate_readiness'
        else 'low_readiness'
    end as cohort_readiness_label
from {{ ref('stg_fact_community_analytics') }}
