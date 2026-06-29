-- mart_user_health_score
--
-- Grain: one row per user per day.
--
-- IMPORTANT DESIGN DECISION: dim_user is SCD Type 2. A user's job_type,
-- fitness_goal, or medical_risk_modifier can change over time, and this
-- fact has a real event_date — so this join uses the date to find the
-- dim_user row that was ACTUALLY ACTIVE on that day, not just whichever
-- dim_user row happens to be is_current=true today. Using is_current
-- here would silently misattribute historical fatigue scores to a
-- user's CURRENT profile instead of their profile at the time, which
-- defeats the entire purpose of building SCD2 in the first place.
--
-- Joins on effective_date_for_join (from stg_dim_user), not the raw
-- effective_date — confirmed via dbt run-operation
-- diagnose_scd2_date_mismatch that raw effective_date is a load
-- timestamp, not a true business-validity date, which was silently
-- dropping every row (10 -> 0) before this fix. See stg_dim_user.sql
-- for the full explanation and decisions.md §3.12.
--
-- end_date handling: confirmed NULL for all current rows (not a
-- sentinel value) via the same diagnostic — COALESCE to far-future is
-- correct as originally written.

with fatigue as (
    select * from {{ ref('stg_fact_fatigue_recovery') }}
),

user_history as (
    select * from {{ ref('stg_dim_user') }}
)

select
    f.fact_id,
    f.user_id,
    f.event_date,
    u.user_sk,
    u.age,
    u.gender,
    u.weight_kg,
    u.medical_history,
    u.medical_risk_modifier,
    u.job_type,
    u.fitness_goal,
    u.gym_id,
    u.gym_type,
    f.recovery_score,
    f.stress_score,
    f.hrv,
    f.total_sleep_hours,
    f.sleep_debt_hours,
    f.training_load_today,
    f.fatigue_score,
    f.recovery_readiness,
    f.overtraining_flag,
    -- Risk-adjusted fatigue score: a user with a higher medical_risk_modifier
    -- carries more weight from the same raw fatigue_score. This is the one
    -- piece of genuinely new business logic this mart adds beyond a pass-through.
    f.fatigue_score * coalesce(u.medical_risk_modifier, 1.0) as risk_adjusted_fatigue_score
from fatigue f
inner join user_history u
    on f.user_id = u.user_id
    and f.event_date >= u.effective_date_for_join
    and f.event_date < coalesce(u.end_date, date '9999-12-31')
