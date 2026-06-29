-- Thin 1:1 view over the raw SCD2 user dimension. No business logic here —
-- that belongs in the marts. Renames only for consistency with naming
-- conventions used elsewhere in this project.
--
-- effective_date_for_join: dim_user.effective_date was confirmed (via
-- dbt run-operation diagnose_scd2_date_mismatch) to be the literal Glue
-- load timestamp, not a true business-validity start date — every row
-- in this test batch carries the exact same effective_date, down to
-- the second, regardless of when that user's data actually started
-- existing. This silently dropped every row in mart_user_health_score
-- (10 raw fact rows -> 0 mart rows), because fact event_dates from the
-- test producer predate the load timestamp.
--
-- Fix: for each user's EARLIEST dim_user row (rn = 1), treat the lower
-- join bound as open-ended ('1900-01-01') instead of the literal load
-- timestamp. This is the standard correct convention for a dimension's
-- first-ever SCD2 version — it should cover all history up to its
-- end_date, not just history after whenever the ETL happened to run.
-- Any LATER version (rn > 1, i.e. an actual attribute change) keeps its
-- real effective_date, since that one genuinely does mark a point of
-- change. This is a staging-layer fix specifically because it's
-- correcting a load-time artifact, not adding new business logic.

with dim_user_raw as (
    select
        user_sk,
        user_id,
        age,
        gender,
        weight_kg,
        medical_history,
        medical_risk_modifier,
        job_type,
        fitness_goal,
        gym_id,
        gym_type,
        effective_date,
        end_date,
        is_current,
        row_number() over (partition by user_id order by effective_date) as rn
    from {{ source('fitness_star_schema', 'dim_user') }}
)

select
    user_sk,
    user_id,
    age,
    gender,
    weight_kg,
    medical_history,
    medical_risk_modifier,
    job_type,
    fitness_goal,
    gym_id,
    gym_type,
    effective_date,
    end_date,
    is_current,
    case
        when rn = 1 then timestamp '1900-01-01 00:00:00'
        else effective_date
    end as effective_date_for_join
from dim_user_raw
