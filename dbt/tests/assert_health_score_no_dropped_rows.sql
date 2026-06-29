-- Singular test: mart_user_health_score must have exactly as many rows
-- as the raw fact_fatigue_recovery table. This is the test that
-- catches the bug fixed in decisions.md §3.12 (effective_date being a
-- load timestamp rather than a true business-validity date silently
-- dropped 10/10 rows) from ever silently recurring — e.g. if a future
-- AWS reload changes the effective_date convention again, or end_date
-- starts being populated with a sentinel instead of NULL.
--
-- Returns a row (causing failure) if the counts ever diverge.

with raw_count as (
    select count(*) as n from {{ source('fitness_star_schema', 'fact_fatigue_recovery') }}
),

mart_count as (
    select count(*) as n from {{ ref('mart_user_health_score') }}
)

select
    raw_count.n as raw_fact_rows,
    mart_count.n as mart_rows
from raw_count, mart_count
where raw_count.n != mart_count.n
