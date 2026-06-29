-- Singular test: completion_rate_pct must fall within a sane 0-100 range.
-- Returns offending rows if violated; dbt fails the test if anything
-- comes back. No dbt_utils dependency needed for this simple a check.

select *
from {{ ref('mart_coach_dashboard') }}
where completion_rate_pct < 0
   or completion_rate_pct > 100
