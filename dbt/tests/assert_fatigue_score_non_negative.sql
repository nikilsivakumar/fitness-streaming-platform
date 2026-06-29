-- Singular test: fatigue_score (and the risk-adjusted version) should
-- never be negative — a negative fatigue score has no physiological
-- meaning under the Foster Session-RPE scoring model this is based on.
-- Returns offending rows if violated.

select *
from {{ ref('mart_user_health_score') }}
where fatigue_score < 0
   or risk_adjusted_fatigue_score < 0
