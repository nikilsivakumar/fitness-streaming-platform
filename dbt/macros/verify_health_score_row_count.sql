{# =====================================================================
   verify_health_score_row_count()
   ---------------------------------------------------------------------
   Compares raw fact_fatigue_recovery row count against
   mart_user_health_score row count. They should match exactly.

   If they don't, the point-in-time SCD2 join in mart_user_health_score
   (event_date >= effective_date AND event_date < COALESCE(end_date,
   '9999-12-31')) is dropping rows — either because some fatigue
   records fall outside any dim_user SCD2 window (late-arriving
   dimension), or because end_date uses a sentinel value rather than
   NULL for the current row, breaking the COALESCE assumption.

   USAGE:
     dbt run-operation verify_health_score_row_count
   ===================================================================== #}

{% macro verify_health_score_row_count() %}

    {% set query %}
        select
            (select count(*) from {{ source('fitness_star_schema', 'fact_fatigue_recovery') }}) as raw_fact_rows,
            (select count(*) from {{ ref('mart_user_health_score') }}) as mart_rows
    {% endset %}

    {% set results = run_query(query) %}

    {% if execute %}
        {% set raw_count = results.rows[0]['raw_fact_rows'] %}
        {% set mart_count = results.rows[0]['mart_rows'] %}
        {{ log("raw_fact_rows:  " ~ raw_count, info=True) }}
        {{ log("mart_rows:      " ~ mart_count, info=True) }}
        {% if raw_count == mart_count %}
            {{ log("MATCH — no rows dropped by the SCD2 point-in-time join.", info=True) }}
        {% else %}
            {{ log("MISMATCH — " ~ (raw_count - mart_count) ~ " row(s) dropped. Investigate end_date convention or late-arriving-dimension gaps before treating Phase 7 as done.", info=True) }}
        {% endif %}
    {% endif %}

{% endmacro %}
