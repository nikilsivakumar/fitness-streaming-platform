{# =====================================================================
   diagnose_scd2_date_mismatch()
   ---------------------------------------------------------------------
   Prints the actual min/max effective_date and end_date from dim_user,
   and the actual min/max event_date from fact_fatigue_recovery, plus
   a few raw sample rows from each. Run this when the point-in-time
   SCD2 join drops more rows than expected, to see the real date
   ranges instead of guessing why they don't overlap.

   USAGE:
     dbt run-operation diagnose_scd2_date_mismatch
   ===================================================================== #}

{% macro diagnose_scd2_date_mismatch() %}

    {% set ranges_query %}
        select
            (select min(effective_date) from {{ source('fitness_star_schema', 'dim_user') }}) as min_effective_date,
            (select max(effective_date) from {{ source('fitness_star_schema', 'dim_user') }}) as max_effective_date,
            (select min(end_date) from {{ source('fitness_star_schema', 'dim_user') }}) as min_end_date,
            (select max(end_date) from {{ source('fitness_star_schema', 'dim_user') }}) as max_end_date,
            (select count(*) from {{ source('fitness_star_schema', 'dim_user') }} where end_date is null) as null_end_date_count,
            (select min(event_date) from {{ source('fitness_star_schema', 'fact_fatigue_recovery') }}) as min_event_date,
            (select max(event_date) from {{ source('fitness_star_schema', 'fact_fatigue_recovery') }}) as max_event_date
    {% endset %}

    {% set results = run_query(ranges_query) %}

    {% if execute %}
        {% set r = results.rows[0] %}
        {{ log("dim_user.effective_date range: " ~ r['min_effective_date'] ~ "  to  " ~ r['max_effective_date'], info=True) }}
        {{ log("dim_user.end_date range:       " ~ r['min_end_date'] ~ "  to  " ~ r['max_end_date'], info=True) }}
        {{ log("dim_user rows with end_date IS NULL: " ~ r['null_end_date_count'], info=True) }}
        {{ log("fact_fatigue_recovery.event_date range: " ~ r['min_event_date'] ~ "  to  " ~ r['max_event_date'], info=True) }}
    {% endif %}

    {% set sample_query %}
        select user_id, effective_date, end_date, is_current
        from {{ source('fitness_star_schema', 'dim_user') }}
        order by user_id
        limit 5
    {% endset %}

    {% set sample_results = run_query(sample_query) %}

    {% if execute %}
        {{ log("", info=True) }}
        {{ log("Sample dim_user rows:", info=True) }}
        {% for row in sample_results.rows %}
            {{ log("   user_id=" ~ row['user_id'] ~ "  effective_date=" ~ row['effective_date'] ~ "  end_date=" ~ row['end_date'] ~ "  is_current=" ~ row['is_current'], info=True) }}
        {% endfor %}
    {% endif %}

    {% set fact_sample_query %}
        select user_id, event_date
        from {{ source('fitness_star_schema', 'fact_fatigue_recovery') }}
        order by user_id
        limit 5
    {% endset %}

    {% set fact_sample_results = run_query(fact_sample_query) %}

    {% if execute %}
        {{ log("", info=True) }}
        {{ log("Sample fact_fatigue_recovery rows:", info=True) }}
        {% for row in fact_sample_results.rows %}
            {{ log("   user_id=" ~ row['user_id'] ~ "  event_date=" ~ row['event_date'], info=True) }}
        {% endfor %}
    {% endif %}

{% endmacro %}
