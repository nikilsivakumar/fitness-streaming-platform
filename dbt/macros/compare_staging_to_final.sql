{# =====================================================================
   compare_staging_to_final()
   ---------------------------------------------------------------------
   Compares actual row content (not just counts) between
   stg_fatigue_recovery <-> fact_fatigue_recovery and
   stg_user_profile <-> dim_user, to confirm or rule out the theory
   that these are COPY-landing staging tables feeding the final
   surrogate-keyed / SCD2 tables, rather than abandoned leftovers.

   USAGE:
     dbt run-operation compare_staging_to_final
   ===================================================================== #}

{% macro compare_staging_to_final() %}

    {% set fatigue_compare %}
        select
            s.user_id,
            s.date as stg_date,
            f.event_date as fact_event_date,
            s.fatigue_score as stg_fatigue_score,
            f.fatigue_score as fact_fatigue_score
        from public.stg_fatigue_recovery s
        left join public.fact_fatigue_recovery f
            on s.user_id = f.user_id and s.date = f.event_date
        order by s.user_id, s.date
    {% endset %}

    {% set fatigue_results = run_query(fatigue_compare) %}

    {% if execute %}
        {{ log("=== stg_fatigue_recovery vs fact_fatigue_recovery ===", info=True) }}
        {% for row in fatigue_results.rows %}
            {{ log("   user_id=" ~ row['user_id'] ~ "  date=" ~ row['stg_date'] ~ "  stg_fatigue=" ~ row['stg_fatigue_score'] ~ "  fact_fatigue=" ~ row['fact_fatigue_score'] ~ "  fact_event_date=" ~ row['fact_event_date'], info=True) }}
        {% endfor %}
    {% endif %}

    {% set profile_compare %}
        select
            s.user_id,
            s.job_type as stg_job_type,
            d.job_type as dim_job_type,
            d.is_current,
            d.effective_date
        from public.stg_user_profile s
        left join public.dim_user d
            on s.user_id = d.user_id
        order by s.user_id, d.effective_date
    {% endset %}

    {% set profile_results = run_query(profile_compare) %}

    {% if execute %}
        {{ log("", info=True) }}
        {{ log("=== stg_user_profile vs dim_user ===", info=True) }}
        {% for row in profile_results.rows %}
            {{ log("   user_id=" ~ row['user_id'] ~ "  stg_job_type=" ~ row['stg_job_type'] ~ "  dim_job_type=" ~ row['dim_job_type'] ~ "  is_current=" ~ row['is_current'] ~ "  effective_date=" ~ row['effective_date'], info=True) }}
        {% endfor %}
    {% endif %}

{% endmacro %}
