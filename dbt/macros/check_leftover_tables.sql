{# =====================================================================
   check_leftover_tables()
   ---------------------------------------------------------------------
   Checks row counts on the two leftover-looking tables found during
   introspection (stg_fatigue_recovery, stg_user_profile) so the
   decision to drop them is based on real evidence, not a guess that
   they're unused.

   USAGE:
     dbt run-operation check_leftover_tables
   ===================================================================== #}

{% macro check_leftover_tables() %}

    {% set query %}
        select
            (select count(*) from public.stg_fatigue_recovery) as stg_fatigue_recovery_rows,
            (select count(*) from public.stg_user_profile) as stg_user_profile_rows
    {% endset %}

    {% set results = run_query(query) %}

    {% if execute %}
        {% set r = results.rows[0] %}
        {{ log("stg_fatigue_recovery row count: " ~ r['stg_fatigue_recovery_rows'], info=True) }}
        {{ log("stg_user_profile row count:      " ~ r['stg_user_profile_rows'], info=True) }}
        {{ log("", info=True) }}
        {{ log("Neither table is referenced by any dbt source/model in this project.", info=True) }}
        {{ log("If row counts are 0, these are almost certainly safe to drop.", info=True) }}
        {{ log("If non-zero, check their content manually before dropping anything.", info=True) }}
    {% endif %}

{% endmacro %}
