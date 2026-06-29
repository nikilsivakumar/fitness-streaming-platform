{# =====================================================================
   list_databases()
   ---------------------------------------------------------------------
   Lists every database in this Redshift Serverless namespace, not just
   the one the current connection (profiles.yml dbname) is pointed at.
   Use this when the connected database's schemas come back empty —
   the star schema may live in a different database under the same
   namespace, which requires changing REDSHIFT_DBNAME and reconnecting,
   not just changing schema.

   USAGE:
     dbt run-operation list_databases
   ===================================================================== #}

{% macro list_databases() %}

    {% set query %}
        select datname
        from pg_database
        order by datname
    {% endset %}

    {% set results = run_query(query) %}

    {% if execute %}
        {{ log("=== Databases visible in this namespace ===", info=True) }}
        {% for row in results.rows %}
            {{ log("   " ~ row['datname'], info=True) }}
        {% endfor %}
        {{ log("=== " ~ results.rows | length ~ " databases total ===", info=True) }}
        {{ log("", info=True) }}
        {{ log("If your data isn't in 'dev', set REDSHIFT_DBNAME to the right one and reconnect (dbt debug) before re-running list_all_tables.", info=True) }}
    {% endif %}

{% endmacro %}