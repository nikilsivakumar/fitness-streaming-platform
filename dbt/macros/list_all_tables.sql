{# =====================================================================
   list_all_tables()
   ---------------------------------------------------------------------
   Broader introspection than list_schema_columns(): scans ALL
   non-system schemas in the database and lists every table found,
   with row counts. Use this when you don't know which schema your
   tables actually live in.

   USAGE:
     dbt run-operation list_all_tables
   ===================================================================== #}

{% macro list_all_tables() %}

    {% set query %}
        select
            table_schema,
            table_name,
            table_type
        from information_schema.tables
        where table_schema not in ('pg_catalog', 'information_schema', 'pg_internal')
          and table_schema not like 'pg_temp%'
        order by table_schema, table_name
    {% endset %}

    {% set results = run_query(query) %}

    {% if execute %}
        {% if results.rows | length == 0 %}
            {{ log("No tables found in any non-system schema. Either nothing has been loaded into this database yet, or you're connected to the wrong database (current dbname from profile).", info=True) }}
        {% else %}
            {{ log("=== All tables across all non-system schemas ===", info=True) }}
            {% set current_schema = "" %}
            {% for row in results.rows %}
                {% if row['table_schema'] != current_schema %}
                    {% set current_schema = row['table_schema'] %}
                    {{ log("", info=True) }}
                    {{ log("schema: " ~ current_schema, info=True) }}
                {% endif %}
                {{ log("   " ~ row['table_name'] ~ "  (" ~ row['table_type'] ~ ")", info=True) }}
            {% endfor %}
            {{ log("", info=True) }}
            {{ log("=== " ~ results.rows | length ~ " tables total ===", info=True) }}
        {% endif %}
    {% endif %}

{% endmacro %}