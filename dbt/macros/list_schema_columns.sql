{# =====================================================================
   list_schema_columns()
   ---------------------------------------------------------------------
   One-off introspection macro for AWS Phase 7 setup.

   Queries Redshift's information_schema directly (no manual typing,
   no guessing) and prints every table + column in the target schema
   to the console. Run this once, paste the output back, and the real
   staging/mart models get built against confirmed names instead of
   assumed ones.

   USAGE:
     dbt run-operation list_schema_columns
     dbt run-operation list_schema_columns --args '{schema_name: some_other_schema}'

   If schema_name is omitted, it defaults to whatever schema is set
   in your active dbt target (profiles.yml).
   ===================================================================== #}

{% macro list_schema_columns(schema_name=target.schema) %}

    {% set query %}
        select
            table_name,
            column_name,
            data_type,
            ordinal_position
        from information_schema.columns
        where table_schema = '{{ schema_name }}'
        order by table_name, ordinal_position
    {% endset %}

    {% set results = run_query(query) %}

    {% if execute %}
        {{ log("=== Schema: " ~ schema_name ~ " ===", info=True) }}
        {% set current_table = "" %}
        {% for row in results.rows %}
            {% if row['table_name'] != current_table %}
                {% set current_table = row['table_name'] %}
                {{ log("", info=True) }}
                {{ log("-- " ~ current_table, info=True) }}
            {% endif %}
            {{ log("   " ~ row['column_name'] ~ "  (" ~ row['data_type'] ~ ")", info=True) }}
        {% endfor %}
        {{ log("", info=True) }}
        {{ log("=== " ~ results.rows | length ~ " columns across this schema ===", info=True) }}
    {% endif %}

{% endmacro %}
