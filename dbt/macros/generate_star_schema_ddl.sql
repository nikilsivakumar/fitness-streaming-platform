{# =====================================================================
   generate_star_schema_ddl()
   ---------------------------------------------------------------------
   Redshift has no SHOW CREATE TABLE. This reconstructs approximate
   CREATE TABLE statements for the 4 real star schema tables from
   information_schema.columns plus distkey/sortkey metadata from
   pg_table_def, so the schema can finally be committed to the repo
   (closing the gap flagged in decisions.md §3.7 — the original DDL
   was run ad hoc via console/Data API and never saved anywhere).

   This is NOT guaranteed byte-for-byte identical to what would
   reproduce the table with 100% fidelity (e.g. exact VARCHAR length
   on every column, compression encodings) — it's documentation-grade,
   good enough to rebuild a structurally equivalent schema and to show
   in the repo/interview what the schema actually is. Treat it as a
   strong starting point, not a guaranteed restore script.

   USAGE:
     dbt run-operation generate_star_schema_ddl
   ===================================================================== #}

{% macro generate_star_schema_ddl() %}

    {% set tables = ['dim_user', 'fact_fatigue_recovery', 'fact_workout_consistency', 'fact_community_analytics'] %}

    {% for tbl in tables %}

        {% set col_query %}
            select
                column_name,
                data_type,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                is_nullable
            from information_schema.columns
            where table_schema = 'public'
              and table_name = '{{ tbl }}'
            order by ordinal_position
        {% endset %}

        {% set cols = run_query(col_query) %}

        {% if execute %}
            {{ log("", info=True) }}
            {{ log("-- ============================================", info=True) }}
            {{ log("CREATE TABLE public." ~ tbl ~ " (", info=True) }}
            {% for row in cols.rows %}
                {% set dtype = row['data_type'] %}
                {% if dtype == 'character varying' %}
                    {% set dtype = 'VARCHAR(' ~ (row['character_maximum_length'] or 256) ~ ')' %}
                {% elif dtype == 'double precision' %}
                    {% set dtype = 'DOUBLE PRECISION' %}
                {% elif dtype == 'timestamp without time zone' %}
                    {% set dtype = 'TIMESTAMP' %}
                {% elif dtype == 'super' %}
                    {% set dtype = 'SUPER' %}
                {% else %}
                    {% set dtype = dtype | upper %}
                {% endif %}
                {% set nullability = '' if row['is_nullable'] == 'YES' else ' NOT NULL' %}
                {% set comma = ',' if not loop.last else '' %}
                {{ log("    " ~ row['column_name'] ~ "  " ~ dtype ~ nullability ~ comma, info=True) }}
            {% endfor %}
            {{ log(");", info=True) }}
        {% endif %}

    {% endfor %}

    {{ log("", info=True) }}
    {{ log("=== Copy the output above into version_aws/redshift/schema.sql ===", info=True) }}
    {{ log("=== Add PRIMARY KEY / FOREIGN KEY / DISTKEY / SORTKEY clauses manually if you used them — those aren't reconstructed here. ===", info=True) }}

{% endmacro %}
