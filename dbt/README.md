# dbt on Redshift — AWS Phase 7

## Status: setup-only. Models not yet written.

This project starts with **introspection, not models**, because the real
Redshift star schema (table/column names for `dim_user` and the fact
tables) was built ad hoc via console/Data API and was never committed
anywhere in the repo — there's no DDL file to read, so the only honest
source of truth is the live database itself.

## Setup

```bash
cd dbt
pip install dbt-core==1.7.10 dbt-redshift==1.7.4   # already in repo requirements.txt

# 1. Copy the profile template OUTSIDE the repo
cp profiles.yml.example ~/.dbt/profiles.yml

# 2. Set connection env vars (in your shell, or a .env you source — never commit these)
export REDSHIFT_HOST="<your-workgroup-endpoint>.ap-south-1.redshift-serverless.amazonaws.com"
export REDSHIFT_DBNAME="dev"
export REDSHIFT_USER="<your-redshift-user>"
export REDSHIFT_PASSWORD="<your-redshift-password>"
export REDSHIFT_SCHEMA="<schema your star schema tables actually live in>"

# 3. Confirm dbt can reach Redshift at all
dbt debug

# 4. Run the introspection macro — this is the step that matters
dbt run-operation list_schema_columns
```

`dbt run-operation list_schema_columns` queries
`information_schema.columns` directly and prints every real table and
column in your schema to the console — no manual schema typing, no
assumptions baked into a sources.yml ahead of time.

## Next step

Paste the full console output from step 4 back into the conversation.
That becomes the actual `sources.yml`, and the staging/mart models get
written against confirmed table and column names — including handling
whatever the real `dim_user` SCD Type 2 columns and fact table grains
turn out to be, rather than the four-Gold-table shape assumed from the
Glue script alone.

## Planned model shape (subject to what introspection reveals)

```
models/
├── sources.yml                       # generated from introspection output
├── staging/
│   ├── stg_dim_user.sql
│   ├── stg_fact_fatigue_recovery.sql
│   ├── stg_fact_workout_consistency.sql
│   └── stg_fact_community_analytics.sql
└── marts/
    ├── mart_user_health_score.sql    # gold_fatigue_recovery + dim_user
    ├── mart_coach_dashboard.sql      # gold_workout_consistency + dim_user
    └── mart_community_analytics.sql  # gold_community_analytics, mostly pass-through
```
