-- Redshift star schema DDL — fitness_dw.public
--
-- Reconstructed via `dbt run-operation generate_star_schema_ddl`
-- (queries information_schema.columns directly against the live
-- Redshift Serverless instance) on 2026-06-26, because the original
-- DDL was run ad hoc via console/Data API during AWS Phase 6 and was
-- never committed anywhere — this closes that gap (decisions.md §3.7).
--
-- NOTE: this is documentation-grade DDL, not a guaranteed byte-for-byte
-- restore script. It correctly captures column names, types, and
-- NOT NULL constraints as they actually exist today. It does NOT
-- capture DISTKEY/SORTKEY/compression encodings or PRIMARY/FOREIGN KEY
-- constraints (Redshift doesn't enforce these anyway, but they're
-- useful documentation if they were originally specified) — add those
-- manually if/when confirmed.
--
-- Two related staging tables exist in this schema as well
-- (stg_fatigue_recovery, stg_user_profile) — these appear to be the
-- COPY-landing tables that fact_fatigue_recovery and dim_user are
-- built from (raw row counts and content match the final tables,
-- minus surrogate keys / SCD2 columns). Not included here since
-- they're load-process infrastructure, not the analytical star schema
-- itself — see decisions.md for the investigation that confirmed this.

-- ============================================
-- dim_user: SCD Type 2 user dimension
-- ============================================
CREATE TABLE public.dim_user (
    user_sk                  BIGINT NOT NULL,
    user_id                  VARCHAR(20) NOT NULL,
    age                      INTEGER,
    gender                   VARCHAR(20),
    weight_kg                DOUBLE PRECISION,
    medical_history          VARCHAR(50),
    medical_risk_modifier    DOUBLE PRECISION,
    job_type                 VARCHAR(30),
    fitness_goal             VARCHAR(30),
    gym_id                   VARCHAR(20),
    gym_type                 VARCHAR(30),
    effective_date           TIMESTAMP NOT NULL,
    end_date                 TIMESTAMP,
    is_current               BOOLEAN NOT NULL
);

-- ============================================
-- fact_fatigue_recovery: one row per user per day
-- ============================================
CREATE TABLE public.fact_fatigue_recovery (
    fact_id                  BIGINT NOT NULL,
    user_id                  VARCHAR(20),
    event_date               DATE,
    recovery_score           DOUBLE PRECISION,
    stress_score              DOUBLE PRECISION,
    hrv                       DOUBLE PRECISION,
    total_sleep_hours         DOUBLE PRECISION,
    sleep_debt_hours          DOUBLE PRECISION,
    training_load_today       BIGINT,
    fatigue_score             DOUBLE PRECISION,
    recovery_readiness        DOUBLE PRECISION,
    overtraining_flag         BOOLEAN
);

-- ============================================
-- fact_workout_consistency: one cumulative row per user
-- ============================================
CREATE TABLE public.fact_workout_consistency (
    fact_id                   BIGINT NOT NULL,
    user_id                   VARCHAR(20),
    total_sessions            BIGINT,
    completed_sessions        BIGINT,
    avg_rpe                   DOUBLE PRECISION,
    avg_duration_minutes      DOUBLE PRECISION,
    total_training_load       BIGINT,
    completion_rate_pct       DOUBLE PRECISION,
    muscle_group_breakdown    SUPER
);

-- ============================================
-- fact_community_analytics: one row per (job_type, fitness_goal) cohort
-- ============================================
CREATE TABLE public.fact_community_analytics (
    fact_id                   BIGINT NOT NULL,
    job_type                  VARCHAR(30),
    fitness_goal               VARCHAR(30),
    avg_recovery_score         DOUBLE PRECISION,
    avg_stress_score           DOUBLE PRECISION,
    avg_hrv                    DOUBLE PRECISION,
    user_count_wearable         BIGINT,
    avg_sleep_hours             DOUBLE PRECISION,
    avg_training_load           DOUBLE PRECISION,
    avg_completion_rate_pct     DOUBLE PRECISION
);
