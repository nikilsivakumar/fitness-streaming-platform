# Architecture Decision Log — Fitness Streaming Analytics Platform

This log captures *why* each architectural choice was made and, where relevant, what alternatives were considered and rejected. The project's core thesis is that the same Bronze→Silver→Gold logic should be portable across three infrastructure paradigms (Local, AWS, Databricks) — so most decisions below are framed in terms of how each stack solves the *same* problem differently, not as isolated choices.

---

## 1. Cross-cutting decisions (apply to all three stacks)

### 1.1 Medallion architecture (Bronze → Silver → Gold) as the universal pipeline shape
**Decision:** Every stack implements the same three-layer pattern: Bronze (raw, untransformed, append-only), Silver (validated/typed/deduplicated, one record type per table), Gold (business-level aggregates).
**Why:** This is the one architectural skeleton that has a clean equivalent in a hand-rolled local pipeline, in fully managed AWS services, and in Databricks/Delta Lake — which is what makes the three-way comparison possible at all. A pipeline shape unique to one stack (e.g., a pure Kafka Streams topology) wouldn't translate.
**Rejected:** A single-layer "raw → final" pipeline. Rejected because it collapses the validation/quarantine story and the "why Silver write pattern ≠ Gold write pattern" talking point, which is one of the strongest interview differentiators in this project.

### 1.2 Logic parity across stacks, not reinvention
**Decision:** When porting a transformation (e.g., fatigue scoring, quarantine rules) from Local → AWS → Databricks, always reference the existing implementation rather than redesigning it for the new platform.
**Why:** The comparative value of this project ("I built validation logic in PySpark, then in Glue, then in a Databricks notebook") collapses if the underlying logic diverges between stacks. Divergent logic would mean the comparison is between three different pipelines, not one pipeline on three platforms.

### 1.3 Silver uses MERGE INTO / upsert; Gold uses full overwrite-on-recompute
**Decision:** Silver tables (`user_profile`, `wearable_event`, `workout_log`, `sleep_log`, `nutrition_snapshot`) use idempotent upsert (MERGE INTO in Databricks; equivalent upsert logic in AWS Glue/AWS). Gold tables are always fully recomputed and overwritten on each run.
**Why:** Silver rows have a stable identity (a user, a workout session, a sleep log entry) — there is a natural merge key, so MERGE INTO gives idempotency cheaply: re-running ingestion doesn't duplicate or corrupt rows. Gold tables are aggregates (e.g., cohort-level averages, fatigue scores derived from multiple Silver sources) — there is no stable row-level key to merge on, since the *grain* of a Gold table is a computed group, not a source record. Forcing a merge key onto an aggregate would mean inventing a synthetic key purely to satisfy a write pattern, which is backwards: the data's natural structure should determine the write pattern, not the other way around.
**Rejected:** MERGE INTO on Gold using a synthetic composite key (e.g., cohort + date). Rejected because it adds complexity without adding correctness — full overwrite already guarantees Gold always reflects the current state of Silver, and a fabricated key invites subtle bugs if the aggregation grain ever changes.

### 1.4 Idempotency is layer-specific, not pipeline-wide
**Decision:** Each layer's write pattern is chosen for idempotency *given its own data characteristics* — Silver via MERGE INTO, Gold via full overwrite. There is no single "the pipeline is idempotent" claim; idempotency is argued layer by layer.
**Why:** A blanket idempotency claim invites the interview follow-up "how exactly?" — having a distinct, defensible mechanism per layer is stronger than one umbrella claim.

### 1.5 Blast-radius isolation over DRY (one notebook/job per record type)
**Decision:** Bronze→Silver transformations are written as one notebook/script per record type (`bronze_to_silver_wearable_event.py`, `bronze_to_silver_workout_log.py`, etc.) rather than a single parameterized loop over record types.
**Why:** A bug or schema change in one record type's logic (e.g., `sleep_log`) should not be able to break ingestion for the other four record types. Parameterizing into a single loop would mean one shared code path — efficient, but a single point of failure across all record types. This is an explicit architectural trade: less DRY, but failure in one pipeline can't cascade into the others.
**Rejected:** Single parameterized notebook with record-type as a config/loop variable. Rejected for the blast-radius reason above — efficiency was knowingly traded for isolation.

### 1.6 Quarantine over silent drop or hard failure for bad records
**Decision:** Records failing validation are written to a quarantine path/table rather than being silently dropped or causing the whole batch to fail.
**Why:** Silently dropping bad records hides data quality problems; failing the whole batch on one bad record makes the pipeline too brittle for real streaming data, which is never perfectly clean. Quarantining preserves the bad records for inspection while letting good records flow through — verified in DB-3 by confirming quarantine row counts match intentionally injected bad data exactly.

### 1.7 Verify against real output before declaring a phase done
**Decision:** No phase is "done" until it has actually been run and its output inspected — schema, row counts, and specific values — not just read as code or assumed correct from design intent.
**Why:** Every real bug caught in this project (wrong column name in a validation rule, `user_profile` missing from the producer's table list, the Gold grain mismatch surfaced by Athena, the `gold_community_analytics` null-cohort issue, the Workflows UI silently chaining a Gold→Gold dependency) was caught by running against real data or real UI state — never by re-reading code or config. This is treated as a working discipline, not a one-off habit, because it has a 100% catch rate against a 0% catch rate for review-by-reading in this project so far.

### 1.8 Bugfixes are committed separately from feature work
**Decision:** When a bug is found mid-session (e.g., a null-cohort issue in `gold_community_analytics`), it is committed on its own with a descriptive message, separate from whatever feature phase was in progress.
**Why:** Keeps git history legible — a reviewer (or interviewer walking through commit history) can see "this commit is a targeted fix, here's exactly what broke and why" instead of a fix buried inside a larger feature commit.

### 1.9 Decisions are logged with rejected alternatives, not just outcomes
**Decision:** This file exists specifically to capture *why a path was rejected*, not only what was chosen.
**Why:** "We used X" is a fact; "we considered Y and Z, and rejected them because..." is the actual signal of engineering judgment that an interview is testing for. A decision log that only records the winner reads as either luck or someone else's decision.

---

## 2. Local stack decisions

### 2.1 Custom `StreamBus` instead of Kafka/Redpanda
**Decision:** Built a custom lightweight `StreamBus` class in Python rather than running Kafka, Docker-based Redpanda, or another message broker locally.
**Why:** Kafka/Docker/Redpanda were attempted first and abandoned specifically due to WSL2/binary incompatibilities on Windows 10 — not a stylistic preference, a real environment constraint. `StreamBus` reproduces the conceptually important part (decoupled producer/consumer with a buffered hand-off) without requiring a Linux-container layer that this machine couldn't run reliably.
**Rejected:** Kafka, Docker, Redpanda — all rejected on this specific machine due to repeated binary/WSL2 compatibility failures, not because they're architecturally wrong. Worth stating in interviews as "I know what Kafka would give me here; on this Windows machine it didn't run reliably, so I built the minimum reproduction of the same producer/consumer contract."

### 2.2 Hive-partitioned Snappy Parquet for local storage
**Decision:** Local Bronze/Silver/Gold layers are stored as Hive-partitioned, Snappy-compressed Parquet files rather than a local database or flat CSV.
**Why:** Parquet + Hive partitioning is the same on-disk shape that S3 + Glue and Delta Lake both build on, so the local stack's storage model stays conceptually adjacent to the AWS and Databricks equivalents (object storage + partition pruning + columnar compression) even without an actual object store or table format underneath it.

### 2.3 PySpark + DuckDB combination locally
**Decision:** Use PySpark for the transformation logic and DuckDB for local query/validation, rather than picking only one.
**Why:** PySpark keeps the transformation code structurally similar to what runs in Glue and Databricks (same DataFrame API surface), which matters for the "identical logic across stacks" goal. DuckDB is used as a fast, dependency-light local query engine for verifying output — a stand-in for what Athena does on AWS and what notebook SQL does in Databricks.

---

## 3. AWS stack decisions

### 3.1 Kinesis → Lambda → Glue, not EC2/EMR or Step Functions+Airflow
**Decision:** Streaming ingestion via Kinesis (1 shard) feeding a Lambda writer (`fitness-bronze-writer`), batch transformation via Glue ETL jobs (`fitness-bronze-to-silver`, `fitness-silver-to-gold`), rather than running pipeline code on EC2/EMR or orchestrating with Step Functions/Airflow.
**Why:** This combination was deliberately benchmarked against a working data engineer's real-world stack (EC2, S3, Step Functions, PySpark, Airflow) as the comparison point, and Kinesis/Lambda/Glue was chosen instead specifically because it mirrors the *managed-service, low-ops* posture that contrasts most sharply with both the Local stack (everything self-managed) and Databricks (managed compute + managed orchestration via Workflows). The three-way comparison is strongest when AWS represents "assemble managed primitives yourself," not "run your own cluster."
**Rejected:** EC2/EMR — rejected because it would make AWS look like a re-run of the Local stack on bigger machines, losing the comparative point. Step Functions/Airflow — rejected for *this* AWS leg specifically, not because they're wrong tools, but because Glue's own job triggering covered orchestration needs at this scale, and adding Step Functions would have added orchestration complexity duplicating what Databricks Workflows demonstrates on the other leg of the comparison.

### 3.2 Athena for query/validation only, never for pipeline movement
**Decision:** Athena is used strictly to query and validate Gold tables (via Glue Crawler-populated metadata) — it never moves or transforms data as part of the pipeline itself.
**Why:** Keeps a clean separation between "the pipeline" (Kinesis/Lambda/Glue) and "how I check the pipeline's output" (Athena/DuckDB locally/notebook SQL in Databricks) — the same separation of concerns exists identically across all three stacks, which keeps the comparison fair.

### 3.3 Redshift Serverless + Data API, with star schema and SCD Type 2 on `dim_user`
**Decision:** Final analytics layer on AWS is Redshift Serverless (namespace `fitness-namespace`, workgroup `fitness-workgroup`, 8 RPU), connected via the Data API rather than a persistent JDBC connection, with a star schema and Slowly Changing Dimension Type 2 specifically on `dim_user`.
**Why:** Redshift Serverless avoids paying for an always-on cluster for a portfolio project with bursty, not continuous, usage — directly informed by the cost-discipline principle (pause Redshift immediately after use). The Data API was chosen over a persistent connection because it doesn't require holding open a database connection/credentials in long-lived application code, which fits the same "pay/run only when active" posture. SCD Type 2 on `dim_user` specifically (not on every dimension) because user attributes like `job_type`, `fitness_goal`, or `medical_history` can genuinely change over time and downstream Gold facts need to be attributable to the user's state *at the time of the fact*, not their current state — which is exactly the textbook case SCD Type 2 exists for. Other dimensions either don't change (e.g., `gym_master`) or change rarely enough that Type 1 (overwrite) is acceptable.
**Rejected:** A standard always-on Redshift provisioned cluster — rejected for cost reasons on a project with intermittent usage. SCD Type 2 on every dimension table — rejected as unnecessary complexity where dimensions are effectively static.

### 3.4 IAM as a deliberate security boundary, not an oversight
**Decision:** The working IAM identity (`retail-pipeline-dev`, inline policy `RetailPipelineAccess`, carried forward from the prior retail pipeline project) deliberately lacks `iam:CreateRole`. The one role this project needed that required elevated privilege (`RedshiftS3ReadRole`) was created via the AWS console using a separate, higher-privileged account, not by granting `retail-pipeline-dev` permission to create roles for itself.
**Why:** This demonstrates the principle of least privilege in practice: the day-to-day working identity can do its job (read/write data, run Glue/Lambda/Kinesis) without ever holding the ability to grant itself or anything else new permissions. If that identity's credentials were ever compromised, the blast radius is "can move data," not "can create arbitrary new IAM roles." This is worth stating explicitly in interviews as a security decision, not just "I happened to use a narrow policy."
**Rejected:** Granting `retail-pipeline-dev` `iam:CreateRole` directly so it could self-provision `RedshiftS3ReadRole`. Rejected because it would have collapsed the least-privilege boundary for a one-time convenience.

### 3.5 Cost-discipline as an operational decision, not an afterthought
**Decision:** Kinesis runs only during active testing (and the shard is deleted after), Glue jobs are triggered once per session rather than left on a schedule, Redshift is paused immediately after use, and a separate Athena query-results bucket is tracked explicitly for teardown.
**Why:** AWS managed services bill by usage/uptime; a portfolio project run carelessly on personal infrastructure has real, avoidable cost. Treating teardown discipline as part of the architecture (not a cleanup chore) is itself the kind of operational maturity an interviewer is checking for when asking "how do you manage cloud cost on a project like this."

### 3.6 dbt on Redshift (Phase 7) deliberately deferred, not skipped
**Decision:** AWS Phase 7 (dbt transformations on Redshift) was explicitly postponed until after the Databricks track stabilized, rather than being done in AWS-build order.
**Why:** This is a sequencing decision: stabilizing the Databricks Bronze→Gold→Workflows track first means dbt gets built once, informed by lessons already learned in two other stacks, rather than being built early and then needing rework. It is recorded here specifically so it doesn't read as an abandoned phase if the project is reviewed mid-stream.

---

## 4. Databricks stack decisions

### 4.1 Unity Catalog with a single catalog, four schemas
**Decision:** One catalog (`fitness_streaming`) with `bronze`, `silver`, `gold`, and `reference` schemas, rather than one catalog per layer or per environment.
**Why:** Keeps the medallion layering visible directly in the three-part namespace (`catalog.schema.table`) — `fitness_streaming.bronze.wearable_event` is self-describing — and matches how Unity Catalog is meant to be used (catalog as the top-level governance/sharing boundary, schema as the logical grouping inside it) rather than fragmenting governance across many catalogs for a project this size.

### 4.2 Auto Loader (`cloudFiles`) with `trigger(availableNow=True)` instead of continuous streaming
**Decision:** Bronze ingestion uses Databricks Auto Loader with `trigger(availableNow=True)` — process all currently available files once and stop — rather than a continuously running streaming job.
**Why:** This is a Lambda Architecture boundary decision made deliberately: the project doesn't need sub-second continuous ingestion, it needs reliable, idempotent, incremental batch ingestion of whatever new files have landed since the last run. `availableNow=True` gives Auto Loader's exactly-once file tracking and schema evolution handling without paying for an always-on streaming cluster between test runs — directly consistent with the cost-discipline principle applied on the AWS side.
**Rejected:** A truly continuous streaming trigger (default `trigger()` with no `availableNow`). Rejected because it would keep a cluster running between test sessions for no benefit at this data velocity, and because "batch-triggered incremental ingestion" is itself a deliberate, explainable architectural position worth defending in an interview — not a missing feature.

### 4.3 One Databricks Job, 11 tasks, coarse barrier between Silver and Gold (DB-5)
**Decision:** The entire pipeline — producer → ingestion → 5 parallel Silver tasks → all 4 Gold tasks — is orchestrated as a single Job (`fitness_streaming_db5_pipeline`), with every Gold task depending on *all five* Silver tasks (a coarse barrier), rather than each Gold task depending only on the specific Silver tables it actually reads.
**Why:** During design it was confirmed — not assumed — that no Gold task depends on another Gold task; each reads only specific Silver tables. A fine-grained dependency graph (each Gold task wired only to the exact Silver tables it touches) would technically allow more parallelism. The coarse barrier was chosen anyway: it trades a small amount of wall-clock time for a dependency model that cannot silently become wrong later. If a Gold notebook's Silver reads change (e.g., `gold_workout_consistency` starts also reading `sleep_log`), a fine-grained graph would need someone to remember to update its dependency edges, or it would run with stale/incomplete inputs without erroring. The coarse "wait for everything Silver" barrier needs no maintenance to stay correct.
**Rejected:** Fine-grained, per-table Gold dependencies. Rejected as an optimization that introduces a class of silent staleness bug for marginal wall-clock benefit at this data volume.

### 4.4 Retry policy split by failure class
**Decision:** `producer` and `ingestion` tasks get `retries=2`. All 5 Silver tasks and all 4 Gold tasks get `retries=0`.
**Why:** This mirrors a real production distinction, not a project-specific quirk: producer/ingestion failures are typically transient infrastructure issues (package install hiccups, cluster init delays) that may genuinely succeed on a second attempt. Silver and Gold tasks fail on deterministic logic — schema asserts, validation checks — and a deterministic failure will produce the exact same failure on retry every time. Retrying a deterministic failure doesn't fix anything; it only delays the alert that something is actually broken, which is worse than failing fast.
**Rejected:** A single uniform retry policy (e.g., `retries=2` everywhere). Rejected because it would mask logic failures behind a false sense of "it'll probably work eventually," and would actively slow down debugging during active development.

### 4.5 `gold_community_analytics` cohort fix: derive cohort from `silver.user_profile`, fill unmatched as "Unprofiled" — root-cause fix, not a workaround
**Decision:** When 35 users had wearable/sleep/workout activity but no matching `user_profile` row (an early-arriving fact / late-arriving dimension situation, caused by the deliberately low-frequency ~5% CDC-style `user_profile` producer event over a finite test batch), the fix was to apply `.fillna({"job_type": "Unprofiled", "fitness_goal": "Unprofiled"})` to all three joined sources *before* grouping, rather than silently letting the cohort join surface nulls, or filtering those 35 users out of the Gold table entirely.
**Why:** Filtering the unmatched users out would have been a workaround — it makes the symptom disappear (no more nulls) but discards real activity data and would understate Gold table totals without anyone noticing why. Explicitly labeling them "Unprofiled" instead preserves their activity data, makes the *reason* visible in the data itself (this is a deliberate cohort, not a data quality accident), and is honest about what the late-arriving-dimension problem actually is — some users showed up before their profile dimension caught up to them. Verified via real run output, not assumed correct: row 4 of the Gold table after the fix shows `Unprofiled/Unprofiled` with `user_count_wearable = 35`, matching the originally-flagged number exactly, and all 10 expected cohorts confirmed by the existing assert.
**Rejected:** Silent inner join (drops unmatched users, undercounts Gold totals with no trace of why) — rejected as data loss. Left join surfacing raw nulls into the Gold table (original bug) — rejected because nulls in a published Gold table read as a *bug* to any downstream consumer, when the actual situation is an explainable, expected data condition.
**Separately noted (not yet fixed, intentionally low priority):** one cohort (`mixed`/`endurance`) shows `avg_sleep_hours = null` in current output — this is a *different* failure class: no `sleep_log` rows exist for that cohort at this test data volume, so the left join to the sleep cohort table correctly has nothing to match. This is expected sparsity from a small test batch, not a join logic bug — worth documenting as a distinct category from the Unprofiled fix above (missing dimension match vs. missing source rows are different problems with different correct responses).

### 4.6 UI dependency-chaining gotcha treated as a verification target, not just a one-off catch
**Decision:** After discovering that the Databricks Workflows UI's "Depends on" field defaults to chaining a new task onto whichever task was created immediately before it (caught when `gold_community_analytics` initially saved with `gold_workout_consistency` as its dependency — a Gold→Gold chain with no real data reason, that would have run successfully and looked correct on the canvas), all 4 Gold task dependency configurations were individually re-checked rather than trusting the canvas rendering.
**Why:** This is the same "verify against real output, don't trust by reading" discipline applied to UI configuration instead of code — a wrong dependency wouldn't have thrown an error or looked wrong in the DAG view, it would have just silently serialized two independent tasks. The fact that this could pass a visual inspection and still be wrong is exactly why every task's config was checked directly rather than glanced at.

### 4.7 Removing the duplicate `run_producer(...)` call before the first Job run
**Decision:** `live_producer.py` originally had two sequential `run_producer(...)` calls (a small calibration call followed by a larger one) written for interactive, cell-by-cell notebook use. Before the first Workflows Job run, the second call was removed.
**Why:** A Job task executes the entire notebook top-to-bottom on every "Run Now," unlike interactive use where a developer might run only one cell. Left as-is, every single Job run would have silently fired *both* producer calls — doubling (effectively) the data generated and ingested per run, with no error or warning, just a quietly larger and more expensive batch every single execution. Caught and fixed before the first run specifically because of the "verify before running, not after" discipline, avoiding what would have been a silent cost/data-volume bug baked into every future run.

### 4.8 Unity Catalog automatic lineage as a structural, not incidental, advantage
**Decision:** No extra code was written to track lineage; Unity Catalog's automatic cross-pipeline lineage (the DB-5 Job run surfaced "11 upstream tables, 13 downstream tables" automatically) is treated as a first-class architectural property of the Databricks stack worth comparing directly against AWS, rather than a side detail.
**Why:** On the AWS leg of this project, equivalent lineage visibility would require manually reconstructing relationships from Glue job definitions and S3 paths, or paying for a separate catalog/lineage tool. Recording this here is intentional: it's one of the cleanest, most concrete points of contrast between the two managed-cloud approaches in the entire project, and it cost zero extra engineering effort to obtain on the Databricks side.

### 4.9 No schedule trigger yet — manual "Run Now" only, deliberately
**Decision:** The DB-5 Job's schedule is explicitly left as "None"; only manual "Run Now" executions have been performed so far.
**Why:** This is a deliberate sequencing choice, not an oversight: confidence in the pipeline's behavior across multiple runs is being built first, before committing to an unattended cron schedule that could run a flawed pipeline repeatedly and unattended. Recorded explicitly here so it isn't mistaken for an unfinished task if reviewed out of context.

---

## 5. Open items tracked here (not yet resolved)

- `gold_community_analytics`: `mixed`/`endurance` cohort `avg_sleep_hours = null` due to no `sleep_log` rows at current test data volume for that cohort — expected sparsity, not a bug; revisit if test data volume increases.
- DB-5 Job-level failure notification email: planned, not yet confirmed wired up.
- DB-5 schedule trigger: intentionally not yet set (see 4.9) — revisit once enough manual runs build confidence.
- AWS Phase 7 (dbt on Redshift): deliberately deferred until after Databricks track stabilizes (see 3.6) — now due per current project sequencing.

### 1.10 Local stack scope intentionally capped at Phase 4 — not a target deployment environment
**Decision:** The Local stack (PySpark/DuckDB/`StreamBus`) stops at Phase 4 (Bronze→Silver→Gold pipeline logic). The originally planned Phases 7–9 (dbt with a DuckDB profile, Streamlit dashboard, local orchestration) are intentionally not built.
**Why:** The Local stack's purpose in this project was always to prove the medallion logic once, on the cheapest possible iteration loop, before porting it to AWS and Databricks — not to be a third production-candidate deployment target. No real-world version of this pipeline would ever run as "local PySpark + DuckDB" in production; it's either a managed-cloud (AWS) or managed-platform (Databricks) deployment. Continuing to build out dbt/Streamlit/orchestration on a stack that will never be the deployment target would be polishing a throwaway prototype rather than investing further time in the two stacks that actually map to real hiring conversations.
**Rejected:** Building out Local Phases 7–9 for full three-way symmetry. Rejected because symmetry for its own sake has no payoff here — the comparative value of this project comes from AWS vs. Databricks trade-offs (managed primitives vs. managed platform), which Local was never positioned to be a third entry in. Local's job (prove the logic, cheaply) was already done by the end of Phase 4.
