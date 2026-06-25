# Interview Q&A — Fitness Streaming Analytics Platform

Project-specific questions and answers, organized by theme. Answers are written the way you'd actually say them out loud — specific, grounded in real numbers/names from this project, with the "why" always attached to the "what."

---

## A. Project framing & motivation

**1. Why did you build the same pipeline three times instead of going deep on one stack?**
Most candidates can show one pipeline on one stack. Building the same Bronze→Silver→Gold logic on a hand-rolled local stack, on AWS managed services, and on Databricks lets me talk about *why* each platform makes a given decision differently, not just *that* I used the platform. "I built the validation logic in PySpark, then in Glue, then in a Databricks notebook" is a stronger interview sentence than "I know PySpark." It's a comparison engine, not three disconnected demos.

**2. What's the actual business domain, and why fitness data?**
Wearable/workout/sleep/nutrition streaming data, modeled as five record types feeding a medallion pipeline (Bronze raw events → Silver validated/typed records → Gold business aggregates like fatigue-recovery scores and community cohort analytics). Fitness data has good shape for this: high-frequency wearable events (true streaming), slower-changing dimensions (user profiles), and genuinely meaningful aggregates (fatigue scoring grounded in real exercise science — Foster Session-RPE 2001, Van Dongen 2003, Meeusen/ECSS 2013) — so the Gold layer isn't arbitrary, it's defensible.

**3. What would you say is the single biggest differentiator of this project versus a typical bootcamp pipeline project?**
The comparative structure, and the decision log behind it. Anyone can follow a tutorial to build one pipeline. Being able to say "Silver uses MERGE INTO because rows have stable identity, Gold uses full overwrite because aggregates don't have a merge key — and here's why that's true in PySpark, in Glue, and in Databricks" shows the reasoning transfers across tools, which is what actually matters on the job.

---

## B. Medallion architecture & general concepts

**4. Walk me through your medallion architecture.**
Bronze is raw, untransformed, append-only — exactly what arrived, no judgment calls. Silver is validated, typed, deduplicated, one table per record type (`user_profile`, `wearable_event`, `workout_log`, `sleep_log`, `nutrition_snapshot`) — this is where bad records get pulled into quarantine rather than silently dropped or failing the whole batch. Gold is business-level aggregates — fatigue/recovery scores, workout consistency, community cohort analytics, enriched user profiles — computed from Silver, not from Bronze directly.

**5. Why is Silver one table per record type instead of one wide table?**
Blast-radius isolation. If I parameterize a single notebook/job to loop over record types, a bug in the `sleep_log` validation logic can take down ingestion for all five record types at once, since they'd share a code path. Five separate notebooks costs more in code duplication but means a schema change or bug in one record type can't cascade into the others. That's a deliberate trade, not an oversight — I have the loop-based version in my head and chose against it.

**6. Why does Silver use MERGE INTO and Gold use full overwrite? Isn't that inconsistent?**
It looks inconsistent until you look at the grain. Silver rows have stable identity — a specific user, a specific workout session — so there's a real merge key, and MERGE INTO gives idempotency cheaply: rerun ingestion, no duplicates. Gold rows are aggregates — a cohort, a recovery score across days — there's no natural row-level key to merge on. I could invent a synthetic key just to use MERGE INTO, but that's solving a problem that doesn't exist; full overwrite already guarantees Gold matches current Silver state every run. The two layers chose write patterns from their own data shape, not from a desire for uniformity.

**7. How do you handle bad/malformed data?**
Quarantine, not drop, not hard-fail. Records that fail Silver validation get written to a quarantine path/table instead of vanishing or blowing up the whole batch. I verified this directly in DB-3 by injecting known-bad rows and confirming the quarantine count matched exactly — not just trusting that the logic looked right.

**8. What's your definition of "done" for a pipeline phase?**
It ran against real data and I inspected the actual output — row counts, specific values, schema — not "the code looks correct" and not "no exception was thrown." Multiple real bugs in this project were only caught this way: a wrong column name in a validation rule, `user_profile` missing entirely from a producer's table list, a Gold table's actual columns differing from what I'd assumed before checking Athena, and a Databricks Workflows UI dependency silently chaining two tasks that looked fine on the canvas. Zero of those were caught by re-reading code.

---

## C. Local stack

**9. Why didn't you use Kafka locally?**
I tried — Kafka via Docker, and Redpanda as a lighter alternative — and hit real WSL2/binary incompatibilities on Windows 10 that didn't resolve with reasonable effort. So I built a minimal `StreamBus` class that reproduces the part that actually matters conceptually: a decoupled producer/consumer hand-off with buffering. It's not "Kafka but worse," it's "the smallest thing that gives me the same producer/consumer contract on hardware that wouldn't run the real thing reliably."

**10. What storage format did you use locally, and why?**
Hive-partitioned, Snappy-compressed Parquet. That's not arbitrary — it's the same on-disk shape that underlies both S3+Glue (AWS) and Delta Lake (Databricks): columnar, compressed, partition-pruned. Keeping the local storage model conceptually adjacent to the other two stacks means the comparison stays apples-to-apples even without an actual object store underneath it.

**11. Why both PySpark and DuckDB locally instead of just one?**
PySpark for transformation logic, to keep the code structurally close to what runs in Glue and in Databricks notebooks — same DataFrame API shape, so "the logic ported" claim is actually true, not hand-waved. DuckDB for fast local query/validation — a lightweight stand-in for what Athena does on AWS and what notebook SQL does in Databricks.

**12. What's left to finish on the Local stack?**
dbt with a DuckDB profile, a Streamlit dashboard, and local orchestration (currently manual/sequential). Those are deliberately the last things built, after the comparative pipeline logic itself across all three stacks was solid.

**13. If you had Linux/WSL2 working cleanly, would you go back and use Kafka?**
Possibly, for the realism of true pub/sub semantics — consumer groups, offsets, replay. But I'd say it explicitly as a "this is the more production-realistic choice if the environment supports it" rather than pretending `StreamBus` is functionally equivalent to Kafka. It isn't; it's a deliberately minimal reproduction of one property Kafka has.

---

## D. AWS stack

**14. Walk me through the AWS pipeline end to end.**
Kinesis (1 shard) receives streaming events → a Lambda (`fitness-bronze-writer`, Python 3.13, AWSSDKPandas layer) writes them to Bronze in S3 → Glue ETL job `fitness-bronze-to-silver` validates/types/quarantines into Silver → Glue ETL job `fitness-silver-to-gold` aggregates into Gold → a Glue Crawler (`fitness-gold-crawler`) catalogs Gold into Athena for querying → Redshift Serverless holds the final star schema for BI-style access.

**15. Why Kinesis/Lambda/Glue instead of EC2 or EMR?**
I benchmarked this project against a real data engineer's actual workflow — EC2, S3, Step Functions, PySpark, Airflow — specifically so I could choose differently and explain why. EC2/EMR would make the AWS leg look like "Local stack on bigger hardware," which loses the comparative point of the project. Kinesis/Lambda/Glue represents the managed-primitives, low-ops posture that AWS is actually good at, and that contrasts meaningfully with both Local (self-managed) and Databricks (managed compute + managed orchestration).

**16. Why not Step Functions or Airflow for orchestration on AWS, if a "real" DE workflow uses them?**
At this scale, Glue's own job triggering covered what I needed, and adding Step Functions or Airflow here would have meant building a second orchestration story that duplicates what Databricks Workflows already demonstrates on the other leg of the comparison. It's a sequencing/scope decision specific to this AWS leg, not a claim that Step Functions/Airflow are wrong tools in general — I'd use them on a real job if orchestration complexity justified it.

**17. What's Athena's role, and why doesn't it do any transformation?**
Athena is query/validation only — it never moves or transforms pipeline data. That separation (pipeline vs. "how I check the pipeline's output") is intentionally identical across all three stacks: DuckDB plays the same role locally, notebook SQL plays the same role in Databricks. Keeping that boundary clean is part of what makes the comparison fair.

**18. Why Redshift Serverless instead of a provisioned cluster?**
Cost. A provisioned cluster bills for uptime regardless of usage; this project has bursty, not continuous, usage. Redshift Serverless (namespace `fitness-namespace`, workgroup `fitness-workgroup`, 8 RPU) lets me pay only when I'm actually running queries, and I pause it immediately after a session — that's a deliberate operational discipline, not an afterthought.

**19. Why the Data API instead of a normal JDBC connection to Redshift?**
The Data API doesn't require holding open a live database connection and credentials in application code — it fits the same "only active when actively used" posture as Serverless itself. A persistent JDBC connection is the right call for an always-on application; for a project run in bursts, it's unnecessary surface area.

**20. Explain your star schema and why SCD Type 2 only on `dim_user`.**
Facts (e.g., `fact_fatigue_recovery`) reference dimensions including `dim_user`, `dim_gym`, `dim_workout_catalog`. SCD Type 2 specifically on `dim_user` because user attributes like `job_type`, `fitness_goal`, or `medical_history` genuinely change over time, and a Gold fact needs to reflect the user's state *at the time of that fact*, not their current state — that's the textbook reason SCD Type 2 exists. Other dimensions are either static (`gym_master`) or change rarely enough that overwrite (Type 1) is fine — applying SCD2 everywhere would be complexity with no payoff.

**21. Tell me about a real bug you hit on the AWS side.**
`fact_fatigue_recovery`'s schema was initially built from assumptions about what the Gold table contained. Querying the actual Gold table through Athena revealed the real columns differed from what I'd assumed, so I corrected the fact table schema to match reality instead of patching around the mismatch. That's the "verify against real output" discipline applied to schema design specifically — I didn't trust my own mental model of a table I hadn't actually inspected.

**22. Tell me about your IAM setup and why it matters.**
`retail-pipeline-dev` (carried forward from a prior retail pipeline project, inline policy `RetailPipelineAccess`) deliberately lacks `iam:CreateRole`. The one role this project needed that required elevated privilege, `RedshiftS3ReadRole`, was created via the console using a separate, higher-privileged account — not by giving the working identity the ability to create roles for itself. That's least privilege in practice: if `retail-pipeline-dev`'s credentials were ever compromised, the blast radius is "can move data," never "can mint new permissions." I'd rather explain a slightly less convenient setup process than weaken that boundary.

**23. Why defer dbt on Redshift instead of building it earlier?**
Sequencing, not skipping. Stabilizing the Databricks Bronze→Gold→Workflows track first means when I build dbt on Redshift, I bring lessons already learned twice (Local logic, Databricks orchestration patterns) instead of building it cold and reworking it later. It's logged explicitly as deferred-not-skipped so it doesn't read as an abandoned phase.

---

## E. Databricks stack

**24. Walk me through the Databricks pipeline end to end.**
A live producer notebook generates streaming events → Auto Loader (`cloudFiles`, `trigger(availableNow=True)`) ingests into five Bronze Delta tables under Unity Catalog (`fitness_streaming.bronze.*`) → five Bronze→Silver notebooks (MERGE INTO for `user_profile`, append-style for the other four) write to `fitness_streaming.silver.*` with quarantine for bad rows → four Silver→Gold notebooks fully overwrite `fitness_streaming.gold.*` → all 11 steps are orchestrated as one Databricks Job (`fitness_streaming_db5_pipeline`, Job ID `186405698110`).

**25. Why Unity Catalog with one catalog and four schemas, instead of one catalog per layer?**
The three-part namespace (`catalog.schema.table`) already encodes the medallion layering directly — `fitness_streaming.silver.wearable_event` is self-describing. Splitting into separate catalogs per layer would fragment the governance/sharing boundary Unity Catalog is built around, for no real benefit at this project's size.

**26. Why `trigger(availableNow=True)` instead of continuous streaming?**
This is a deliberate Lambda Architecture boundary: I don't need sub-second ingestion, I need reliable, idempotent, incremental batch ingestion of whatever's landed since the last run. `availableNow=True` gives Auto Loader's exactly-once file tracking and schema evolution without paying for an always-on streaming cluster between test sessions — same cost-discipline logic as pausing Redshift on the AWS side. It's a position I can defend, not a missing feature.

**27. Why MERGE INTO for `user_profile` specifically but append-only for the other four Silver tables?**
`user_profile` rows have a real upsert use case — a user's profile can be updated, and I need the latest state per user, keyed on `user_id`. The other four (`wearable_event`, `workout_log`, `sleep_log`, `nutrition_snapshot`) are event-style records — each row is a new, immutable occurrence, so append is the correct semantic; merging would be solving a problem that doesn't exist for pure events.

**28. Walk me through your Databricks Workflows DAG design (DB-5).**
11 tasks: producer → ingestion → 5 parallel Silver tasks → a barrier → 4 parallel Gold tasks. During design I explicitly confirmed (not assumed) that no Gold task depends on another Gold task — each only reads specific Silver tables. Given that, I chose a coarse barrier — every Gold task depends on *all five* Silver tasks — over wiring each Gold task to only the exact Silver tables it reads.

**29. Why the coarse barrier instead of fine-grained per-table dependencies, if you already knew the real dependencies?**
Wall-clock cost vs. maintainability. Fine-grained dependencies are technically more parallel, but they create a silent staleness risk: if a Gold notebook's Silver reads change later (say `gold_workout_consistency` starts also reading `sleep_log`), someone has to remember to update its dependency edges in the UI, or it'll run against incomplete inputs with no error. The coarse "wait for all Silver" barrier can't go silently wrong later — it costs a little time, buys correctness that doesn't need maintenance.

**30. Explain your retry policy and why it's not uniform.**
`producer`/`ingestion` get `retries=2` — those are typically transient infra issues (package install, cluster init) that may genuinely succeed on a second try. All 5 Silver and all 4 Gold tasks get `retries=0` — they fail on deterministic asserts/schema checks, and a deterministic failure produces the identical failure on retry every time. Retrying it doesn't fix anything, it only delays the alert that something's actually broken — which is strictly worse during active development.

**31. Tell me about the `gold_community_analytics` bug — what was wrong and how did you fix it?**
35 users had wearable/sleep/workout activity but no matching `user_profile` row — an early-arriving fact / late-arriving dimension situation, caused by the test producer intentionally emitting `user_profile` events at only ~5% frequency relative to activity events over a finite batch. The original cohort join silently surfaced nulls for those users into the Gold table. I fixed it by applying `.fillna({"job_type": "Unprofiled", "fitness_goal": "Unprofiled"})` to all three joined sources before grouping — that's a root-cause fix, not a workaround, because it keeps the 35 users' real activity data in the Gold table and makes the *reason* visible (an explicit "Unprofiled" cohort) instead of either silently dropping them or letting unexplained nulls leak into a published table. Verified against real output: row 4 of the rerun Gold table shows `Unprofiled/Unprofiled` with `user_count_wearable = 35` — matching the originally flagged count exactly.

**32. Was that the only data gap in that table — would you call it fully fixed?**
No, and that's deliberate. There's a separate, still-open issue: the `mixed`/`endurance` cohort shows `avg_sleep_hours = null`. That's not the same bug — there are simply no `sleep_log` rows for that cohort at this test data volume, so the left join to the sleep aggregation correctly has nothing to match. I keep these two as explicitly different failure classes in my decision log: missing dimension match (fixed) vs. missing source rows entirely (expected sparsity at small test volume, revisit if data volume grows) — conflating them would mean either over-fixing a non-bug or under-explaining a real one.

**33. Tell me about a bug you caught before it became a real production problem.**
`live_producer.py` had two sequential `run_producer(...)` calls written for interactive notebook use — fine when run cell-by-cell, but a Job task runs the entire notebook top to bottom on every "Run Now." Left as-is, every single scheduled or manual Job run would have silently fired both calls, roughly doubling the data generated and ingested per run with zero error or warning. I caught and removed the second call before the very first Job run — applying "verify before running" prospectively, not just after something already broke.

**34. What's a non-obvious gotcha you hit in the Databricks UI itself, not in code?**
The Workflows UI's "Depends on" field defaults to chaining a new task onto whatever task was created immediately before it, unless you explicitly correct it. I caught this on `gold_community_analytics`, which initially saved with `gold_workout_consistency` as its dependency — a Gold→Gold chain with no real data reason. It would have run successfully and looked fine on the DAG canvas, just silently serializing two independent tasks. I re-checked all 4 Gold tasks' dependencies individually afterward rather than trusting the visual — the same "verify, don't trust by reading" discipline applied to configuration, not just code.

**35. What's a concrete advantage Databricks gave you "for free" compared to AWS?**
Unity Catalog's automatic lineage tracking. After the first full Job run, the run details panel showed "11 upstream tables, 13 downstream tables" — full Bronze-to-Gold lineage, with zero extra code written for it. On AWS, getting equivalent visibility means manually reconstructing relationships from Glue job definitions and S3 paths, or paying for a separate catalog/lineage product. That's one of the cleanest, most concrete contrasts in the whole project.

**36. Why is the Job's schedule still set to "None"?**
Deliberately. I want confidence from several manual "Run Now" executions before committing to an unattended cron schedule that could otherwise run a flawed pipeline repeatedly with no one watching. It's a sequencing choice, not an unfinished task — recorded explicitly so it isn't mistaken for an oversight.

**37. What's left on the Databricks track?**
DB-6 through DB-8: writing up the orchestration story (why Workflows over Airflow/Step Functions, using the lineage and retry-by-failure-class material from DB-5 as evidence) and the formal AWS-vs-Databricks comparison documentation. Plus wiring up the job-level failure notification email, and eventually turning the schedule on once I trust multiple runs.

---

## F. Cross-stack comparison

**38. If someone asks "which of the three stacks is best," what's your answer?**
None of them universally — each wins on a different axis, and that's the actual point of building all three. Local: zero cost, full control, but you own every operational concern yourself (I had to build `StreamBus` because the "real" tool didn't run on my machine). AWS: managed primitives assembled yourself — you get scalability and pay-as-you-go, but you own the orchestration/lineage/cataloging story (Athena query-only, lineage reconstructed manually). Databricks: the most batteries-included for this specific pipeline shape — Auto Loader, Unity Catalog lineage, Workflows orchestration — at the cost of being more opinionated about how you structure things (Delta Lake, catalog-first design).

**39. What's the clearest concrete difference between AWS and Databricks you found?**
Lineage tracking, hands down. Unity Catalog gave me full pipeline lineage with zero extra code the moment the first Job ran. The AWS equivalent isn't built in — it's either manual reconstruction from Glue job configs and S3 paths, or a separate paid tool. Same underlying need (know what feeds what), wildly different cost to satisfy it.

**40. Did building it three times actually change any decisions, or did you just port the same logic mechanically?**
It changed real decisions. The Silver-vs-Gold write pattern (MERGE INTO vs. full overwrite) is something I reasoned through once and then had to re-verify made sense in each engine's terms — Delta Lake's MERGE INTO syntax, Glue's upsert pattern, and the local Parquet-rewrite equivalent are mechanically different even though the underlying decision is identical. And some things genuinely don't translate cleanly — Auto Loader's `availableNow=True` has no exact AWS or local equivalent; the closest AWS analog is a scheduled Glue job trigger, which isn't the same mechanism even though it serves a similar purpose.

**41. How do you keep "the logic" actually identical across three different engines, in practice?**
Discipline, not tooling: whenever I port a transformation, I open the existing Local or AWS version first and translate it, rather than redesigning it fresh for the new platform. That's why I can say with confidence that the fatigue-scoring formula and the quarantine rules are the same business logic everywhere — I didn't reinvent them three times, I moved one definition through three execution engines.

---

## G. Failure & debugging scenarios

**42. Tell me about the worst bug you found, and what would have happened if you hadn't caught it.**
The duplicate `run_producer()` call in the Databricks producer notebook (see Q33). If it had shipped, every single Workflows Job run — manual or eventually scheduled — would have silently generated and ingested roughly double the intended data volume, forever, with no error to flag it. On a real production system that's a cost and data-correctness incident that could run for weeks before anyone noticed, because nothing fails loudly — it just quietly gets bigger.

**43. What's your process when something fails partway through a pipeline run?**
First, which failure class is it — transient infra or deterministic logic? That's literally encoded in my retry policy: producer/ingestion get 2 retries because infra hiccups are plausible; Silver/Gold tasks get 0 retries because they fail on asserts/schema checks that won't change on a retry. Then I go to real output, not logs alone — row counts, the actual assert that fired, the actual schema — because every real bug in this project (wrong column name, missing producer table, Gold grain mismatch, the Unprofiled cohort issue, the UI dependency chaining) was only found by inspecting real output, never by re-reading code.

**44. How would you debug a Gold table that suddenly shows unexpected nulls?**
Exactly what happened with `gold_community_analytics`: first, confirm it's actually a join producing unmatched rows versus genuinely absent source data — those need different fixes. Unmatched dimension rows (my Unprofiled case) need an explicit fallback that preserves the data and labels the gap. Genuinely absent source rows (my `mixed`/`endurance` `avg_sleep_hours` case) need you to either accept the null as an honest representation of "we don't have that yet," or document why, not force a value in.

**45. What would you do differently if this pipeline had to run in actual production with real users?**
Turn the Databricks Job schedule on, but only after wiring the failure notification email I haven't finished yet — running unattended without alerting on failure is the exact opposite of the "fail fast and loudly" principle the retry policy is built on. I'd also revisit the coarse Gold barrier under real load — it's the right call at this volume, but if Silver tasks started taking meaningfully different amounts of time, the wall-clock cost of waiting for the slowest one might justify moving to fine-grained dependencies despite the maintenance risk.

**46. If a Silver MERGE INTO job fails halfway through, what happens to the table?**
Delta Lake's MERGE INTO is transactional — a failed merge doesn't leave the table in a half-written state; it's all-or-nothing per transaction. That's actually part of why MERGE INTO was the right choice for a layer where idempotency matters: a retried (or in my case, deliberately not-retried) failed Silver job can't corrupt the table, it just doesn't apply, and the deterministic-failure-gets-zero-retries policy means I find out immediately rather than the job quietly retrying against a table I'd want to inspect first.

---

## H. Scaling scenarios

**47. This runs on toy data volumes. What breaks first if this had to handle 100x the data?**
Locally: `StreamBus` first, since it's a hand-rolled minimal reproduction, not a real broker — no consumer groups, no replay, no backpressure handling. On AWS: 1 Kinesis shard caps throughput; that's the first lever (more shards, or moving to a Kinesis-on-demand mode). On Databricks: Auto Loader and the Job itself scale reasonably well on Serverless, but `availableNow=True` batch-trigger ingestion would need revisiting toward a continuously-running trigger if true near-real-time latency became a requirement rather than a "good enough" batch cadence.

**48. How would Gold's full-overwrite pattern hold up at much larger scale?**
It would get expensive — recomputing an entire aggregate table from scratch every run doesn't scale linearly forever. The honest next step at real scale is incremental aggregation (e.g., maintaining running aggregates with periodic full reconciliation, or partitioning Gold by time window so only the affected window gets recomputed) rather than full overwrite of the whole table every time. I'd frame full overwrite as the right choice *for this data volume and update frequency*, not as the permanent answer regardless of scale.

**49. If `gold_community_analytics`'s "Unprofiled" cohort kept growing as a percentage of total users, what would that tell you, and what would you do?**
It would mean the `user_profile` producer's ~5% emission frequency is genuinely too low relative to real user onboarding rates, not just a quirk of test data — at production scale, a persistently large Unprofiled cohort is a real data-quality signal, possibly indicating a broken or lagging profile-sync process upstream. I'd add monitoring on the Unprofiled percentage itself as a Gold-layer data quality metric, not just accept it as expected behavior indefinitely.

**50. How would your IAM/security approach need to change at real production scale with multiple engineers?**
The single `retail-pipeline-dev` identity model works for one person; at team scale I'd move to per-engineer or per-service roles with the same least-privilege principle (no engineer's day-to-day role gets `iam:CreateRole`), centralize role creation through a smaller, audited set of admin identities (the same pattern I already use for `RedshiftS3ReadRole`), and add CloudTrail-based auditing on role/permission changes specifically, since that's the highest-blast-radius action class.

**51. If you had to add a fourth "stack" to this comparison, what would make a good candidate, and why?**
Something that forces a genuinely different trade-off than the existing three — e.g., a pure event-streaming-first platform (Kafka + ksqlDB/Flink) would push hardest on the "true streaming vs. batch-triggered" boundary I currently handle with `availableNow=True`, since none of my current three stacks have a continuously-running stream processor. That would be the most informative addition, rather than another managed-warehouse variant that would mostly repeat lessons I already have from Redshift/Databricks.

---

## I. Behavioral / engineering judgment

**52. Tell me about a time you had to choose between a "more correct" and a "simpler" solution.**
The Gold task dependency design in DB-5 (Q29): fine-grained per-table dependencies are technically more correct/parallel, but I chose the coarser, simpler "wait for all Silver" barrier because it can't silently go stale later. I'd rather defend a slightly less optimal but more maintainable design than an optimal one that quietly breaks if someone changes a notebook's reads six months from now and forgets to update a dependency edge.

**53. Describe a moment you almost shipped something wrong, and how you caught it.**
The duplicate producer call (Q33/Q42) — it would have passed every "looks correct" check, run successfully, and only ever shown up as a slowly inflating data/cost number with no error. I caught it specifically because I have a standing rule to verify real run behavior, not notebook logic in isolation, before the first execution of anything new — and a Job runs the whole notebook top-to-bottom, which interactive cell-by-cell testing doesn't surface.

**54. How do you decide when something is "done enough" to move on versus needing more work?**
Done means it ran against real data and the output matches what I expected, with the specific numbers checked, not just "no error." Anything short of that — even if I'm fairly confident in the logic — stays open. That's also why I keep an explicit "open items" list (the `sleep_log` sparsity gap, the unwired failure notification email, the unset schedule) instead of letting unresolved-but-low-priority things quietly disappear from view.
