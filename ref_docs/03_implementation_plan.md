# Implementation Plan — Invoice Factoring Lakehouse

Scope: **Idea #1 (core)**, plus **#2's ML feature table** and **#4's data contracts and quarantine**.
Design reference: `02_design_factoring_lakehouse.md`.

> **Status (2026-11-06): all 7 phases implemented.** ADRs live in `docs/adr/` (not `ref_docs/adr/`),
> next to the detailed architecture doc `docs/architecture.md`. Unit tests are in `tests/unit/`,
> end-to-end tests in `tests/test_pipeline_e2e.py`, and BDD scenarios (behave) in `tests/features/`.

Guiding rule: **everything that can run locally must run locally and be tested.** Spark + Delta run
in local mode; Snowflake, Databricks and Airflow code is real and deployable, and is validated by
static checks (compile/parse/DAG import) plus a dry-run mode.

---

## Phase 0 — Repo foundation
| # | Task | Deliverable |
|---|---|---|
| 0.1 | Python package layout, `pyproject.toml` (ruff, pytest config), `.gitignore`, `Makefile` | `make setup / test / lint / demo` |
| 0.2 | Config: `conf/pipeline.yaml` (paths, DQ thresholds, env: `local` / `databricks`) | `config.py` loader |
| 0.3 | Spark session factory: local Delta (`configure_spark_with_delta_pip`) vs. Databricks runtime | `spark_session.py` |

**Done when:** `make setup` builds the venv and `make lint` passes.

## Phase 1 — Synthetic sources and data contracts (idea #4)
| # | Task | Deliverable |
|---|---|---|
| 1.1 | Pydantic contracts for `clients`, `debtors`, `invoices`, `payments` → Spark `StructType` | `contracts.py` |
| 1.2 | Deterministic data generator (seeded) that writes JSONL to the landing zone, with **planted defects**: duplicates, nulls, negative amounts, orphan FKs, late-arriving updates, and debtor attribute changes (to trigger SCD2) | `datagen.py` |
| 1.3 | Source DDL for Postgres (`docker/postgres/init.sql`) and a sample Mongo payment document | docker assets |

**Done when:** `make data` produces two batches (day 1 and day 2) with known defect counts.

## Phase 2 — Extraction layer (Postgres / MongoDB → landing)
| # | Task | Deliverable |
|---|---|---|
| 2.1 | Incremental watermark extractor for Postgres (parameterised SQL, server-side cursor, JSONL out, watermark state) | `ingestion/postgres_incremental.py` |
| 2.2 | MongoDB payment-event extractor (filter on `event_ts > watermark`, BSON → JSON) | `ingestion/mongo_events.py` |
| 2.3 | `docker-compose.yml` with Postgres + Mongo for an optional live extraction demo | compose file |

**Done when:** unit tests cover the query/filter builders and watermark advancement.

## Phase 3 — Databricks medallion pipeline (core)
| # | Task | Deliverable |
|---|---|---|
| 3.1 | **Bronze**: schema-enforced read and lineage columns (`_ingested_at`, `_source_file`, `_batch_id`); batch mode locally, Auto Loader (`cloudFiles`) on Databricks | `bronze.py` |
| 3.2 | **DQ framework**: rule types (not_null, unique, range, accepted_values, referential), quarantine split with a `_dq_failures` array, reject-rate threshold, DQ metrics table | `quality.py` |
| 3.3 | **Silver**: clients Type-1 MERGE; debtors **SCD2** MERGE (row hash); invoices MERGE guarded by `updated_at`; payments dedupe + insert-only MERGE | `silver.py` |
| 3.4 | **Gold**: `portfolio_snapshot` (outstanding, days past due, aging bucket, advance/reserve) and `debtor_features` for ML (idea #2: avg days late, % paid late, open exposure, dilution, 90-day activity; point-in-time on `as_of_date`) | `gold.py` |
| 3.5 | Pipeline CLI: `python -m factoring_lakehouse.pipeline --batch day1 --as-of 2026-10-31` | `pipeline.py` |
| 3.6 | Databricks deployment: Asset Bundle `databricks.yml` (job with bronze → silver → gold tasks, job cluster, schedules) + a **Delta Live Tables** version with `@dlt.expect_or_drop` | `databricks/` |

**Done when:** `make demo` runs day 1 then day 2 end-to-end locally, and tests prove:
idempotent reruns, correct SCD2 history, late-arriving update rejection, quarantine counts that match
the planted defects, and correct aging buckets and features.

## Phase 4 — Snowflake platform
| # | Task | Deliverable |
|---|---|---|
| 4.1 | Account setup: roles (RBAC hierarchy), warehouses (sized and auto-suspended), resource monitor, database/schemas | `snowflake/sql/00_account_setup.sql` |
| 4.2 | Storage integration (S3; Azure/GCS variants commented), file formats, external stage | `01_integration_stage.sql` |
| 4.3 | RAW tables + **Snowpipe** (auto-ingest) | `02_raw_snowpipe.sql` |
| 4.4 | **Streams + Tasks** for incremental merge RAW → CORE | `03_streams_tasks.sql` |
| 4.5 | **Dynamic table** for near-real-time debtor exposure | `04_dynamic_tables.sql` |
| 4.6 | Governance: masking policy on debtor PII, row-access policy per client, tags | `05_governance.sql` |
| 4.7 | Python loader: export gold/silver to Parquet → `COPY INTO` → MERGE; `--dry-run` prints the SQL; **reconciliation** (counts and sums, Delta vs. Snowflake) | `snowflake_io/loader.py` |

**Done when:** sqlfluff (Snowflake dialect) passes and loader unit tests validate the generated SQL
and the reconciliation logic.

## Phase 5 — dbt on Snowflake (BI semantic layer)
| # | Task | Deliverable |
|---|---|---|
| 5.1 | Project, `profiles.example.yml`, sources with freshness checks | `dbt/` |
| 5.2 | Staging models (`stg_*`) | views |
| 5.3 | Marts: `fct_portfolio_aging`, `fct_debtor_concentration`, `fct_client_kpis` (DSO, collected, funded, dilution, advance vs. reserve), `dim_debtor` (from the SCD2 table) | incremental / table models |
| 5.4 | Tests: generic (unique, not_null, relationships, accepted_values) + singular (concentration share sums to 1) | `schema.yml`, `tests/` |

**Done when:** `dbt parse` succeeds in CI.

## Phase 6 — Orchestration (Airflow)
| # | Task | Deliverable |
|---|---|---|
| 6.1 | DAG `factoring_lakehouse_daily`: TaskGroups extract → `DatabricksRunNowOperator` → Snowflake `COPY` (`SQLExecuteQueryOperator`) → reconcile gate → dbt build → publish Dataset | `airflow/dags/` |
| 6.2 | Retries, SLA, `on_failure_callback`, idempotent `{{ ds }}`-driven params, backfill-safe | same |
| 6.3 | DAG integrity test (imports, no cycles, task count), skipped when Airflow isn't installed | `tests/test_dag_integrity.py` |

## Phase 7 — Infra, CI and docs
| # | Task | Deliverable |
|---|---|---|
| 7.1 | Terraform: S3 landing bucket + IAM role, Snowflake warehouse/db/roles/storage integration | `infra/terraform/` |
| 7.2 | GitHub Actions: ruff, pytest (with Java), sqlfluff, `dbt parse` | `.github/workflows/ci.yml` |
| 7.3 | README: architecture diagram (Mermaid), quickstart, design decisions, a skills → code map for interviewers | `README.md` |
| 7.4 | ADRs: Delta vs. Iceberg hand-off, Databricks/Snowflake split, SCD2 approach | `docs/adr/` |

---

## Order and estimates
| Phase | Effort | Depends on |
|---|---|---|
| 0 Foundation | S | — |
| 1 Contracts + datagen | M | 0 |
| 2 Extraction | S | 1 |
| 3 Databricks medallion | **L** (core) | 1 |
| 4 Snowflake | M | 3 (output format) |
| 5 dbt | M | 4 |
| 6 Airflow | S | 3, 4, 5 |
| 7 Infra / CI / docs | M | all |

Critical path: **0 → 1 → 3 → 4 → 5 → 6 → 7**. Phase 2 can run in parallel with phase 3.

## Out of scope (stated in the README as "next steps")
- A real cloud deployment (needs accounts and credentials); the code is deploy-ready, and the demo runs locally.
- Streaming (idea #3) and the Neo4j graph (idea #6).
- MLflow model training: the pipeline stops at a validated feature table.

## Risks
| Risk | Mitigation |
|---|---|
| Delta jars need network access on the first Spark start | Cached in `~/.ivy2`; documented in the README |
| Snowflake/Databricks can't be integration-tested without accounts | Dry-run mode, SQL linting, `dbt parse`, a clear README note |
| Scope creep | Phases 0–3 alone are a complete, demo-able project |
