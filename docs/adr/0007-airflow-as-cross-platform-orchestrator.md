# 0007. Airflow as the cross-platform orchestrator

- **Status:** Accepted
- **Date:** 2026-11-05

## Context
A daily run spans Postgres, MongoDB, Databricks, Snowflake and dbt. Within each platform there are
native schedulers (Databricks Workflows, Snowflake Tasks), but none of them sees the whole chain or
can gate dbt on a cross-platform reconciliation.

## Decision
**Airflow** orchestrates the end-to-end DAG. Platform-native schedulers handle intra-platform work:
the Databricks job runs its own task graph, and Snowflake Tasks run the streaming payment merge.
Key settings: `max_active_runs=1`, `catchup=True` (safe because every task is idempotent per `{{ ds }}`),
a deferrable `DatabricksRunNowOperator`, a `ShortCircuitOperator` reconcile gate, a failure callback
to Slack/PagerDuty, and a Dataset outlet so BI and ML DAGs trigger on fresh marts.

## Alternatives considered
| Option | Pros | Cons |
|---|---|---|
| Databricks Workflows end-to-end | One tool on the lake side | Weaker cross-system sensors and gating; Snowflake/dbt tasks are second-class |
| Snowflake Tasks | Serverless | Cannot drive extraction or Databricks |
| Dagster / Prefect | Asset-centric, modern | Not yet in the client's estate; migration cost |

## Consequences
- One place for SLAs, retries, backfills and lineage across systems.
- Airflow workers stay thin: heavy compute runs on Databricks and Snowflake.
