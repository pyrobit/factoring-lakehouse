# Architecture Decision Records

We record significant architecture decisions as short, immutable ADRs
([Michael Nygard's format](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions)).
A superseded ADR is never edited: a new ADR supersedes it, and the old one's status links forward.

| ADR | Title | Status |
|---|---|---|
| [0001](0001-record-architecture-decisions.md) | Record architecture decisions | Accepted |
| [0002](0002-databricks-for-engineering-snowflake-for-serving.md) | Databricks for engineering, Snowflake for serving | Accepted |
| [0003](0003-scd2-in-databricks-silver.md) | Build SCD2 debtor history in Databricks silver with one MERGE | Accepted |
| [0004](0004-wheel-jobs-primary-dlt-alternative.md) | Wheel-based jobs as the primary deployment, DLT as the alternative | Accepted |
| [0005](0005-parquet-export-handoff-with-reconciliation.md) | Parquet export hand-off to Snowflake with manifest reconciliation | Accepted |
| [0006](0006-contract-derived-quality-and-quarantine.md) | Contract-derived data quality with quarantine and reject-rate gates | Accepted |
| [0007](0007-airflow-as-cross-platform-orchestrator.md) | Airflow as the cross-platform orchestrator | Accepted |
| [0008](0008-testing-strategy-pytest-and-behave.md) | Testing strategy: pytest for units, behave for business behaviour | Accepted |
| [0009](0009-idempotency-and-late-arriving-data.md) | Idempotent writes and an `updated_at` guard for late-arriving data | Accepted |

New ADR: copy [`template.md`](template.md), take the next number, and open a PR.
