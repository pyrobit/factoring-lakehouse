# 0005. Parquet export hand-off to Snowflake with manifest reconciliation

- **Status:** Accepted
- **Date:** 2026-11-05

## Context
Snowflake must receive silver and gold daily, and the warehouse must provably match the lake before
analysts see new numbers. Options include Delta Sharing, Iceberg tables, the Spark Snowflake
connector, or file export plus `COPY INTO`.

## Decision
Databricks writes **Parquet** to `exports/<table>/as_of_date=D/` plus `_manifest/as_of_date=D/manifest.json`
(row count and amount checksum per table). Snowflake loads it through a **storage integration** (IAM
role, no keys): `COPY INTO` a temporary landing table, then `MERGE` (silver) or `DELETE+INSERT`
of the partition (gold), all in one transaction. The Airflow gate reconciles Snowflake against the
manifest and **short-circuits dbt** on any mismatch.

## Alternatives considered
| Option | Pros | Cons |
|---|---|---|
| Spark Snowflake connector writes | One step | Couples Spark jobs to warehouse availability; harder to make idempotent |
| Delta Sharing / Iceberg external tables | No copy | Query performance and governance trade-offs; maturity varies by account |
| **Parquet + COPY + manifest (chosen)** | Decoupled, idempotent, cheap, auditable files | A copy of the data; the manifest must be maintained |

## Consequences
- Reloading a date is safe: folder-scoped COPY, plus MERGE / partition replace.
- The manifest is reusable by any consumer for completeness checks.
- Revisit per ADR-0002 if zero-copy interoperability becomes viable.
