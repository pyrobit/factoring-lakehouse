# 0002. Databricks for engineering, Snowflake for serving

- **Status:** Accepted
- **Date:** 2026-11-05

## Context
The company needs both heavy data engineering (semi-structured CDC from Postgres and Mongo,
deduplication, SCD2, ML feature computation, cheap historical backfills) and a governed, highly
concurrent SQL layer for finance and risk analysts using BI tools. Data scientists work in Python;
analysts work in SQL.

## Decision
- **Databricks (Spark + Delta Lake)** owns ingestion, cleansing, conformance (bronze/silver) and
  ML-oriented gold tables (`portfolio_snapshot`, `debtor_features`).
- **Snowflake** owns the serving layer: RAW copies of silver/gold, near-real-time CORE via streams
  and tasks, dbt marts for BI, and governance (RBAC, masking, row access).
- Data crosses once per day (plus a streaming payments path), through a reconciled hand-off (ADR-0005).

## Alternatives considered
| Option | Pros | Cons |
|---|---|---|
| Snowflake only (Snowpark) | One platform, simpler governance | Python/ML ecosystem and backfill cost less favourable; less control over Spark-style transforms |
| Databricks only (Databricks SQL) | One copy of data, Unity Catalog | Analysts and the existing BI estate are on Snowflake; would need a migration |
| **Split by strength (chosen)** | Each engine used where it is strongest; teams keep their tools | Two platforms to operate; the hand-off must be reconciled |

## Consequences
- Cost attribution is clear: engineering compute (Databricks jobs) vs. serving (Snowflake warehouses).
- The hand-off is a contract: export layout, manifest and reconciliation must be maintained.
- Revisit when Iceberg/Delta interoperability (e.g. Snowflake reading Iceberg tables written by
  Databricks via Unity Catalog) is mature enough to remove the copy.
