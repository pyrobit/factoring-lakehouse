# Design — Invoice Factoring Lakehouse (Idea #1)

## 1. Business context

A factoring company buys its **clients'** (sellers') invoices issued to **debtors** (buyers).

| Term | Meaning |
|---|---|
| Advance rate | % of the invoice face value paid to the client up front (e.g. 85%) |
| Reserve | the remainder, held back until the debtor pays |
| Fee | the factor's discount fee, charged on collection |
| Aging | days past due of open invoices, grouped into buckets |
| DSO | Days Sales Outstanding: how long debtors take to pay |
| Concentration | share of a client's open portfolio owed by a single debtor (risk if > 25%) |
| Dilution | invoice value lost to credits, disputes, or cancellations |

Data consumers:
- **Risk & finance analysts** → Snowflake marts (aging, concentration, client KPIs) in BI.
- **Data science** → the debtor late-payment feature table (gold, Delta + Snowflake).

## 2. Architecture

```
 Postgres (ops DB)  ──incremental watermark──┐
 MongoDB (payment events) ──ts watermark─────┼──► Landing zone (S3 / ADLS / GCS, JSONL)
 Partner SFTP/API files ─────────────────────┘                │
                                                               ▼
                                   ┌──────── Databricks (Spark + Delta Lake) ────────┐
                                   │ BRONZE  raw + lineage cols (Auto Loader)          │
                                   │ SILVER  contracts, DQ, dedupe, MERGE, SCD2,       │
                                   │         quarantine of bad rows                    │
                                   │ GOLD    portfolio_snapshot, debtor_features (ML)  │
                                   └──────────────┬───────────────────────────────────┘
                                                  │ export Parquet to stage
                                                  ▼
                                   ┌──────────── Snowflake ───────────────────────────┐
                                   │ RAW  (Snowpipe / COPY INTO from external stage)   │
                                   │ STREAMS + TASKS (incremental merge)               │
                                   │ dbt: staging → marts (aging, concentration, KPIs) │
                                   │ Governance: RBAC, masking, resource monitors      │
                                   └──────────────────────────────────────────────────┘
 Orchestration: Airflow DAG (extract → Databricks job → Snowflake load → reconcile → dbt build)
```

### Responsibility split (a deliberate decision)

| Layer | Engine | Why |
|---|---|---|
| Ingestion, cleansing, dedupe, SCD2, ML features | **Databricks** | Heavy semi-structured transforms, Python/ML ecosystem, cheap compute for backfills |
| BI semantic layer, governed access | **Snowflake + dbt** | SQL-first analysts, concurrency, RBAC/masking, zero-copy clones for dev |

## 3. Data model

Source entities: `clients`, `debtors`, `invoices`, `payments`.

| Layer | Table | Grain | Load pattern |
|---|---|---|---|
| Bronze | `bronze/<entity>` | one row per raw record | append + `_ingested_at`, `_source_file`, `_batch_id` |
| Silver | `silver/clients` | client | Type 1 upsert (MERGE) |
| Silver | `silver/debtors` | debtor version | **SCD2** (`valid_from`, `valid_to`, `is_current`, `_row_hash`) |
| Silver | `silver/invoices` | invoice (latest state) | MERGE, only if newer `updated_at` (late-arrival safe) |
| Silver | `silver/payments` | payment | dedupe by `payment_id`, insert-only MERGE |
| Silver | `quarantine/<entity>` | rejected row + `_dq_failures` | append |
| Gold | `gold/portfolio_snapshot` | invoice × as_of_date | overwrite partition `as_of_date` |
| Gold | `gold/debtor_features` | debtor × as_of_date | overwrite partition `as_of_date` |

## 4. Reliability practices

1. **Data contracts** (pydantic → Spark `StructType`): an explicit schema at read time, no inference.
2. **Expectations**: not-null, unique, accepted range/values, referential integrity. Bad rows are
   **quarantined, not dropped**. The batch fails if the reject rate is above a configured threshold.
3. **Idempotency**: every write is a MERGE or a partition overwrite, so reruns are safe.
4. **Late / out-of-order data**: a silver merge only applies when `source.updated_at > target.updated_at`.
5. **Reconciliation**: row counts and amount sums are compared between Delta and Snowflake before dbt runs.
6. **Observability**: structured logs, a DQ metrics table, Airflow SLAs, and a failure callback.
7. **CI**: ruff, pytest (local Spark + Delta), sqlfluff (Snowflake dialect), `dbt parse`.

## 5. Repository map

See the root `README.md`. The detailed, maintained architecture document (C4 context and container
diagrams, sequence diagrams, ER model) is `docs/architecture.md`; decisions are in `docs/adr/`.
