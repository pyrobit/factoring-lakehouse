# Portfolio Project Ideas — Senior Data Engineer (Snowflake + Databricks)

Target role: Senior Data Engineer building scalable Snowflake and Databricks pipelines that deliver
reliable data for analytics and machine learning.
Business domain: **invoice factoring / receivables finance** (a "factor funding" company buys its
clients' unpaid invoices, advances cash against them, and collects from the debtors).

Every idea combines **Snowflake + Databricks (Spark/Delta)** with Airflow, SQL, Python, and a cloud
storage layer (AWS S3 / Azure ADLS / GCP GCS).

---

## 1. Invoice Factoring Lakehouse (end-to-end flagship) — ✅ CHOSEN

A medallion pipeline (bronze → silver → gold).

- **Sources:** invoices, debtors, clients and advances from **Postgres** (incremental, watermark-based
  CDC); payment events from **MongoDB**; files landing in **S3/ADLS**.
- **Databricks:** Auto Loader ingestion, Delta `MERGE` with SCD2 for debtors, data-quality expectations
  (Delta Live Tables variant included), and a quarantine table for bad records.
- **Snowflake:** storage integration + Snowpipe/`COPY INTO`, streams & tasks, then dbt marts for
  portfolio aging, DSO, debtor-concentration risk, and advance vs. reserve.
- **Airflow:** one DAG that runs extract → Databricks → export → Snowflake load → reconciliation →
  dbt build.
- **Why:** it covers nearly the whole skill list in one coherent, runnable repo.

## 2. Debtor Credit-Risk Feature Store for ML

Point-in-time-correct features for a **debtor default / late-payment model**: rolling payment delay,
dilution rate, dispute frequency, and industry payment trends.

- **Databricks:** Spark feature pipelines with no leakage from the future, Feature Store, MLflow handoff.
- **Snowflake:** feature tables served to analysts; dynamic tables for near-real-time scores.
- **Why:** speaks directly to "reliable data for analytics *and* machine learning".

## 3. Real-Time Payment Reconciliation & Fraud Signals

Streaming bank remittances (Kafka/Kinesis format) matched to open invoices with fuzzy matching on
amount, reference and date. Flags duplicate invoices, double-pledged receivables, and payments that
bypass the factor.

- **Databricks:** Structured Streaming with watermarks and stateful deduplication.
- **Snowflake:** Streams + Tasks keep a live exceptions table.
- **Why:** shows streaming + batch skills, and fraud is a real pain point in factoring.

## 4. Multi-Source Client Onboarding & Data Contracts Platform

Each factoring client sends ledgers in a different format (CSV, Excel, ERP API exports, JSON). A
config-driven pipeline with **pydantic data contracts**, schema-evolution handling, and quarantine
tables.

- **Databricks:** generic, metadata-driven ingestion framework.
- **Snowflake:** VARIANT raw layer, masking policies, and RBAC for client/debtor PII.
- **Why:** shows platform/governance maturity and scalable onboarding.

## 5. Portfolio Analytics & Covenant Monitoring Warehouse

A Snowflake data-modeling showcase (Data Vault 2.0 or star schema, built with dbt): borrowing base,
eligibility rules, aging buckets, covenant breaches, daily snapshots, and time travel for audit.

- **Databricks:** heavy historical backfills on a cost-tuned Spark compute layer.
- **Snowflake:** clustering, warehouse sizing, resource monitors, cost dashboards.
- **Why:** SQL modeling depth plus FinOps awareness, which senior interviews probe.

## 6. Migration & Modernization Case Study: Legacy → Lakehouse

A simulated migration of a legacy **Postgres + Cassandra** factoring system (plus a **Neo4j** graph of
debtor/client relationships) to Databricks and Snowflake. Includes dual-run validation, row/hash
reconciliation, and automated cutover checks. The graph is used to find related-party and
concentration risk.

- **Why:** consultants often join client teams mid-migration; this shows the senior judgment that
  work needs.

---

### Recommendation

Build **#1 as the core**, borrowing #2's ML feature table and #4's data contracts + quarantine. The
result is one coherent story: ingest from many sources → reliable Delta + Snowflake pipelines →
data ready for analytics and ML.
