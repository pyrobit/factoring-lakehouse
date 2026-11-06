# Architecture: Invoice Factoring Lakehouse

> Status: implemented (local demo, tests and deploy-ready configs) · Owner: data platform · Last updated: 2026-11-06

This document covers what the system does, who it serves, how data moves through it, and why it is
built this way. The decisions behind it are in the [Architecture Decision Records](adr/README.md).

---

## 1. Business context

A **factoring company** buys the unpaid invoices of its **clients** (small and mid-size sellers) that
are owed by **debtors** (their customers). It advances most of the face value right away (the
*advance rate*, e.g. 85%), holds back the rest as a *reserve*, collects from the debtor, then
releases the reserve minus its *fee*.

The business runs on a few questions that need reliable, timely data:

| Question | Metric | Consumer |
|---|---|---|
| How much money is out, and how late is it? | Aging buckets, days past due, DSO | Risk / finance (BI) |
| Is a client over-exposed to one buyer? | Debtor concentration share (> 25% is a breach) | Credit committee |
| How much invoice value is lost? | Dilution rate (short pays, cancellations) | Underwriting |
| Will this debtor pay late? | Point-in-time debtor features | Data science (ML) |
| Can we prove what we knew on a given date? | SCD2 history, time travel, quarantine audit | Compliance |

---

## 2. System context (C4 level 1)

```mermaid
flowchart LR
    classDef person fill:#08427b,color:#fff,stroke:#052e56
    classDef system fill:#1168bd,color:#fff,stroke:#0b4884
    classDef external fill:#999,color:#fff,stroke:#6b6b6b

    analyst([Risk & finance analysts]):::person
    ds([Data scientists]):::person
    officer([Credit officers / compliance]):::person
    oncall([Data on-call engineer]):::person

    subgraph platform[Factoring Lakehouse Platform]
        direction TB
        lake[Databricks Lakehouse<br/>Spark + Delta Lake<br/>bronze · silver · gold]:::system
        wh[Snowflake Warehouse<br/>RAW · CORE · MARTS · ML<br/>dbt semantic layer]:::system
        orch[Airflow<br/>daily orchestration]:::system
    end

    pg[(Operational DB<br/>Postgres)]:::external
    mongo[(Payments service<br/>MongoDB events)]:::external
    partner[Partner / client files<br/>SFTP · API]:::external
    s3[(Object storage<br/>S3 · ADLS · GCS)]:::external
    bi[BI tool<br/>dashboards]:::external
    ml[ML platform<br/>MLflow / training]:::external
    alert[Slack / PagerDuty]:::external

    pg -- "incremental CDC<br/>(updated_at watermark)" --> s3
    mongo -- "payment events<br/>(event_ts watermark)" --> s3
    partner -- "ledger files" --> s3
    s3 -- "Auto Loader" --> lake
    lake -- "Parquet export + manifest" --> s3
    s3 -- "COPY INTO / Snowpipe<br/>(storage integration)" --> wh
    orch -. "triggers / gates" .-> lake
    orch -. "load · reconcile · dbt" .-> wh
    orch -. "failure alerts" .-> alert
    wh --> bi
    analyst --> bi
    officer -- "SCD2 history, audit" --> wh
    ds -- "debtor_features" --> lake
    ds --> ml
    lake -- "feature tables" --> ml
    alert --> oncall
```

### Containers (C4 level 2)

```mermaid
flowchart TB
    subgraph sources[Sources]
        PG[(Postgres<br/>clients · debtors · invoices)]
        MG[(MongoDB<br/>payment_events)]
    end

    subgraph ingest[Extraction - Python on Airflow workers]
        PGX[postgres_incremental.py<br/>server-side cursor, lookback overlap]
        MGX[mongo_events.py<br/>contract validation, _rejected file]
        WM[(watermarks.json /<br/>control table)]
    end

    subgraph landing[Landing zone - object storage]
        L[("landing zone<br/>batch / entity / *.jsonl")]
    end

    subgraph dbx[Databricks - Asset Bundle job / DLT]
        B[bronze.py<br/>explicit schema + lineage cols]
        Q[quality.py<br/>contract-derived rules]
        S[silver.py<br/>dedupe · MERGE · SCD2]
        G[gold.py<br/>snapshot · features]
        QT[(quarantine/*<br/>ops/dq_metrics)]
        X[pipeline.export<br/>Parquet + manifest.json]
    end

    subgraph sf[Snowflake]
        RAW[(RAW<br/>COPY + MERGE)]
        CORE[(CORE<br/>streams + tasks)]
        DT[(ML.debtor_exposure_live<br/>dynamic table)]
        DBT[dbt: staging → marts]
        GOV[Governance<br/>RBAC · masking · row access]
    end

    PG --> PGX --> L
    MG --> MGX --> L
    PGX <--> WM
    MGX <--> WM
    L --> B --> Q
    Q -- valid --> S --> G --> X
    Q -- rejected --> QT
    X --> RAW
    RAW --> CORE --> DT
    RAW --> DBT
    GOV -. policies .- DBT
```

---

## 3. Daily run: sequence

The happy path for logical date `D`. Each arrow is idempotent, so a retry or backfill of `D`
converges to the same state.

```mermaid
sequenceDiagram
    autonumber
    participant AF as Airflow<br/>factoring_lakehouse_daily
    participant PG as Postgres
    participant MG as MongoDB
    participant LZ as Landing zone (S3)
    participant DBX as Databricks job
    participant DL as Delta Lake
    participant EX as Export (S3)
    participant SF as Snowflake
    participant DBT as dbt
    participant AL as Slack / PagerDuty

    AF->>PG: SELECT ... WHERE updated_at > watermark - 10min (server-side cursor)
    PG-->>AF: changed rows
    AF->>LZ: write D/<entity>/part-00000.jsonl (atomic rename)
    AF->>AF: advance watermark (only after the file is durable)
    AF->>MG: find({event_ts > watermark, type: payment.received})
    MG-->>AF: payment events
    AF->>LZ: write D/payments/*.jsonl (+ _rejected/ for contract violations)

    AF->>DBX: DatabricksRunNowOperator(batch_id=D, as_of=D) [deferrable]
    activate DBX
    DBX->>LZ: read D/* with explicit contract schema
    DBX->>DL: bronze: replaceWhere _batch_id = D
    loop clients → debtors → invoices → payments
        DBX->>DBX: latest_per_key (dedupe replays)
        DBX->>DBX: apply contract rules + FK checks against silver
        DBX->>DL: quarantine rows with _dq_failures, write dq_metrics
        alt reject rate > threshold
            DBX-->>AF: DataQualityError (task fails)
            AF->>AL: on_failure_callback
        else within threshold
            DBX->>DL: MERGE into silver (SCD2 for debtors, updated_at guard for invoices)
        end
    end
    DBX->>DL: gold: portfolio_snapshot + debtor_features (overwrite partition as_of_date = D)
    DBX->>EX: Parquet per table + _manifest/manifest.json (rows, amount checksum)
    DBX->>DL: OPTIMIZE / ZORDER / VACUUM
    deactivate DBX
    DBX-->>AF: run succeeded

    AF->>SF: snowflake-load --as-of D
    activate SF
    SF->>EX: COPY INTO RAW.<t>_LANDING FROM @stage/<t>/as_of_date=D/
    SF->>SF: BEGIN · MERGE (silver) / DELETE+INSERT (gold) · COMMIT
    deactivate SF
    AF->>SF: reconcile: COUNT(*), SUM(amount) per table
    AF->>EX: read manifest.json
    alt any mismatch
        AF--xDBT: ShortCircuit: skip dbt (marts keep yesterday's data)
        AF->>AL: alert reconciliation failure
    else all tables match
        AF->>DBT: dbt source freshness && dbt build --target prod
        DBT->>SF: incremental delete+insert marts, run tests (store_failures)
        DBT-->>AF: success
        AF->>AF: emit Dataset snowflake://factoring/marts
    end
```

### Near-real-time payment path (parallel to the batch)

```mermaid
sequenceDiagram
    autonumber
    participant EX as Export (S3) /stream/payments
    participant SP as Snowpipe (auto-ingest)
    participant ST as Stream raw_payments_stream
    participant TK as Task merge_stream_payments (5 min)
    participant CORE as CORE.PAYMENTS
    participant DT as Dynamic table debtor_exposure_live

    EX-)SP: S3 event notification (SQS)
    SP->>SP: COPY INTO RAW.PAYMENTS_STREAM_LANDING
    SP-)ST: new rows tracked (append-only)
    TK->>ST: WHEN system$stream_has_data
    TK->>CORE: MERGE deduplicated events (QUALIFY row_number)
    Note over TK,CORE: child task reconciles with the batch load (batch wins)
    DT->>CORE: incremental refresh within target_lag = 10 min
```

---

## 4. Data model

```mermaid
erDiagram
    CLIENT ||--o{ INVOICE : sells
    DEBTOR ||--o{ INVOICE : owes
    INVOICE ||--o{ PAYMENT : "is paid by"
    DEBTOR ||--o{ DEBTOR_VERSION : "has history"
    INVOICE ||--o{ PORTFOLIO_SNAPSHOT : "snapshotted daily"
    DEBTOR ||--o{ DEBTOR_FEATURES : "featurised daily"

    CLIENT {
        string client_id PK
        string client_name
        float advance_rate
        float fee_rate
        decimal credit_limit
    }
    DEBTOR_VERSION {
        string debtor_id PK
        timestamp valid_from PK
        timestamp valid_to
        bool is_current
        string risk_rating
        string tax_id "PII - masked"
        string _row_hash
    }
    INVOICE {
        string invoice_id PK
        string client_id FK
        string debtor_id FK
        date due_date
        decimal amount
        string status
        decimal advance_amount
        timestamp updated_at "MERGE guard"
    }
    PAYMENT {
        string payment_id PK
        string invoice_id FK
        decimal amount
        timestamp paid_at
    }
    PORTFOLIO_SNAPSHOT {
        date as_of_date PK
        string invoice_id PK
        decimal outstanding_amount
        int days_past_due
        string aging_bucket
        string debtor_risk_rating "point-in-time"
        decimal dilution_amount
    }
    DEBTOR_FEATURES {
        date as_of_date PK
        string debtor_id PK
        decimal open_exposure
        float credit_utilization
        float avg_days_late_90d
        float pct_paid_late_90d
        float dilution_rate_90d
    }
```

### Layers and load patterns

| Layer | Table | Grain | Write pattern | Idempotency |
|---|---|---|---|---|
| Bronze | `bronze/<entity>` | raw record | append, partitioned by `_batch_id` | `replaceWhere _batch_id = D` |
| Quarantine | `quarantine/<entity>` | rejected record + `_dq_failures[]` | partitioned by `_batch_id` | `replaceWhere` |
| Silver | `clients` | client | Type 1 MERGE, newest `updated_at` wins | MERGE |
| Silver | `debtors` | debtor version | **SCD2** staged-union MERGE, row hash | unchanged hash = no-op |
| Silver | `invoices` | invoice | MERGE guarded by `s.updated_at > t.updated_at` | stale/replayed events skipped |
| Silver | `payments` | payment | insert-only MERGE | duplicates ignored |
| Gold | `portfolio_snapshot` | invoice × as_of_date | dynamic partition overwrite | recompute = same partition |
| Gold | `debtor_features` | debtor × as_of_date | dynamic partition overwrite | point-in-time, no leakage |
| Snowflake RAW | 1:1 with exports | same | MERGE / DELETE+INSERT, one transaction | reconciled vs manifest |
| Snowflake MARTS | aging, concentration, KPIs | as_of × client (× debtor) | dbt incremental delete+insert | rerun replaces dates |

---

## 5. Data quality

```mermaid
flowchart LR
    C[pydantic contract<br/>Invoice / Debtor / ...] -->|spark_schema| R[Explicit read schema<br/>all nullable]
    C -->|derive_rules| RU[Rules<br/>not_null · range · accepted · length · unique · FK]
    R --> D[latest_per_key<br/>dedupe replays]
    D --> A[apply_rules → _dq_failures array]
    RU --> A
    A -->|size = 0| V[valid → MERGE silver]
    A -->|size > 0| QR[quarantine + reasons]
    A --> M[ops/dq_metrics<br/>total · rejected · per-rule counts]
    M --> T{reject rate ><br/>threshold?}
    T -- yes --> F[fail task → alert<br/>quarantine already written]
    T -- no --> V
```

Guarantees, each covered by tests:

1. **Nothing is dropped silently.** Every rejected row is in quarantine with every rule it broke.
2. **Fail loud.** If more than the configured share of a batch is bad, the batch fails *after*
   writing quarantine, so stewards can see why.
3. **Replays are harmless.** Exact duplicates collapse, stale CDC events cannot roll state back, and
   unchanged debtor rows create no SCD2 versions.
4. **Warehouse matches lake.** Row counts and amount checksums are reconciled before dbt publishes.
5. **Marts are tested.** dbt generic and singular tests cover uniqueness, relationships, concentration
   shares summing to 1, outstanding within face value, and one current SCD2 version per debtor.

---

## 6. Deployment view

| Concern | Choice | Where |
|---|---|---|
| Compute (lake) | Databricks job clusters, Photon, spot with fallback, autoscale 1–6 | `databricks/resources/factoring_job.yml` |
| Alternative | Delta Live Tables (serverless) with `apply_changes` | `databricks/dlt/factoring_dlt.py` |
| Packaging | Python wheel with entry points, Databricks Asset Bundle `dev`/`prod` | `pyproject.toml`, `databricks/databricks.yml` |
| Warehouse | Separate load / transform / BI warehouses, resource monitor | `snowflake/sql/00_account_setup.sql`, Terraform |
| Storage access | Storage integration + IAM role (no keys) | `snowflake/sql/01_*`, `infra/terraform/main.tf` |
| Orchestration | Airflow, deferrable Databricks operator, ShortCircuit reconcile gate | `airflow/dags/` |
| Secrets | Airflow secrets backend; Snowflake key-pair auth | DAG docstring, `dbt/profiles.example.yml` |
| CI | ruff, sqlfluff, pytest (Spark + Delta), behave, dbt parse, DAG integrity, terraform validate | `.github/workflows/ci.yml` |

### Multi-cloud notes

The pipeline code depends only on URIs and the Spark/Delta APIs. Moving clouds changes configuration,
not code:

- **AWS:** `s3://`, storage integration `STORAGE_PROVIDER='S3'`, IAM role trust.
- **Azure:** `abfss://`, `STORAGE_PROVIDER='AZURE'` with tenant consent, Databricks on Azure, ADF or Airflow.
- **GCP:** `gs://`, `STORAGE_PROVIDER='GCS'` service account, Dataproc/Databricks on GCP, Cloud Composer (Airflow).

---

## 7. Operational runbook (short)

| Symptom | Likely cause | Action |
|---|---|---|
| `DataQualityError: invoices reject rate …` | Source regression | Query `quarantine/invoices` for the batch and group by `_dq_failures`; fix the source; rerun the DAG for `D` |
| Reconcile gate skipped dbt | Partial COPY, or the export overwritten mid-load | Rerun `snowflake_load` then the gate for `D` (MERGE is idempotent) |
| Concentration report empty | Gold partition missing | Check the `gold` task for `D`; `pipeline --layer gold --as-of D` |
| Snowpipe lag | SQS notification misconfigured | `select system$pipe_status('raw.payments_pipe')` |
| Cost spike | Warehouse not suspending / BI concurrency | Check the resource monitor; `warehouse_load_history` |

---

## 8. Testing strategy

| Level | Tooling | What it proves | Location |
|---|---|---|---|
| Unit | pytest + local Spark/Delta | Contracts, rules, SCD2/upsert merges, aging, features, SQL generation, extractors | `tests/unit/` |
| End-to-end | pytest | Two days through every layer: quarantine matches planted defects, idempotent rerun, manifest | `tests/test_pipeline_e2e.py` |
| Behaviour (BDD) | behave (Gherkin) | Business-readable acceptance criteria: quarantine, SCD2, late data, aging | `tests/features/` |
| Static | ruff, sqlfluff, dbt parse, terraform validate, DAG integrity | Code and config correctness without cloud accounts | CI |
