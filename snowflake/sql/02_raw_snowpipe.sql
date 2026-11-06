-- =============================================================================================
-- 02 - RAW tables (1:1 with the Delta exports) and Snowpipe auto-ingest for payments.
-- Batch tables are loaded by the Python loader (COPY + MERGE, inside one transaction).
-- Payments also get a Snowpipe, so near-real-time cash dashboards don't wait for the daily batch.
-- =============================================================================================
use role loader;
use schema factoring.raw;

create table if not exists clients (
    client_id varchar not null,
    client_name varchar,
    industry varchar,
    advance_rate float,
    reserve_rate float,
    fee_rate float,
    credit_limit number(18, 2),
    updated_at timestamp_ntz,
    _ingested_at timestamp_ntz,
    _batch_id varchar,
    constraint pk_clients primary key (client_id)
);

create table if not exists debtors_history (
    debtor_id varchar not null,
    debtor_name varchar,
    tax_id varchar,
    industry varchar,
    country varchar(2),
    credit_limit number(18, 2),
    risk_rating varchar(1),
    updated_at timestamp_ntz,
    _ingested_at timestamp_ntz,
    _batch_id varchar,
    _row_hash varchar(64),
    valid_from timestamp_ntz not null,
    valid_to timestamp_ntz,
    is_current boolean,
    constraint pk_debtors_history primary key (debtor_id, valid_from)
);

create table if not exists invoices (
    invoice_id varchar not null,
    client_id varchar,
    debtor_id varchar,
    invoice_number varchar,
    invoice_date date,
    due_date date,
    amount number(18, 2),
    currency varchar(3),
    status varchar,
    advance_amount number(18, 2),
    funded_at timestamp_ntz,
    updated_at timestamp_ntz,
    _ingested_at timestamp_ntz,
    _batch_id varchar,
    constraint pk_invoices primary key (invoice_id)
)
cluster by (client_id);

create table if not exists payments (
    payment_id varchar not null,
    invoice_id varchar,
    amount number(18, 2),
    paid_at timestamp_ntz,
    method varchar,
    event_ts timestamp_ntz,
    _ingested_at timestamp_ntz,
    _batch_id varchar,
    constraint pk_payments primary key (payment_id)
);

create table if not exists portfolio_snapshot (
    as_of_date date not null,
    invoice_id varchar not null,
    client_id varchar,
    client_name varchar,
    debtor_id varchar,
    debtor_name varchar,
    debtor_risk_rating varchar(1),
    debtor_credit_limit number(18, 2),
    debtor_country varchar(2),
    invoice_date date,
    due_date date,
    status varchar,
    amount number(18, 2),
    collected_amount number(18, 2),
    outstanding_amount number(18, 2),
    advance_amount number(18, 2),
    reserve_amount number(18, 2),
    dilution_amount number(18, 2),
    expected_fee number(18, 2),
    days_past_due integer,
    aging_bucket varchar,
    is_open boolean,
    last_paid_at timestamp_ntz,
    constraint pk_portfolio_snapshot primary key (as_of_date, invoice_id)
)
cluster by (as_of_date);

create table if not exists debtor_features (
    as_of_date date not null,
    debtor_id varchar not null,
    risk_rating varchar(1),
    credit_limit number(18, 2),
    open_exposure number(18, 2),
    credit_utilization float,
    pct_exposure_30plus float,
    max_days_past_due integer,
    open_clients integer,
    invoices_90d integer,
    billed_amount_90d number(18, 2),
    dilution_rate_90d float,
    disputes_90d integer,
    payments_90d integer,
    avg_days_to_pay_90d float,
    avg_days_late_90d float,
    pct_paid_late_90d float,
    constraint pk_debtor_features primary key (as_of_date, debtor_id)
);

-- Snowpipe: payment events written by the lakehouse streaming export land here within ~1 minute.
create table if not exists payments_stream_landing like payments;

create pipe if not exists payments_pipe
auto_ingest = true
comment = 'S3 event notifications -> SQS -> Snowpipe'
as
copy into payments_stream_landing
from @lakehouse_export_stage/stream/payments/
file_format = (format_name = 'parquet_ff')
match_by_column_name = case_insensitive;

-- `show pipes` returns the notification_channel (SQS ARN) to wire into the S3 bucket notification.
show pipes like 'payments_pipe';
