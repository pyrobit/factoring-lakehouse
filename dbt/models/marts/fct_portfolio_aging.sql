{{
  config(
    materialized='incremental',
    unique_key=['as_of_date', 'client_id', 'aging_bucket'],
    incremental_strategy='delete+insert',
    cluster_by=['as_of_date'],
    post_hook=["{{ apply_client_rap() }}"]
  )
}}

-- Aging of the open book per client and bucket, one row per as-of date. Incremental:
-- only new as-of dates are processed; a re-run of a date replaces it (delete+insert).
select
    as_of_date,
    client_id,
    any_value(client_name) as client_name,
    aging_bucket,
    {{ aging_bucket_order('aging_bucket') }} as aging_bucket_order,
    count(*) as open_invoices,
    sum(outstanding_amount) as outstanding_amount,
    sum(advance_amount) as advanced_amount,
    sum(reserve_amount) as reserve_amount,
    max(days_past_due) as max_days_past_due
from {{ ref('stg_portfolio_snapshot') }}
where
    is_open
    {% if is_incremental() %}
        and as_of_date >= (select coalesce(max(as_of_date), '1900-01-01') from {{ this }})
    {% endif %}
group by as_of_date, client_id, aging_bucket
