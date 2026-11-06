{{
  config(
    materialized='incremental',
    unique_key=['as_of_date', 'client_id'],
    incremental_strategy='delete+insert',
    post_hook=["{{ apply_client_rap() }}"]
  )
}}

-- Daily client scorecard used by account managers and the credit committee.
--   DSO              = open receivables / credit sales in the window * window days
--   dilution_rate    = value lost to short pays / cancellations / credit sales in the window
--   advance vs reserve: cash at risk (advanced) vs cushion held back (reserve)
{% set window = var('dso_window_days') %}

with snapshot as (
    select * from {{ ref('stg_portfolio_snapshot') }}
    {% if is_incremental() %}
        where as_of_date >= (select coalesce(max(as_of_date), '1900-01-01') from {{ this }})
    {% endif %}
),

per_client as (
    select
        as_of_date,
        client_id,
        any_value(client_name) as client_name,
        sum(outstanding_amount) as open_receivables,
        sum(iff(is_open, advance_amount, 0)) as funds_in_use,
        sum(iff(is_open, reserve_amount, 0)) as reserve_held,
        sum(iff(invoice_date > dateadd(day, -{{ window }}, as_of_date), amount, 0)) as credit_sales_window,
        sum(iff(invoice_date > dateadd(day, -{{ window }}, as_of_date), dilution_amount, 0)) as dilution_window,
        sum(iff(days_past_due > 30, outstanding_amount, 0)) as past_due_30_plus,
        sum(iff(invoice_date > dateadd(day, -{{ window }}, as_of_date), expected_fee, 0)) as fees_window,
        count_if(is_open) as open_invoices
    from snapshot
    group by as_of_date, client_id
),

breaches as (
    select
        as_of_date,
        client_id,
        count_if(is_breach) as concentration_breaches
    from {{ ref('fct_debtor_concentration') }}
    group by as_of_date, client_id
)

select
    p.as_of_date,
    p.client_id,
    p.client_name,
    p.open_invoices,
    p.open_receivables,
    p.funds_in_use,
    p.reserve_held,
    p.credit_sales_window,
    p.fees_window,
    round({{ safe_divide('p.open_receivables', 'p.credit_sales_window') }} * {{ window }}, 1) as dso_days,
    round({{ safe_divide('p.dilution_window', 'p.credit_sales_window') }}, 4) as dilution_rate,
    round({{ safe_divide('p.past_due_30_plus', 'p.open_receivables') }}, 4) as pct_past_due_30_plus,
    round({{ safe_divide('p.funds_in_use', 'p.open_receivables') }}, 4) as effective_advance_rate,
    coalesce(b.concentration_breaches, 0) as concentration_breaches
from per_client as p
left join breaches as b
    on p.as_of_date = b.as_of_date and p.client_id = b.client_id
