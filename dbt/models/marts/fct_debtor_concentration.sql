{{
  config(
    materialized='incremental',
    unique_key=['as_of_date', 'client_id', 'debtor_id'],
    incremental_strategy='delete+insert',
    post_hook=["{{ apply_client_rap() }}"]
  )
}}

-- Share of each client's open book owed by each debtor. Above the limit, the factor is exposed
-- to a single buyer's default, so these rows drive the risk team's daily breach report.
with exposure as (
    select
        as_of_date,
        client_id,
        debtor_id,
        any_value(debtor_name) as debtor_name,
        any_value(debtor_risk_rating) as debtor_risk_rating,
        sum(outstanding_amount) as debtor_exposure
    from {{ ref('stg_portfolio_snapshot') }}
    where
        is_open
        {% if is_incremental() %}
            and as_of_date >= (select coalesce(max(as_of_date), '1900-01-01') from {{ this }})
        {% endif %}
    group by as_of_date, client_id, debtor_id
)

select
    as_of_date,
    client_id,
    debtor_id,
    debtor_name,
    debtor_risk_rating,
    debtor_exposure,
    sum(debtor_exposure) over (partition by as_of_date, client_id) as client_exposure,
    {{ safe_divide('debtor_exposure', 'sum(debtor_exposure) over (partition by as_of_date, client_id)') }}
        as concentration_share,
    concentration_share > {{ var('concentration_limit') }} as is_breach
from exposure
