{{ config(materialized='table') }}

-- Type-2 debtor dimension: join facts on debtor_id AND event date between valid_from / valid_to
-- to get the attributes that were true at the time.
select
    debtor_version_key,
    debtor_id,
    debtor_name,
    tax_id,            -- masked by the PII tag policy for non-privileged roles
    industry,
    country,
    credit_limit,
    risk_rating,
    valid_from,
    valid_to,
    is_current
from {{ ref('stg_debtors_history') }}
