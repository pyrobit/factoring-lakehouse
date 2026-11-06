{{
  config(
    materialized='incremental',
    unique_key=['as_of_date', 'debtor_id'],
    incremental_strategy='delete+insert'
  )
}}

-- Point-in-time debtor features computed in Databricks (gold.debtor_features), published in
-- Snowflake for analysts and for training sets joined by (debtor_id, as_of_date).
select
    f.*,
    d.industry as debtor_industry,
    d.country as debtor_country
from {{ source('raw', 'debtor_features') }} as f
left join {{ ref('dim_debtor') }} as d
    on
        f.debtor_id = d.debtor_id
        and f.as_of_date::timestamp_ntz >= d.valid_from
        and f.as_of_date::timestamp_ntz < d.valid_to
{% if is_incremental() %}
    where f.as_of_date >= (select coalesce(max(as_of_date), '1900-01-01') from {{ this }})
{% endif %}
