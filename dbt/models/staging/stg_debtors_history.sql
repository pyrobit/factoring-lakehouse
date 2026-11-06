select
    debtor_id,
    debtor_name,
    tax_id,
    industry,
    country,
    credit_limit,
    risk_rating,
    valid_from,
    coalesce(valid_to, '9999-12-31'::timestamp_ntz) as valid_to,
    is_current,
    {{ dbt_utils_surrogate_key(['debtor_id', 'valid_from']) }} as debtor_version_key
from {{ source('raw', 'debtors_history') }}
