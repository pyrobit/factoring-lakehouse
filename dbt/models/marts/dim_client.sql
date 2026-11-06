select
    client_id,
    client_name,
    industry,
    advance_rate,
    reserve_rate,
    fee_rate,
    credit_limit
from {{ ref('stg_clients') }}
