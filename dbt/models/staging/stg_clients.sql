select
    client_id,
    client_name,
    industry,
    advance_rate,
    reserve_rate,
    fee_rate,
    credit_limit,
    updated_at
from {{ source('raw', 'clients') }}
