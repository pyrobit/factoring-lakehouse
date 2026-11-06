select
    payment_id,
    invoice_id,
    amount,
    paid_at,
    paid_at::date as paid_date,
    method
from {{ source('raw', 'payments') }}
