select
    invoice_id,
    client_id,
    debtor_id,
    invoice_number,
    invoice_date,
    due_date,
    amount,
    currency,
    status,
    advance_amount,
    funded_at,
    updated_at,
    status in ('OPEN', 'FUNDED', 'DISPUTED') as is_open
from {{ source('raw', 'invoices') }}
