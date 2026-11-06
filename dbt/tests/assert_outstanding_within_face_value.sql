-- An invoice can never owe more than its face value, nor a negative amount.
select
    as_of_date,
    invoice_id,
    amount,
    outstanding_amount
from {{ ref('stg_portfolio_snapshot') }}
where outstanding_amount < 0 or outstanding_amount > amount
