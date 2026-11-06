-- Each client's debtor shares must add up to 100% of its open book (tolerance for rounding).
select
    as_of_date,
    client_id,
    sum(concentration_share) as total_share
from {{ ref('fct_debtor_concentration') }}
where client_exposure > 0
group by as_of_date, client_id
having abs(sum(concentration_share) - 1) > 0.0001
