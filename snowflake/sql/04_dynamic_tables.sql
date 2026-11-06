-- =============================================================================================
-- 04 - Dynamic table: near-real-time open exposure per debtor, combining the daily snapshot with
-- payments streamed in since. Snowflake refreshes it incrementally within the target lag.
-- =============================================================================================
use role transformer;
use schema factoring.ml;

create or replace dynamic table debtor_exposure_live
target_lag = '10 minutes'
warehouse = transform_wh
refresh_mode = auto
comment = 'Open exposure per debtor = last snapshot outstanding - payments received since'
as
with last_snapshot as (
    select *
    from factoring.raw.portfolio_snapshot
    where as_of_date = (select max(latest.as_of_date) from factoring.raw.portfolio_snapshot as latest)
),

payments_since as (
    select
        p.invoice_id,
        sum(p.amount) as paid_since_snapshot
    from factoring.core.payments as p
    inner join last_snapshot as s on p.invoice_id = s.invoice_id
    where p.paid_at >= dateadd(day, 1, s.as_of_date)
    group by p.invoice_id
)

select
    s.debtor_id,
    any_value(s.debtor_name) as debtor_name,
    any_value(s.debtor_risk_rating) as risk_rating,
    max(s.as_of_date) as snapshot_date,
    sum(s.outstanding_amount) as snapshot_exposure,
    sum(greatest(s.outstanding_amount - coalesce(p.paid_since_snapshot, 0), 0)) as live_exposure,
    any_value(s.debtor_credit_limit) as credit_limit,
    div0(
        sum(greatest(s.outstanding_amount - coalesce(p.paid_since_snapshot, 0), 0)),
        any_value(s.debtor_credit_limit)
    ) as live_credit_utilization
from last_snapshot as s
left join payments_since as p on s.invoice_id = p.invoice_id
where s.is_open
group by s.debtor_id;
