-- =============================================================================================
-- 03 - Streams + Tasks: incremental CDC merge of Snowpipe payments into CORE, every 5 minutes,
-- and only when the stream actually has data (no warehouse cost when idle).
-- =============================================================================================
use role loader;
use schema factoring.core;

create table if not exists payments like factoring.raw.payments;

create stream if not exists raw_payments_stream
on table factoring.raw.payments_stream_landing
append_only = true;

create or replace task merge_stream_payments
    warehouse = load_wh
    schedule = '5 MINUTE'
    suspend_task_after_num_failures = 3
    user_task_timeout_ms = 600000
    comment = 'Dedupe + merge streamed payment events into CORE.PAYMENTS'
when system$stream_has_data('FACTORING.CORE.RAW_PAYMENTS_STREAM')
as
    merge into factoring.core.payments as t
    using (
        select *
        from factoring.core.raw_payments_stream
        qualify row_number() over (partition by payment_id order by event_ts desc) = 1
    ) as s
        on t.payment_id = s.payment_id
    when not matched then insert (
        payment_id, invoice_id, amount, paid_at, method, event_ts, _ingested_at, _batch_id
    ) values (
        s.payment_id, s.invoice_id, s.amount, s.paid_at, s.method, s.event_ts, current_timestamp(), 'stream'
    );

-- Child task: keep CORE in sync with the daily batch load too (batch wins on conflicts).
create or replace task merge_batch_payments
    warehouse = load_wh
    after merge_stream_payments
as
    merge into factoring.core.payments as t
    using factoring.raw.payments as s
        on t.payment_id = s.payment_id
    when matched and t._batch_id = 'stream' then
        update set
            t.amount = s.amount,
            t.paid_at = s.paid_at,
            t.method = s.method,
            t._batch_id = s._batch_id
    when not matched then insert (
        payment_id, invoice_id, amount, paid_at, method, event_ts, _ingested_at, _batch_id
    ) values (
        s.payment_id, s.invoice_id, s.amount, s.paid_at, s.method, s.event_ts, s._ingested_at, s._batch_id
    );

-- Tasks are created suspended: resume children first, then the root.
alter task merge_batch_payments resume;
alter task merge_stream_payments resume;
