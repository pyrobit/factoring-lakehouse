# 0009. Idempotent writes and an `updated_at` guard for late-arriving data

- **Status:** Accepted
- **Date:** 2026-11-06

## Context
Extraction is at-least-once (watermark lookback overlap, retries), Airflow retries tasks, and
backfills rerun past dates. CDC events can arrive out of order: an old "DISPUTED" event may land after
the invoice was paid.

## Decision
- Every write is idempotent: bronze/quarantine use `replaceWhere _batch_id`, silver uses MERGE, gold
  uses dynamic partition overwrite, and Snowflake uses MERGE / DELETE+INSERT inside a transaction.
- Within a batch, `latest_per_key` keeps the newest version per key (NULL keys pass through to be
  quarantined).
- Across batches, the invoice MERGE only updates when `source.updated_at > target.updated_at`; the
  number of skipped stale events is logged as `stale_skipped`.
- Watermarks advance only after the landing file is atomically renamed into place.

## Consequences
- Any task can be retried, and any date backfilled, without manual cleanup.
- Proven by `test_rerun_of_day2_is_idempotent`, `test_stale_late_arriving_updates_are_ignored` and
  `late_arriving_data.feature`.
