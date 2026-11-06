# 0003. Build SCD2 debtor history in Databricks silver with one MERGE

- **Status:** Accepted
- **Date:** 2026-11-05

## Context
Credit officers must answer "what was this debtor's risk rating when we funded the invoice?". Gold
snapshots and ML features must use **point-in-time** attributes, otherwise features leak the future.
Debtor rows arrive via CDC, sometimes replayed unchanged and sometimes out of order.

## Decision
`silver/debtors` is SCD Type 2 (`valid_from`, `valid_to`, `is_current`, `_row_hash`), maintained by a
**single atomic Delta MERGE** using the staged-union pattern:
- changed keys appear twice in the source: once to close the current row, once (with a NULL merge
  key) to insert the new version;
- `_row_hash` over the tracked columns makes unchanged replays a no-op;
- a version is only applied if `updated_at > current.valid_from`, so older events cannot reopen history.

dbt snapshots are not used: history already exists upstream, and Snowflake receives it as
`RAW.DEBTORS_HISTORY`.

## Alternatives considered
| Option | Pros | Cons |
|---|---|---|
| Two statements (UPDATE then INSERT) | Simple to read | Not atomic; a failure between them corrupts history |
| dbt snapshot in Snowflake | Familiar to analysts | History would start at Snowflake; gold/ML in Databricks need it first |
| DLT `apply_changes(stored_as_scd_type=2)` | Declarative, handles ordering | Ties the logic to DLT; kept as the DLT variant (ADR-0004) |

## Consequences
- Point-in-time joins (`valid_from <= t < valid_to`) are used in gold and in `ml_debtor_features`.
- Tests: unit (`test_silver_merge.py`), BDD (`debtor_history.feature`), dbt singular test
  (one current version, no overlaps).
