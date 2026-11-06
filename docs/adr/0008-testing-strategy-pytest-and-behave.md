# 0008. Testing strategy: pytest for units, behave for business behaviour

- **Status:** Accepted
- **Date:** 2026-11-06

## Context
Pipelines fail in ways unit tests on pure functions miss: merge semantics, ordering, idempotency,
point-in-time joins. Business stakeholders (risk, credit) must be able to read and agree on the
acceptance criteria. Cloud accounts are not available in CI.

## Decision
- **pytest unit tests** with a session-scoped local Spark + Delta session: contracts, every rule
  kind, SCD2/upsert/insert-only merges, aging boundaries, feature leakage, SQL generation,
  extractors and watermarks.
- **pytest end-to-end**: generated data with *known planted defects* (`manifest.json`) runs through
  two days. Asserts include: quarantine exactly equals the planted defects, SCD2 history, stale
  updates skipped, idempotent rerun, and an export manifest that matches.
- **behave (Gherkin) BDD** features in business language: quarantine with reasons, failing loudly,
  debtor history, late-arriving data, aging buckets, dilution. The steps drive the real pipeline
  code, not mocks.
- **Static checks**: ruff, sqlfluff (Snowflake dialect), `dbt parse`, DAG integrity, terraform validate.
- Timestamps are pinned to UTC in the test harness for portability.

## Consequences
- Tests run in minutes on a laptop or in CI with Java 17; Delta jars are cached.
- Feature files double as living documentation for non-engineers.
