# 0006. Contract-derived data quality with quarantine and reject-rate gates

- **Status:** Accepted
- **Date:** 2026-11-05

## Context
Source systems produce nulls, negative amounts, invalid enums, orphan foreign keys and duplicates.
Dropping bad rows hides problems; failing on any bad row blocks the whole business on one typo.
Rules written separately from schemas drift.

## Decision
- Each entity has a **pydantic contract** (`contracts.py`). From it we derive the explicit Spark read
  schema (all nullable, so bad rows reach the checks) and the rules: not_null, range, accepted
  values, length, unique, and foreign key.
- `quality.split` adds a `_dq_failures` array listing every broken rule. Valid rows continue;
  rejected rows go to `quarantine/<entity>` with reasons; metrics go to `ops/dq_metrics`.
- A per-entity **reject-rate threshold** (`conf/pipeline.yaml`) fails the batch *after* writing
  quarantine, so the evidence is always available.
- The same contracts validate Mongo documents at extraction time and generate DLT expectations.

## Alternatives considered
| Option | Pros | Cons |
|---|---|---|
| Great Expectations / Soda | Rich ecosystem, docs | Extra runtime and config; rules separate from schema |
| DLT expectations only | Native UI | Databricks-only; quarantine needs extra work |
| **Contract-derived rules (chosen)** | One source of truth; testable; portable | We own a small framework |

## Consequences
- Adding a field constraint to the contract automatically adds the rule everywhere.
- Thresholds are tuned per environment (stricter in prod, see `conf/pipeline.databricks.yaml`).
