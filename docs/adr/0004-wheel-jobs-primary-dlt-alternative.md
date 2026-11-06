# 0004. Wheel-based jobs as the primary deployment, DLT as the alternative

- **Status:** Accepted
- **Date:** 2026-11-05

## Context
Client teams differ: some standardise on Delta Live Tables, others want plain Spark code that is
portable (EMR, Dataproc, local) and fully unit-testable in CI without a workspace.

## Decision
The primary deployment is a **Python wheel** (`factoring_lakehouse`) run by a **Databricks Asset
Bundle** job (bronze → silver → gold → export → maintenance tasks on a Photon job cluster). The same
contracts and rules also drive a **DLT pipeline** (`databricks/dlt/factoring_dlt.py`) with
`expect_all_or_drop`, quarantine tables and `apply_changes`.

## Alternatives considered
| Option | Pros | Cons |
|---|---|---|
| DLT only | Managed retries, lineage, expectations UI | Hard to unit test locally; Databricks-only |
| Notebooks | Fast to start | Poor testability, review and reuse |
| **Wheel + bundle, DLT variant (chosen)** | Testable, portable, CI/CD friendly; DLT available | Two deployment paths to keep consistent |

## Consequences
- One source of truth for rules (`contracts.py`) keeps both paths consistent.
- `max_concurrent_runs: 1` on the job, because MERGE targets need a single writer.
