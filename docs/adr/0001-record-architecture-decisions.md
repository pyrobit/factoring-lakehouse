# 0001. Record architecture decisions

- **Status:** Accepted
- **Date:** 2026-11-05
- **Deciders:** data platform team

## Context
This platform spans two compute engines (Databricks, Snowflake), an orchestrator, and several clouds.
Consultants rotate between client teams, so the reasoning behind choices must survive staff changes
and be reviewable by client architects.

## Decision
We keep lightweight ADRs in `docs/adr/`, numbered and immutable, reviewed in pull requests like
code. Every change that affects data contracts, storage formats, engine responsibilities, SLAs or
security gets an ADR.

## Consequences
- New engineers can understand *why* in minutes rather than by archaeology.
- Changing direction is explicit: write a superseding ADR.
- Small overhead per decision (one short markdown file).
