# GovData — Test Strategy

The govdata adapter's specialization of the [shared core](strategy.md). The testable unit is a
**schema** (econ, sec, geo, …). Ledger: [../requirements/govdata.yaml](../requirements/govdata.yaml).

Unlike `file`, govdata requirements are **single-store and engine-agnostic** — there is no matrix.
The challenge is different: the legacy suite hits **live government APIs** and asserts only liveness
(`assertRowCount(..., 100)`), so it is flaky, slow, and blind to correctness.

## Reverse-engineering a schema

Mine four sources per table and cross-check:

1. **Schema YAML** (`govdata/src/main/resources/<schema>/*.yaml`) — table/column comments (meaning),
   `dataLag`/freshness (cadence rules), partition strategy, key/constraint declarations.
2. **Transformers** (`*ResponseTransformer`) — the source→table mapping. These are **pure functions**
   (API response → rows), so they are directly golden-able, exactly like the file adapter's scanners.
3. **DQ constraints** — invariants already asserted; become `constraint` requirements.
4. **Existing tests** — scenarios/fixtures worth preserving (mined, then archived).

## The golden shape for govdata

- **Hermetic via recorded HTTP.** Replace every live-API call with a committed recorded response
  fixture. Live calls become a tiny separate `@Tag("live")` canary, not the backbone.
- **Transformer golden** — recorded response → exact rows (the highest-leverage, purest test).
- **Table golden** — the materialized table value-diffed against expected (not `rowCount > 0`).
- **Idempotence** — run the ETL twice; the second run writes nothing / identical state. Directly
  targets the resume/re-write defect class.
- **Freshness / etag gates** — re-ingest with an unchanged source (same etag/empty-marker) performs
  no rewrite; partial/unpublished periods past the `dataLag` are never surfaced. This is the bug
  locus (the etag-freshness-gate and empty-marker history).
- **Iceberg materialization** — snapshot diffing where tables materialize to Iceberg.

## Status

Pilot = **econ** (GOV-001..005 sketch the shape; complete the schema 1:1). Then sec, geo, and the
rest — one group per schema. sec is the eventual prize (XBRL, 13F, insider) but the most complex;
econ proves the method without fighting XBRL.
