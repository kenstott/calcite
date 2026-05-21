# ADR-001: Year Dimension Semantics

**Status:** Accepted  
**Date:** 2026-05-21

## Context

The govdata platform uses a `year` (and `month`) partition dimension across all Iceberg tables. Different schemas handle time differently:

- Most schemas fetch the year directly from the source API and store it as-is.
- Schemas with `dataLag` (NCVS, SEDS, some econ tables) cap the end year at `current_year - dataLag`, which means the daily worker produces empty year ranges for lagged datasets — those tables silently go stale.
- SEC stores by filing year (calendar year of EDGAR submission); fiscal year is a data column.

The question was whether `year` should represent the ETL processing year or the effective data year, and how that interacts with `dataLag` and the daily/historical worker split.

## Decision

### 1. `year` is the effective data year for all schemas

`year` (and `month`) represent the period the data describes — the year of the survey, measurement, generation, or report. This is the only semantics that makes sense for analytical queries.

This is already true for most schemas. It is now the explicit, enforced contract. No schema may store a partition year that differs from the year the data describes.

### 2. `dataLag` is a per-dataset effective-year offset applied to all boundaries

For a dataset with `dataLag=N`, the effective year range is derived from the run's processing year range by subtracting N from every boundary:

```
effective_start = processing_start - N
effective_end   = current_year - N
```

This applies uniformly to all workers and run modes — it is not specific to the daily worker. Processing year boundaries (`GOVDATA_START_YEAR`, `GOVDATA_INCREMENTAL_START_YEAR`, `current_year`) are ETL-internal concepts. Operators and users work exclusively in effective data years.

Within a single run, each dataset resolves its own effective year range independently. A daily run in 2026 will process CDE tables (dataLag=1) at effective years anchored to 2025, and NCVS tables (dataLag=2) at effective years anchored to 2024 — simultaneously, in the same worker.

This eliminates the daily worker gap where lagged datasets were silently excluded from daily processing.

### 3. Processing time is Iceberg metadata only

An `_ingest_ts TIMESTAMP` column is added to all tables to record when the row was written. This is not a partition key and is not exposed as a schema dimension. It exists for operational use only (debugging, freshness checks).

### 4. SEC: `year` is filing year; 10-K/10-Q additionally track `fiscal_year`

The universal rule holds for SEC without exception: `year` = effective data year. For SEC, the effective year is the **filing year** — the calendar year the document was submitted to EDGAR (when it became public information).

10-K and 10-Q filings have a second time dimension worth tracking independently: the **fiscal year** the filing covers, which may differ from the filing year (e.g. a 10-K for fiscal year ending March 2024 filed in June 2024 has `year=2024`, `fiscal_year=2024`, but a company with an October fiscal year end would have `year=2025`, `fiscal_year=2024`). `fiscal_year` is added as a partition column on 10-K and 10-Q tables so users can efficiently find all filings covering a given fiscal period.

Other SEC filing types (8-K, proxy, etc.) carry only `year` (filing year) — no fiscal period dimension applies.

## Consequences

### Implementation changes required

1. **DimensionIterator**: all year boundary resolution changed to apply `dataset.dataLag` as an offset to every boundary (`processing_start - N`, `current_year - N`), for all worker modes. No longer a ceiling cap on end year only.
2. **SEC schema**: `fiscal_year` promoted to partition column on 10-K and 10-Q tables.
3. **DQ T8 checks**: NCVS and other lagged tables should check `MAX(year) >= current_year - dataLag` (e.g., 2024 in 2026 with dataLag=2), not `MAX(year) >= current_year`. Daily coverage check applies to the effective year, not the processing year.
4. **Operations docs**: `GOVDATA_START_YEAR` is the effective data start year for unlagged tables. For lagged tables, effective coverage starts at `GOVDATA_START_YEAR` (not `GOVDATA_START_YEAR - dataLag`). Operators do not need to subtract dataLag when setting start year.

### No change

- Cache keys are based on URL parameters, which already use the effective year (the API parameter). No cache invalidation required.
- Stored partition years for most schemas are already correct (effective year = fetched year).
- Historical worker behavior for non-lagged schemas is unchanged.

### Re-DQ requirement after implementation

Re-DQ is scoped to individual datasets (tables), not schemas. Only tables with `dataLag > 0` — whether inherited from the schema-level `dataLagYears` or declared at the table level — must be wiped, re-ETL'd, and re-DQ'd after the `DimensionIterator` change ships. Tables within the same schema that have `dataLag: 0` are unaffected.

At implementation time, the full list of impacted tables must be enumerated by scanning every schema YAML for `dataLag > 0` at either level. From the initial survey, the schemas containing at least some impacted tables are: crime, econ, energy, edu, geo, weather, census. Within each, only the specific tables with `dataLag > 0` require re-DQ.

Re-DQ only applies to tables that have already achieved a DQ PASS or WARN status. Tables not yet DQ'd simply proceed to their first DQ run after the implementation ships — there is nothing to invalidate.

As of 2026-05-21, the specific tables requiring re-DQ are:

| Schema | Current DQ | Tables to re-DQ |
|---|---|---|
| energy | PASS | All 12 tables (each has dataLag 1–2) |
| edu | WARN | All 4 tables (dataLag 2–3 each) |
| geo | PASS | The 2 tables with dataLag: 1 and dataLag: 3 (schema default is 0) |
| patents | WARN | All 3 tables (dataLag: 1 each) |
| weather | WARN | All tables except the one with explicit dataLag: 0 override (schema default dataLagYears: 1) |

Schemas already DQ'd with no dataLag > 0 tables — no re-DQ required: fec, fedregister, lands, health, ref, econ_reference, cyber_threat, cyber_vuln.

Schemas PENDING DQ — first run after implementation, no re-DQ: crime, econ, census, sec.

Tables with `dataLag: 0` (explicitly or by default) and schemas with `dataLagYears: 0` (sec, lands, fedregister) do not require re-DQ for this change.

The `_ingest_ts` column addition and the SEC `fiscal_year` partition change are independent triggers — each requires its own targeted rebuild of the affected tables, scoped by the same per-table principle.

### Known deferred items

- `_ingest_ts` column rollout across all tables.
- SEC `fiscal_year` partition migration.
- DQ T8 threshold updates for all lagged tables (replace hardcoded year values with `current_year - dataLag` computed inline).
