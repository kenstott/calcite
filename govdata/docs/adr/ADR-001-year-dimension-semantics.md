# ADR-001: Year Dimension Semantics

**Status:** Accepted  
**Date:** 2026-05-21

## Context

The govdata platform uses a `year` (and `month`) partition dimension across all Iceberg tables. Different schemas handle time differently:

- Most schemas fetch the year directly from the source API and store it as-is.
- Schemas with `dataLag` (NCVS, SEDS, some econ tables) publish data with a fixed delay — the effective data year differs from the year the ETL ran.
- Some schemas embed the effective year in the response payload (as a bare year, a date field, or a derived expression).

The core questions were: what does `year` mean as a partition key, how does `dataLag` interact with it, and how does the system handle responses where the effective year comes from the data itself rather than the request context.

## Decision

### 1. `year` partition key is always the effective data year

The Iceberg `year` (and `month`) partition column stores the period the data describes — the year of the survey, measurement, generation, or report. This is the only semantics that makes sense for analytical queries: `WHERE year = 2024` returns 2024 data, always.

### 2. Iteration is over publish years; effective year is a derived companion

The ETL iterates over **publish years** — the year the data was requested from the source. `GOVDATA_START_YEAR`, `GOVDATA_INCREMENTAL_START_YEAR`, and `current_year` are all publish year boundaries.

For every YEAR_RANGE dimension, `DimensionIterator` always injects a companion variable:

```
effective_year = year - dataLag
```

When `dataLag=0` (the common case), `effective_year = year` — no distinction. When `dataLag > 0`, `effective_year` is the shifted value. The companion is always available in the URL template context regardless of whether `dataLag` is zero.

The end cap for iteration is `current_year - dataLag` — you cannot request data for publish years where the effective data does not yet exist.

URL templates declare which year to use for the API call:
- `${year}` — publish year (for APIs that accept the publish year)
- `${effective_year}` — effective year (for APIs that accept the data year, which is most current schemas)

The ETL designer chooses. Target state for all existing lagged schemas is `${effective_year}` in the URL template.

### 3. Per-row effective year from response data takes precedence

The `effective_year` companion is a best-effort approximation based on a fixed offset. The response payload is authoritative. When the response contains the effective year for each row, that value is used as the partition key.

The schema declares this via `effectiveYearField` (and `effectiveMonthField`) on the dimension config, pointing to a column name in the output row. That column may be a raw response field or a SQL computed column using the existing `expression:` column syntax:

```yaml
columns:
  - name: data_year
    type: integer
    expression: "EXTRACT(YEAR FROM survey_date)"

dimensions:
  - name: year
    type: YEAR_RANGE
    dataLag: 2
    effectiveYearField: data_year
```

The SQL expression handles all extraction cases — bare year integers, ISO date strings, fiscal year logic — using DuckDB SQL. No special extractor logic is needed in the writer.

**Partition key resolution hierarchy (per row, at write time):**
1. `row[effectiveYearField]` — if `effectiveYearField` is declared and the column is present
2. `effective_year` from dimension context (= `year - dataLag`)
3. `year` (publish year, when `dataLag=0` and no field declared)

A single API response may contain rows with different effective years. The writer fans rows across Iceberg partitions per-row. Iceberg handles multi-partition writes in a single commit.

### 4. Processing time is Iceberg table properties, not a data column

ETL run timing is tracked as Iceberg table properties set at commit time:

| Property | Value | Meaning |
|---|---|---|
| `etl.last_run_ts` | ISO-8601 timestamp | Last time the ETL pipeline ran for this table, regardless of whether rows were written |
| `etl.last_rows_written_ts` | ISO-8601 timestamp | Last time the ETL pipeline committed at least one new row to this table |

These are written by `IcebergMaterializationWriter` at the end of each table's ETL pass. They are operational signals for monitoring and freshness checks, not queryable via SQL `SELECT`.

A per-row `_ingest_ts` column is not the default. It may be added to specific tables where row-level write provenance is genuinely needed, but it is not part of this decision.

### 5. Tracker keyed by publish year

The ETL tracker remains keyed by the publish year (the dimension iteration key). It answers "did we execute this API call?" DQ T8 scans Iceberg directly to confirm effective year coverage.

**Self-healing limitations under this model:**

Self-healing (`trySelfHealFromStoredFiles`) detects orphaned parquet files in storage and re-registers them in Iceberg. It pairs storage paths to tracker entries to mark work as processed.

- **Fixed `dataLag` tables**: storage paths use effective year (`year=2024/`); tracker key uses publish year (`year=2026`). Self-healing can reverse the mapping (`publish_year = effective_year + dataLag`) if the writer stores the publish year in parquet file metadata. Without that metadata, the reverse mapping requires schema knowledge at recovery time.

- **Variable-lag tables (`effectiveYearField` declared)**: a single publish-year fetch may produce rows in multiple effective year partitions. The reverse mapping from effective year back to publish year is ambiguous — self-healing cannot determine which tracker entry to credit. These tables fall back to full re-fetch when the tracker is lost. This is acceptable: self-healing is a recovery path, not normal operation.

To support fixed-dataLag self-healing without schema knowledge, the writer should record the publish year as parquet file metadata at write time.

### 6. SEC fiscal year — deferred

SEC fiscal year handling is a special case and is addressed separately. This ADR does not govern SEC `fiscal_year` partition semantics.

## Consequences

### Implementation changes required

1. **DimensionIterator**: always inject `effective_year = year - dataLag` companion for YEAR_RANGE dimensions (even when `dataLag=0`). End cap remains `current_year - dataLag`. Remove the start-year shifting heuristic added for the daily worker boundary case — the companion variable and per-row resolution replace it.
2. **DimensionConfig**: add `effectiveYearField` and `effectiveMonthField` optional fields.
3. **Iceberg writer**: per-row partition key resolution using the hierarchy above. Fan rows across partitions when effective years vary within a single response.
4. **Lagged schema YAMLs**: update URL templates from `${year}` to `${effective_year}` for APIs that accept the effective year (target state for NCVS, SEDS, CDE, and similar).
5. **IcebergMaterializationWriter**: write `etl.last_run_ts` and `etl.last_rows_written_ts` as Iceberg table properties at the end of each table's ETL pass. Also record the publish year in parquet file metadata to enable self-healing reverse mapping for fixed-dataLag tables.
6. **DQ T8 checks**: use `MAX(year) >= current_year - dataLag` for lagged tables. `year` in Iceberg is the effective year.

### No change

- Cache keys are based on URL parameters. When URL templates are updated to `${effective_year}`, cache keys shift accordingly — this is intentional and correct.
- Tracker infrastructure is unchanged.
- Schemas with `dataLag=0` and no `effectiveYearField` are unaffected: `effective_year = year`, partition key = publish year = effective year.
- Existing SQL computed column infrastructure (`expression:` on column config, evaluated by DuckDB) requires no changes — it is the mechanism for `effectiveYearField` extraction.

### Re-DQ requirement after implementation

Re-DQ is scoped to individual tables, not schemas. Only tables that have already achieved DQ PASS or WARN status and have `dataLag > 0` require wipe, re-ETL, and re-DQ after the writer change ships.

As of 2026-05-21:

| Schema | Current DQ | Tables to re-DQ |
|---|---|---|
| energy | PASS | All 12 tables (each has dataLag 1–2) |
| edu | WARN | All 4 tables (dataLag 2–3 each) |
| geo | PASS | The 2 tables with dataLag: 1 and dataLag: 3 |
| patents | WARN | All 3 tables (dataLag: 1 each) |
| weather | WARN | All tables except the explicit dataLag: 0 table |

No re-DQ required: fec, fedregister, lands, health, ref, econ_reference, cyber_threat, cyber_vuln.

Pending first DQ (not re-DQ): crime, econ, census.

### Known deferred items

- **`etl.last_run_ts` / `etl.last_rows_written_ts`**: implement in `IcebergMaterializationWriter`; no data rebuild required.
- **SEC `fiscal_year`**: addressed separately; not in scope for this ADR.
- **DQ T8 threshold updates**: replace hardcoded year values with `current_year - dataLag` for all lagged tables.
- **Publish-year API support**: if a data source takes the publish year as its API parameter, the URL template uses `${year}`. For APIs that take the effective year (most current schemas), the template uses `${effective_year}`. The `year_source` companion variable concept from earlier drafts is superseded by this model — `effective_year` is the companion, and `${year}` is always the publish year.
