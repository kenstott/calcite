# Lands Data Maintenance Runbook

## Quick Reference

| Worker | Mode | Script |
|---|---|---|
| worker-82 | historical/initial | included in `./run-pool.sh historical` |
| worker-83 | daily/recurring | included in `./run-pool.sh daily` |

```bash
cd scripts/parallel

# First-time setup — lands runs as part of the full historical load
./run-pool.sh historical
# — or lands initial only —
./run-pool.sh --schema lands historical

# Recurring — lands workers run automatically as part of the daily pool
./run-pool.sh daily
# — or lands only —
./run-pool.sh --schema lands daily

# Force all sub-runs regardless of release window (backfill / manual refresh)
./run-pool.sh --force --schema lands daily

# Direct worker invocation (bypasses pool; useful for isolated testing)
./worker-lands.sh historical
./worker-lands.sh daily
./worker-lands.sh daily --force
```

---

## Prerequisites

### Environment Variables

All lands tables use the shared `GOVDATA_*` globals — no schema-specific env vars required:

```bash
# Required — global paths (shared by all govdata workers)
export GOVDATA_PARQUET_DIR=s3://govdata-parquet-v1    # or local path
export GOVDATA_CACHE_DIR=s3://govdata-raw-v1

# Optional — date range for historical/incremental runs
export GOVDATA_START_YEAR=2010                         # historical start (default 2010)
export GOVDATA_INCREMENTAL_START_YEAR=2026             # daily/incremental start (default 2026)
```

No API keys are needed. All lands data sources are public and unauthenticated.

### JAR

Build the govdata shadow JAR before running:

```bash
./gradlew :govdata:shadowJar
```

To use a specific JAR (e.g., from a worktree during development):

```bash
export GOVDATA_JAR=/path/to/calcite-govdata-1.42.0-SNAPSHOT-all.jar
./worker-lands.sh historical
```

---

## Modes in Detail

### `historical` — Run once on first setup

Downloads the full historical dataset from `GOVDATA_START_YEAR` through
`GOVDATA_INCREMENTAL_START_YEAR - 1`. Release-window checks are skipped.

Sub-runs and what they cover:

1. **`lands-historical-static`** — `national_forests`, `nps_units`, `blm_field_offices`.
   These are static reference tables with no year dimension. Fetched once for the full range.
   (~1–5 min; data is small, network-bound)

2. **`lands-historical-timber`** — `timber_sales` (timber harvest activities by fiscal year,
   `GOVDATA_START_YEAR` – present). One ArcGIS query per year. (~5–20 min)

3. **`lands-historical-inventory`** — `forest_inventory` (FIA DataMart, per-year queries).
   Currently returns 0 rows due to FIA API format issue (see Troubleshooting). (~1 min; fast-fail)

4. **`lands-historical-visitation`** — `nps_visitation` (NPS IRMA monthly stats, all parks,
   one query per year). Returns XML; transformer parses `<ArrayOfVisitationData>`. (~10–30 min)

5. **`lands-historical-revenues`** — `onrr_revenues` (ONRR bulk CSV, single all-years file).
   The CSV covers FY 2004–present; downloaded once regardless of year range. (~5–15 min)

### `daily` — Recurring cadence

Runs the current year (`GOVDATA_INCREMENTAL_START_YEAR`) for all tables, gated by release
window. Tables outside their release window complete in milliseconds with no network I/O.

| Table | Release window | Typical daily behavior |
|---|---|---|
| `national_forests` | Month 10 | Skips Jan–Sep and Nov–Dec |
| `timber_sales` | Months 3, 6, 9, 12 | Skips most months; runs in Mar/Jun/Sep/Dec |
| `forest_inventory` | Months 6–9 | Skips most months; runs Jun–Sep |
| `nps_units` | Month 4 | Skips all months except April |
| `nps_visitation` | Months 1–12 | Always runs |
| `blm_field_offices` | Month 6 | Skips all months except June |
| `onrr_revenues` | Months 1–12 | Always runs |

Pass `--force` to bypass all window checks and run every table regardless of today's date.

---

## Troubleshooting

### `forest_inventory` — 0 rows, HTML error from FIA API

The FIA DataMart `fullreport` endpoint requires a `wc` (evaluation group) parameter in
`{statecd}{year}` format — e.g., `12024` for Alabama's 2024 inventory. The current ETL
passes `wc={year}` only, which causes the API to return an HTML error page.

**Workaround:** DQ checks T1 (row count) and T2 (coverage) for `forest_inventory` are
downgraded to `warn`. The table will remain empty until `FiaDatamartTransformer` is
redesigned to iterate all 50 state codes × year combinations. See `DQ-ISSUES.md`.

**Impact:** Low — forest inventory data is not on any critical query path. Use
`econ.bls_employment` (NAICS 113) as an alternative proxy for forestry sector activity.

### `national_forests` — 0 rows or field not found

USFS ArcGIS MapServer/0 returns lowercase field names (`forestnumber`, `forestname`,
`region`, `gis_acres`). If a field changes case, `UsfsForestBoundaryTransformer` will
silently produce nulls. Verify with:

```bash
curl -s "https://apps.fs.usda.gov/arcx/rest/services/EDW/EDW_ForestSystemBoundaries_01/MapServer/0/query?where=1=1&outFields=*&resultRecordCount=1&f=json" | python3 -m json.tool | grep -i forest
```

### `timber_sales` — 0 rows for a year

The FACTS `EDW_TimberHarvest_01/MapServer/0` endpoint requires a `FY_COMPLETED='{year}'`
where-clause filter. If the ArcGIS service returns no features for a year, either no
harvests were completed that year or the service URL has changed. Check directly:

```bash
curl -s "https://apps.fs.usda.gov/arcx/rest/services/EDW/EDW_TimberHarvest_01/MapServer/0/query?where=FY_COMPLETED%3D'2024'&outFields=FACTS_ID,SALE_NAME,FY_COMPLETED&resultRecordCount=5&f=json"
```

### `nps_visitation` — XML parse failure

The NPS IRMA endpoint (`irmaservices.nps.gov/Stats/v1/visitation`) returns
`<ArrayOfVisitationData>` XML. If the response changes to JSON in a future API update,
`NpsIrmaTransformer` auto-detects the format and dispatches to `transformJson` (handles
`UnitCode`, `Year`, `Month`, `RecreationVisitors`, `NonRecreationVisitors` keys).

If you see empty results, confirm the service is up:

```bash
curl -s "https://irmaservices.nps.gov/Stats/v1/visitation?startMonth=1&startYear=2024&endMonth=12&endYear=2024" | head -200
```

### `blm_field_offices` — `exceededTransferLimit` exception

`BlmFieldOfficeTransformer` throws if the ArcGIS response sets `exceededTransferLimit: true`.
Increase `resultRecordCount` in `lands-schema.yaml` (currently set high enough for ~150 offices)
or add `resultOffset` pagination to the schema config.

### `onrr_revenues` — 404 or empty CSV

The ONRR domain moved from `revenuedata.doi.gov` to `revenuedata.onrr.gov` in 2024.
The per-year CSV pattern (`/downloads/{year}/...`) no longer exists. Only the bulk all-years
file is available:

```
https://revenuedata.onrr.gov/downloads/fiscal_year_revenue.csv
```

If `onrr_revenues` returns 0 rows, confirm this URL returns CSV content (not an HTML error page):

```bash
curl -sI "https://revenuedata.onrr.gov/downloads/fiscal_year_revenue.csv" | grep -E "content-type|HTTP"
```

---

## Data Freshness Summary

| Table | Source cadence | Typical data lag | Recommended worker |
|---|---|---|---|
| `national_forests` | Annual | ~1 month | worker-83 (monthly=10) |
| `timber_sales` | Quarterly | ~1 quarter | worker-83 (months 3,6,9,12) |
| `forest_inventory` | Annual | ~6–12 months | worker-83 (months 6–9) |
| `nps_units` | Annual | ~1 month | worker-83 (month=4) |
| `nps_visitation` | Monthly | 1–2 months | worker-83 (daily) |
| `blm_field_offices` | Annual | ~1 month | worker-83 (month=6) |
| `onrr_revenues` | Monthly | ~3 months | worker-83 (daily) |

---

## DQ Script

Run the lands DQ script directly against Iceberg:

```bash
duckdb < scripts/lands_dq.sql
```

Or via the DQ runner (produces the standard pass/warn/fail report):

```bash
bash scripts/parallel/worker-dq-run.sh lands
```

All DQ checks should return `pass` except `forest_inventory` T1/T2 which return `warn`
due to the FIA API issue described above.
