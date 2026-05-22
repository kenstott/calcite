# Energy Data Maintenance Runbook

## Quick Reference

| Slot | When to run |
|---|---|
| `energy:initial` | First-time setup — full historical load |
| `energy:weekly` | Daily pool — EIA weekly storage and stock data |
| `energy:monthly` | Daily pool — monthly EIA series refresh |
| `energy:annual` | Daily pool — annual survey releases |

```bash
cd scripts/parallel

# First-time setup — energy runs as part of the full historical load
./run-pool.sh historical
# — or energy initial only —
./run-pool.sh --schema energy historical

# Recurring — energy workers run automatically as part of the daily pool
./run-pool.sh daily
# — or energy only —
./run-pool.sh --schema energy daily

# Force all sub-runs regardless of release window (backfill / manual refresh)
# Via run-pool (preferred — memory management, logging, pool coordination)
./run-pool.sh --force --schema energy daily
# Direct worker invocation (bypasses pool; useful for isolated testing)
./worker-energy.sh weekly --force
./worker-energy.sh monthly --force
./worker-energy.sh annual --force
```

---

## Prerequisites

### Environment Variables

Set these in `.env.prod` (or the environment used by your cron/scheduler):

```bash
# Required — global paths (shared by all govdata workers)
export GOVDATA_PARQUET_DIR=/data/govdata        # or s3://your-bucket/govdata
export GOVDATA_CACHE_DIR=/data/govdata-cache    # or s3://your-bucket/govdata-cache

# Optional but recommended — raises EIA API rate limit from ~1 req/s to 5 req/s
export ENERGY_EIA_API_KEY=your-eia-api-key      # register free at eia.gov/opendata

# Optional — limits year range for recurring workers (default varies per series)
export GOVDATA_START_YEAR=2010
```

### JAR

The worker scripts require the govdata shadow JAR. Build it with:

```bash
./gradlew :govdata:shadowJar
```

---

## Modes in Detail

### `initial` — Run once on first setup

Downloads the full historical dataset using `GOVDATA_START_YEAR` (default 2010) as the start year.
No release-window checks — initial always runs all tables.

Sub-runs and what they cover:

1. **`energy-initial-electricity`** — EIA API v2 electricity generation and retail prices, monthly series from 2010 onward by state, sector, and energy source (~5–15 min depending on API key)
2. **`energy-initial-annual-surveys`** — EIA Form 861 (utility survey) and EIA Form 860 (power plant inventory), one ZIP archive per year (~10–20 min for all years)
3. **`energy-initial-capacity`** — EIA Form 860M December monthly generator snapshot; available from 2015 onward
4. **`energy-initial-supply`** — EIA API v2 fossil fuel production (crude), SEDS state energy consumption, and refinery operations
5. **`energy-initial-weekly`** — EIA API v2 weekly natural gas storage and petroleum stocks from 2000 onward
6. **`energy-initial-imports`** — EIA Form 814 crude oil imports; monthly XLSX archives, one per month per year (~4–8 min for all years)
7. **`energy-initial-coal`** — MSHA MinesProdYearly bulk pipe-delimited download; transformer filters by year range

### `weekly` — EIA Thursday/Wednesday releases

EIA publishes weekly storage and stock data on a rolling schedule:
- **Natural gas storage** — published Thursdays at 10:30 ET (one-week lag)
- **Petroleum stocks** — published Wednesdays at 10:30 ET (one-week lag)

Each sub-run checks the table's `releaseWindow` from `energy-schema.yaml`; if outside the window,
it skips instantly. Pass `--force` to run regardless.

### `monthly` — Monthly EIA data releases

EIA monthly series typically lag 2–3 months. The monthly worker refreshes the highest-frequency
tables: electricity generation/prices, capacity changes, fossil fuel production, refinery
operations, and crude oil imports.

Run on the 15th of each month to capture the prior month's data for most series.

### `annual` — Annual survey releases

Annual surveys have a 1–2 year data lag. Typical release windows:
- **EIA-861 utility survey** — released May–July for the prior calendar year
- **EIA-860 power plant inventory** — released May–July for the prior calendar year
- **SEDS state consumption** — released July–October, 2-year lag
- **MSHA coal mines** — released early in the calendar year for the prior year

Run in October to capture all annual releases for the prior year.

---

## Troubleshooting

### EIA API 429 / rate limit errors

Register a free EIA API key at https://www.eia.gov/opendata/ and set `ENERGY_EIA_API_KEY` in
`.env.prod`. Without a key the API allows ~1 request/second; with a key it allows ~5 req/s.

### EIA-814 crude imports — HTTP 404

The EIA archive URL pattern is `archive/{year}/{year}_{MM}/data/import.xlsx`. Files are typically
available ~2 months after the reference month. Running with `--force` will attempt all months but
will log warnings for months not yet published; this is expected behavior.

### EIA-860 / EIA-861 XLSX parse failures

EIA periodically changes the XLSX column layout. If the transformer logs column-not-found warnings
and produces 0 rows, inspect a sample XLSX manually and compare column names against the
`Eia861Transformer` / `Eia860Transformer` field mappings.

### EIA-860M "Cannot find zip signature"

The archive URL for older years uses `/archive/xls/december_generator{year}.xlsx`. Confirm the
correct URL pattern if a year returns a parse error; EIA sometimes restructures the archive.

### 0 rows despite successful HTTP downloads

If all months/years return HTTP 200 but row counts are 0, check that the HttpSource `dataPath`
guard is in place in `HttpSource.parseResponse()`: `dataPath` navigation must be skipped when a
`responseTransformer` is active (it already extracted the array). See the fix at
`file/src/main/java/org/apache/calcite/adapter/file/etl/HttpSource.java`.

### Natural gas storage 400 errors

The `/stor/sum` endpoint does not support `frequency=weekly`. Use `/stor/wkly` for weekly series.
This is already correct in `energy-schema.yaml` but verify if you see `HTTP 400` on gas storage.

---

## Data Freshness Summary

| Table | Source cadence | Typical data lag | Slot |
|---|---|---|---|
| `eia_electricity_generation` | Monthly | 2 months | `energy:monthly` |
| `eia_electricity_prices` | Monthly | 2 months | `energy:monthly` |
| `eia_utility_annual` | Annual | 1 year | `energy:annual` |
| `eia_power_plants` | Annual | 1 year | `energy:annual` |
| `eia_capacity_changes` | Monthly | 1 month | `energy:monthly` |
| `eia_fossil_fuel_production` | Monthly | 2 months | `energy:monthly` |
| `eia_state_energy_consumption` | Annual | 2 years | `energy:annual` |
| `eia_natural_gas_storage` | Weekly | 1 week | `energy:weekly` |
| `eia_petroleum_stocks` | Weekly | 1 week | `energy:weekly` |
| `eia_crude_oil_imports` | Monthly | 2 months | `energy:monthly` |
| `eia_refinery_operations` | Monthly | 2 months | `energy:monthly` |
| `eia_coal_mines` | Annual | 1 year | `energy:annual` |
