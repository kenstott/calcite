# Health Data Maintenance Runbook

## Quick Reference

| Slot | When to run |
|---|---|
| `health:initial` | First-time setup — all 15 tables, full fetch |
| `health:daily` | Daily pool — clinical trials delta |
| `health:weekly` | Daily pool — CDC COVID vaccinations + mortality |
| `health:monthly` | Daily pool — stable reference tables |

```bash
cd scripts/parallel

# First-time setup — health runs as part of the full historical load
./run-pool.sh historical
# — or health initial only —
./run-pool.sh --schema health historical

# Recurring — health workers run automatically as part of the daily pool
./run-pool.sh daily
# — or health only —
./run-pool.sh --schema health daily

# Force all sub-runs regardless of release window (backfill / manual refresh)
# Via run-pool (preferred — memory management, logging, pool coordination)
./run-pool.sh --force --schema health daily
# Direct worker invocation (bypasses pool; useful for isolated testing)
./worker-health.sh weekly --force
./worker-health.sh monthly --force
```

---

## Prerequisites

### Environment Variables

Set these in `.env.prod` (or the environment used by your cron/scheduler):

```bash
# Health data uses GOVDATA_PARQUET_DIR and GOVDATA_CACHE_DIR — no schema-specific path overrides needed

# Global incremental cutoffs — set by run-pool.sh from GOVDATA_INCREMENTAL_START_YEAR automatically.
# Override only for manual one-off runs:
# export GOVDATA_SINCE_DATE=2024-01-01          # ISO date for daily/weekly date-filtered tables
# export GOVDATA_SINCE_YEAR=2024                # 4-digit year for year-filtered tables
export MEDICAID_SINCE_QUARTER=1                 # Quarter (1-4) for Medicaid delta start (no global equivalent)

# Optional API keys
export HEALTH_FDA_API_KEY=your-fda-key          # register at open.fda.gov/apis/authentication
export MEDICAID_DRUG_UTIL_DATASET_ID=d890d3a9-6b00-43fd-8b31-fcba4c8e2909  # 2023 default
```

If incremental env vars are unset or empty (the `${VAR:}` default-empty pattern), the
corresponding source fetches all available data — the same behavior as the `initial` mode.

### JAR

The worker script requires the govdata shadow JAR. Build it with:

```bash
./gradlew :govdata:shadowJar
```

---

## Modes in Detail

### `initial` — Run once on first setup

Downloads all 15 health tables without incremental filters. Tables are grouped into separate
model runs to isolate failures and keep each JVM invocation to a manageable scope:

1. **FDA catalogs**: `fda_ndc_products`, `fda_drug_approvals`, `fda_drug_recalls`,
   `fda_adverse_events`, `fda_device_recalls` — paginated openFDA API; rate-limited without key
2. **Clinical trials**: `clinical_trials`, `clinical_trial_conditions`, `clinical_trial_interventions`
   — cursor-paginated clinicaltrials.gov API (~500k studies); takes 30–90 min
3. **CDC sources**: `cdc_covid_vaccinations`, `cdc_mortality`, `cdc_brfss`
   — Socrata SODA API; BRFSS uses unquoted numeric year filter (`quoteValues: false`)
4. **CMS + Medicaid**: `cms_hospital_quality`, `cms_open_payments`, `medicaid_drug_utilization`
   — Socrata SODA API; Medicaid can be multi-million rows depending on dataset year
5. **RxNorm**: `rxnorm_drugs` — NCBI/NLM RxNorm REST API

After initial completes, switch to the recurring cadence workers. Do not re-run `initial`
routinely — it fetches the full dataset for every source.

```bash
./scripts/parallel/worker-health.sh initial
```

---

### `daily` — Incremental clinical trials delta

Downloads only studies updated since `GOVDATA_SINCE_DATE` using the
`lastUpdatePostDate.gte` filter on the clinicaltrials.gov API. Covers three tables:
`clinical_trials`, `clinical_trial_conditions`, `clinical_trial_interventions`.

`GOVDATA_SINCE_DATE` is automatically set to `${GOVDATA_INCREMENTAL_START_YEAR:-2026}-01-01` by
the worker when run via `run-pool.sh daily`. Override only for manual runs:

```bash
export GOVDATA_SINCE_DATE=2024-01-01
./scripts/parallel/worker-health.sh daily
```

---

### `weekly` — CDC COVID vaccinations delta + CDC mortality refresh

- **`cdc_covid_vaccinations`**: Fetches records with `date >= GOVDATA_SINCE_DATE` via
  Socrata `$where` filter (if set). CDC publishes weekly updates.
- **`cdc_mortality`**: Full refresh — the CDC mortality dataset is relatively small and refreshed
  weekly; no incremental filter is applied.

`GOVDATA_SINCE_DATE` is automatically set by the worker. Override only for manual runs:

```bash
export GOVDATA_SINCE_DATE=2024-01-01
./scripts/parallel/worker-health.sh weekly
```

---

### `monthly` — Stable reference tables

Refreshes sources that change on a monthly or slower cadence:

| Table | Source | Incremental mechanism |
|---|---|---|
| `cdc_brfss` | CDC Socrata (BRFSS surveys) | `year >= GOVDATA_SINCE_YEAR` (unquoted; numeric field) |
| `medicaid_drug_utilization` | data.medicaid.gov (DLTSS) | `year`/`quarter` compound `$where` filter via `GOVDATA_SINCE_YEAR` + `MEDICAID_SINCE_QUARTER` |
| `cms_hospital_quality` | data.cms.gov | Full refresh (~5,400 hospitals) |
| `cms_open_payments` | data.cms.gov | Full refresh |
| `fda_ndc_products` | openFDA | Full refresh; NDC catalog changes slowly |
| `fda_drug_approvals` | openFDA | Full refresh |
| `fda_drug_recalls` | openFDA | Full refresh |
| `fda_adverse_events` | openFDA | Full refresh |
| `fda_device_recalls` | openFDA | Full refresh |
| `rxnorm_drugs` | NCBI/NLM RxNorm | Full refresh |

**When to run:** Monthly. A common schedule is the 1st of each month at 02:00 UTC.

```bash
# GOVDATA_SINCE_YEAR is set automatically by run-pool.sh; override only for manual runs
export GOVDATA_SINCE_YEAR=2022
export MEDICAID_SINCE_QUARTER=1
./scripts/parallel/worker-health.sh monthly
```

---

## Cron Schedule

Health workers run as part of the perpetual runner (`run-scheduled.sh`) or `./run-pool.sh daily` — no separate cron entry needed. See the main operations guide for setup.

To run health workers in isolation:

    ./run-pool.sh --schema health daily

---

## Release-Window Checks

The weekly and monthly slots gate sub-runs to known release windows. Each check exits in milliseconds —
no network I/O, no model file written, pool slot released immediately for historical workers.

| Mode | Sub-run | Window | Mechanism | Notes |
|---|---|---|---|---|
| `daily` | clinical trials | Every day | No gate — ClinicalTrials.gov updates continuously | Always runs |
| `weekly` | CDC COVID + mortality | Monday only (DOW 1) | `within_release_dow` | CDC publishes Friday; Monday catches it |
| `monthly` | BRFSS | May–Aug (months 5–8) | `within_release_window` | Annual survey data release |
| `monthly` | Medicaid drug utilization | Mar/Jun/Sep/Dec (months 3,6,9,12) | `within_release_window` | Quarterly, ~3-month lag |
| `monthly` | CMS open payments | Jun–Jul (months 6–7) | `within_release_window` | Annual release |
| `monthly` | CMS hospital quality | Jul–Oct (months 7–10) | `within_release_window` | Annual release |
| `monthly` | FDA catalogs + RxNorm | Every month | No gate — continuous/monthly | Always runs |

To bypass: `./run-pool.sh --force --schema health daily` (preferred) or `./worker-health.sh monthly --force` (direct).
`--force` skips window checks only — year bounds are unchanged.

---

## Troubleshooting

### CDC Socrata rate limiting (HTTP 500)

CDC SODA endpoints return HTTP 500 (not 429) when rate-limited. If multiple health workers
run concurrently or back-to-back, add a delay between them:

```bash
# Run CDC sources after a brief gap following the daily clinical trials run
./run-pool.sh health:daily && sleep 120 && ./run-pool.sh health:weekly
```

The `unquotedNumericYearFilter` integration test also uses `Thread.sleep(2000)` and
`assumeTrue` guards for this reason.

### BRFSS `year` filter: must be unquoted

The BRFSS `year` field in Socrata is a **numeric** type. The `$where` filter must not quote
the value (e.g., `year >= 2020`, not `year >= '2020'`). This is handled by `quoteValues: false`
in the `health-schema.yaml` BRFSS `incremental:` block. Do not change it.

### Clinical trials cursor pagination

The clinicaltrials.gov v2 API uses `nextPageToken` for cursor-based pagination. The
`HttpSource` CURSOR pagination case extracts this token from each response body and passes
it as `pageToken` in the next request. If an initial load hangs, check the log for the last
`pageToken` emitted — the API occasionally returns an empty token mid-page on timeout.

### Medicaid dataset ID

Medicaid publishes separate datasets per year. The default dataset ID
(`d890d3a9-6b00-43fd-8b31-fcba4c8e2909`) is the 2023 annual file. To load a different year,
set `MEDICAID_DRUG_UTIL_DATASET_ID` in `.env.prod` before running:

```bash
export MEDICAID_DRUG_UTIL_DATASET_ID=<dataset-id-for-target-year>
./scripts/parallel/worker-health.sh monthly
```

Dataset IDs can be found at [data.medicaid.gov](https://data.medicaid.gov) by searching
"State Drug Utilization Data".

### openFDA rate limits

Without an API key, openFDA allows ~240 requests/minute. For a full initial load of adverse
events (millions of records), this is the primary bottleneck. Register for a free key at
[open.fda.gov/apis/authentication](https://open.fda.gov/apis/authentication) and set
`HEALTH_FDA_API_KEY` in `.env.prod`.

### Log location

Logs are written to `scripts/parallel/runs/worker-health-<mode>/etl_<timestamp>.log`.

```bash
tail -f scripts/parallel/runs/worker-health-initial/etl_*.log
tail -f scripts/parallel/runs/worker-health-daily/etl_*.log
```
