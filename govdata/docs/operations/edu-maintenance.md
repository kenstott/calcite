# Education Data Maintenance Runbook

## Quick Reference

| Worker | Mode | Workers |
|---|---|---|
| worker-71 | initial/backfill | included in `./run-pool.sh historical` |
| worker-72–73 | recurring | included in `./run-pool.sh daily` |

```bash
cd scripts/parallel

# First-time setup — edu runs as part of the full historical load
./run-pool.sh historical
# — or education initial only —
./run-pool.sh --schema edu historical

# Recurring — edu workers run automatically as part of the daily pool
# Workers 72–73 use release-window checks: each sub-run skips instantly
# if today's date is outside that source's known release window.
./run-pool.sh daily
# — or edu only —
./run-pool.sh --schema edu daily

# Force all sub-runs regardless of release window (backfill / manual refresh)
# Via run-pool (preferred — memory management, logging, pool coordination)
./run-pool.sh --force --schema edu daily
# Direct worker invocation (bypasses pool; useful for isolated testing)
./worker-edu.sh annual --force
./worker-edu.sh biennial --force
```

---

## Prerequisites

### Environment Variables

Set these in `.env.prod` (or the environment used by your cron/scheduler):

```bash
# Recommended — output paths (fall back to GOVDATA_PARQUET_DIR/source=edu and GOVDATA_CACHE_DIR/edu)
export EDU_PARQUET_DIR=/data/edu           # or s3://your-bucket/govdata/source=edu
export EDU_CACHE_DIR=/data/edu-cache

# Required for College Scorecard tables (free key at api.data.gov/signup)
export COLLEGE_SCORECARD_API_KEY=your_key_here

# Global incremental boundary (all edu tables use this; set by run-pool.sh from GOVDATA_INCREMENTAL_START_YEAR)
# Daily workers export GOVDATA_SINCE_YEAR=${GOVDATA_INCREMENTAL_START_YEAR:-2026} automatically.
# Override only if you need a manual one-off run starting from a specific year:
# export GOVDATA_SINCE_YEAR=2023
```

### API Access

| Source | Access | Notes |
|---|---|---|
| Urban Institute Education Data Portal | Free, no key | CCD, CRDC, NAEP, IPEDS via `educationdata.urban.org` |
| College Scorecard | Free key required | `api.data.gov/signup`; without key scorecard tables are skipped |
| NAEP Nation's Report Card | Free, no key | `api.nationsreportcard.gov` |

---

## Data Release Calendar

Understanding release windows helps schedule annual cron jobs. The recurring workers automatically use `GOVDATA_SINCE_YEAR` (set from `GOVDATA_INCREMENTAL_START_YEAR`) as the incremental start.

| Source | Typical Release Window | Tables | Notes |
|---|---|---|---|
| CCD Districts | July–August | `ccd_districts` | NCES posts preliminary in summer, revised in fall |
| CCD Schools | July–August | `ccd_schools` | Same release cycle as CCD Districts |
| IPEDS Institutions/Completions/Tuition | November | `ipeds_institutions`, `ipeds_completions`, `ipeds_tuition` | Fall Data Collection release |
| IPEDS Financials | January | `ipeds_financials` | Finance survey lags other IPEDS surveys by ~2 months |
| College Scorecard | October | `college_scorecard`, `college_scorecard_programs` | Annual cohort data release |
| NAEP Assessments | January–March (odd years) | `naep_scores` | Biennial NAEP cycles: grades 4/8 reading/math |
| CRDC Civil Rights Data | ~18 months after survey year | `crdc_schools` | Even-year surveys; e.g., 2020-21 survey released late 2022 |

**Practical implication for worker 72:**
- Run once in November to pick up IPEDS institutions/completions/tuition + College Scorecard
- Re-run in January to pick up IPEDS financials (same worker, idempotent — only the finance
  tables will have new data)

---

## Table Inventory

### K-12 Tables (worker-72 annual, worker-73 biennial)

| Table | Source | Cadence | Inc. env var | Years available |
|---|---|---|---|---|
| `ccd_districts` | NCES CCD | Annual | `GOVDATA_SINCE_YEAR` | 1987–present |
| `ccd_schools` | NCES CCD | Annual | `GOVDATA_SINCE_YEAR` | 1987–present |
| `naep_scores` | NCES NAEP | Biennial | `GOVDATA_SINCE_YEAR` | 1990–present |
| `crdc_schools` | Dept of Ed CRDC | Biennial | `GOVDATA_SINCE_YEAR` | 2009–present |

### Higher Education Tables (worker-72 annual)

| Table | Source | Cadence | Inc. env var | Years available |
|---|---|---|---|---|
| `ipeds_institutions` | NCES IPEDS HD | Annual | `GOVDATA_SINCE_YEAR` | 1986–present |
| `ipeds_completions` | NCES IPEDS C | Annual | `GOVDATA_SINCE_YEAR` | 1984–present |
| `ipeds_financials` | NCES IPEDS F | Annual (Jan release) | `GOVDATA_SINCE_YEAR` | 1986–present |
| `ipeds_tuition` | NCES IPEDS IC_AY | Annual | `GOVDATA_SINCE_YEAR` | 2000–present |
| `college_scorecard` | Dept of Ed Scorecard | Annual | `GOVDATA_SINCE_YEAR` | 1996–present |
| `college_scorecard_programs` | Dept of Ed Scorecard | Annual | `GOVDATA_SINCE_YEAR` | 2014–present |

---

## Monitoring & Troubleshooting

```bash
# Follow the initial load
tail -f scripts/parallel/runs/worker-edu-initial/etl_*.log

# Follow an annual refresh
tail -f scripts/parallel/runs/worker-edu-annual/etl_*.log

# Check for errors
grep -r "ERROR\|FAILED\|Exception" scripts/parallel/runs/worker-edu-*/

# Verify output — check a recent year of CCD districts
duckdb -c "DESCRIBE SELECT * FROM read_parquet('${EDU_PARQUET_DIR}/source=edu/ccd_districts/year=2022/*.parquet')"
```

### Release-Window Checks

Workers 72 and 73 gate each sub-run to its source's known release window. On a daily pool
run, a sub-run outside its window exits in milliseconds (no network I/O, no model file),
freeing its pool slot for historical workers immediately.

| Sub-run | Window | Year constraint | Worker |
|---|---|---|---|
| CCD districts/schools | July–August (months 7–8) | Any | 72 |
| College Scorecard | October (month 10) | Any | 72 |
| IPEDS institutions/completions/tuition | November (month 11) | Any | 72 |
| IPEDS financials | January (month 1) | Any | 72 |
| NAEP assessments | January–March (months 1–3) | Odd years only | 73 |
| CRDC civil rights data | October–December (months 10–12) | Odd years only (publication year) | 73 |

To bypass all checks for a manual run:
```bash
./run-pool.sh --force --schema edu daily   # preferred; uses pool memory management
./worker-edu.sh annual --force             # direct; bypasses pool
./worker-edu.sh biennial --force
```
`--force` skips window checks only — year bounds (`GOVDATA_SINCE_YEAR`) are unchanged.

### Common Issues

**College Scorecard tables missing from output**
- Check `COLLEGE_SCORECARD_API_KEY` is set in `.env.prod`
- Worker logs will show: `COLLEGE_SCORECARD_API_KEY not set — skipping college_scorecard tables`

**NAEP or CRDC returns no new data**
- Expected off-cycle behavior — these sources are biennial
- Worker completes normally; existing parquet is unchanged
- NAEP odd-year cycles: 2019, 2021, 2022 (partial), 2024; CRDC: 2016, 2018, 2020, 2022

**IPEDS financials not updated after November run**
- Finance survey releases in January, not November
- Re-run worker 72 in January; the run is idempotent (only finance tables will have new data)

**Urban Institute API rate limiting**
- The API is unauthenticated but rate-limited (~60 req/min)
- The adapter backs off automatically; no action needed
- If consistently slow, reduce `ETL_PARALLEL_THREADS` (or omit `-p` flag)

---

## Tuning

```bash
# Initial load: allow longer timeout for full IPEDS history (4+ hours)
./run-pool.sh -t 240 71

# Annual refresh: standard timeout is sufficient
./run-pool.sh -t 90 72

# On a memory-constrained machine (16 GB): reserve more for OS
./run-pool.sh -r 2000 -t 240 71

# Run edu initial alongside health recurring (pool manages memory automatically)
./run-pool.sh 68,69,70,71
```
