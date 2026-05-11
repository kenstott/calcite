# GovData DQ Status

Last updated: 2026-05-11

## How to Read This

- **PASS** — all tests pass
- **WARN** — no failures; at least one warning (acceptable thresholds exceeded)
- **FAIL** — one or more hard failures
- **PENDING** — DQ not yet run or rebuild in progress
- **STALE** — last run > 30 days ago

Results are stored at: `r2:govdata-tracker-v1/dq-results/schema={schema}/run_date=.../type=.../results.parquet`

To re-query any schema:
```bash
duckdb -c "SELECT table_name, test, status, value, detail \
  FROM iceberg_scan('s3://govdata-parquet-v1/...') \
  WHERE status != 'pass';"
```

---

## Schema Status

| Schema       | Last Run   | Result  | Fails | Warns | Notes |
|--------------|------------|---------|-------|-------|-------|
| weather      | 2026-05-11 | WARN    | 0     | 2     | See details below |
| edu          | —          | PENDING | —     | —     | Rebuild running 2026-05-11; config fixed (ccd_schools/ipeds bulk CSV dims, naep dataLag) |
| census       | —          | PENDING | —     | —     | Data in R2; DQ not yet run |
| econ         | —          | PENDING | —     | —     | Data in R2; DQ not yet run |
| crime        | —          | PENDING | —     | —     | Data in R2; DQ not yet run |
| geo          | —          | PENDING | —     | —     | Data in R2; DQ not yet run |
| fec          | —          | PENDING | —     | —     | Data in R2; DQ not yet run |
| fedregister  | —          | PENDING | —     | —     | Data in R2; DQ not yet run |
| lands        | —          | PENDING | —     | —     | Rebuild running 2026-05-11 (historical) |
| health       | —          | PENDING | —     | —     | Data in R2; DQ not yet run |
| patents      | —          | PENDING | —     | —     | Data in R2; DQ not yet run |
| ref          | —          | PENDING | —     | —     | Data in R2; DQ not yet run |
| cyber_threat | —          | NO DATA | —     | —     | No Iceberg data in R2; ETL not yet run |
| cyber_vuln   | —          | NO DATA | —     | —     | No Iceberg data in R2; ETL not yet run |

---

## Weather (2026-05-11) — WARN

All 13 tables readable and non-empty. Two warnings:

| Table | Test | Detail |
|-------|------|--------|
| ghcnd_daily | all_same_value | columns: `year`, `tmax_flag`, `tmin_flag` — expected for single-year partition writes |
| hms_smoke_daily | all_same_value | column: `year` — expected for daily partition writes |

These are structural artifacts of partitioned writes (each partition has one year value). Consider
loosening `all_same_value` threshold for partition key columns, or exempting known partition columns.

---

## Edu — Rebuild in Progress

Config bugs fixed on 2026-05-11 (will take effect on next rebuild):
- `ccd_schools` and `ipeds_institutions`: removed `dimensions.year` (bulk CSV + `responsePartitioning` tables must not have year dimensions — they split internally)
- `naep_scores`: replaced `maxYear: 2024` with `dataLag: 2` (dynamic ceiling); fixed `minYear` to hardcoded `2013` (was env var that broke smoke tests)

Known prior DQ issues to investigate after rebuild:
- `crdc`: null `crdc_id` values
- `ipeds`: null `award_level` values
- `naep_scores`: all-null columns (blocked on clean rebuild)
