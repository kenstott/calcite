# GovData Pipeline Operations Guide

This guide explains how to set up and maintain the full GovData pipeline across all 12 schemas.
The core orchestrator for all loading is `scripts/parallel/run-pool.sh`, which manages
memory-aware concurrent execution of numbered worker scripts.

---

## Worker Map

```
SEC Primary (10-K/10-Q)         workers  1 – 17
SEC Secondary (8-K/Proxy/etc.)  workers 23 – 39
SEC Stock Prices                worker  40
Non-SEC schemas                 workers 18 – 22, 41, 60, 61
Cyber                           workers 62 – 66
```

---

## Prerequisites

### 1. Build the shadow JAR

```bash
cd /path/to/calcite
./gradlew :govdata:shadowJar
```

### 2. Set environment variables

Create `govdata/.env.prod` with at minimum:

```bash
# Storage
GOVDATA_PARQUET_DIR=/data/govdata          # or s3://your-bucket/govdata
GOVDATA_CACHE_DIR=/data/govdata-cache

CYBER_PARQUET_DIR=/data/cyber
CYBER_CACHE_DIR=/data/cyber-cache

# SEC / EDGAR
SEC_EDGAR_USER_AGENT="Your Name your@email.com"

# S3 (if using object storage)
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_ENDPOINT_OVERRIDE=...                  # Cloudflare R2, MinIO, etc.
CALCITE_TRACKER_S3_BUCKET=your-bucket

# Optional but recommended
CYBER_NVD_API_KEY=...                      # nvd.nist.gov/developers
CYBER_OTX_API_KEY=...                      # otx.alienvault.com
CYBER_THREATFOX_API_KEY=...
CYBER_GITHUB_TOKEN=ghp_...
CYBER_OSV_ECOSYSTEMS=PyPI,npm,Go,Maven
BLS_API_KEY=...
FRED_API_KEY=...
```

---

## Initial Setup Strategy

The SEC historical corpus (2010–2025) contains millions of filings and takes several days to
fully materialize. The recommended approach is to **load data that is immediately useful first**,
then run the historical backfill in the background. Recurring updates can begin as soon as
Phase 1 completes — you do not need to wait for the full backfill.

### Phase 1 — Current data (hours, immediately queryable)

Run the current-year SEC workers alongside all non-SEC schemas. These produce a fully
functional dataset: current filings, all economic and reference data, and all cyber intelligence.

```bash
cd scripts/parallel

# Run all Phase 1 workers with memory-aware concurrency
./run-pool.sh 1,18-23,40-41,60-62
```

| Worker | Schema | What it loads |
|---|---|---|
| 1 | sec | Primary filings 2026–present (10-K, 10-K/A, 10-Q, 10-Q/A) |
| 18 | econ | Economic data 2010–2026 (BLS, FRED, BEA, Treasury yields) |
| 19 | census | ACS demographics 2010–2026 |
| 20 | geo | TIGER shapefiles (2024) + HUD crosswalk |
| 21 | crime | FBI/BJS crime statistics 2010–2026 |
| 22 | weather | NWS/NOAA/EPA weather and air quality 2010–2026 |
| 23 | sec | Secondary filings 2026–present (8-K, proxy, insider, 13F, 13D/G) |
| 40 | sec | Stock prices 2010–2026 (Stooq) |
| 41 | ref | Reference identifiers: GLEIF entities, CIK registry, OpenFIGI |
| 60 | fec | FEC campaign finance bulk downloads |
| 61 | fedregister | Federal Register documents 2010–present |
| 62 | cyber | Cyber initial load: full NVD catalog, CWE, KEV, NIST/CIS/OWASP standards, OTX backfill |

**Notes:**
- Workers 18–22, 40, 41, 60, 61 load their full historical range in a single run — they are
  not year-sharded. Their internal incremental tracker skips rows already materialized on re-runs.
- Worker 62 (cyber initial) downloads the full NVD CVE catalog (~350k CVEs). Expect 15–60 minutes
  depending on whether `CYBER_NVD_API_KEY` is set (5× faster with a key).

### Phase 2 — Historical SEC backfill (days, runs in background)

Once Phase 1 is complete, launch the historical SEC workers. These cover each year from 2025 back
to 2010 for both primary (10-K/10-Q) and secondary (8-K/proxy/insider/13F) filing types.

```bash
cd scripts/parallel

# Primary filings: 2025 (worker-02) back to 2010 (worker-17)
# Secondary filings: 2025 (worker-24) back to 2010 (worker-39)
./run-pool.sh 2-17,24-39
```

The pool runner processes these concurrently up to the available memory budget. On a machine with
32GB RAM it typically runs 3–4 workers in parallel. On 64GB, 6–8. Each year takes 2–8 hours
depending on filing volume; the full backfill typically completes in 2–5 days.

To run Phase 1 and Phase 2 together in a single command (for unattended overnight setup):

```bash
./run-pool.sh all
# Equivalent to: ./run-pool.sh 1-41,60-62
```

The `all` keyword always runs current-year and non-SEC workers first since their numbers are
lower and the pool fills in numeric order.

---

## Recurring Updates

Once the initial load is complete, set up the following recurring jobs. These are designed to
be additive — re-running a worker never duplicates data; the tracker marks completed rows.

### Daily (recommended: 06:00 UTC)

```bash
cd scripts/parallel

# SEC current year (picks up filings from today back to 2026-01-01)
./run-pool.sh 1,23

# Cyber: delta NVD CVEs (last 1 day) + CISA KEV refresh
./run-pool.sh 63
```

### Weekly (recommended: Sunday 02:00 UTC)

```bash
cd scripts/parallel

# Non-SEC full re-runs (idempotent; tracker skips already-complete rows)
./run-pool.sh 18-22,40-41,60-61

# Cyber: CWE, OSV, MITRE ATT&CK techniques, GitHub advisories, ATT&CK→NIST mappings
./run-pool.sh 64
```

### Hourly (recommended: every 2 hours)

```bash
cd scripts/parallel

# Cyber live IOC feeds + OTX delta (1 day window)
CYBER_OTX_DELTA_DAYS=1 ./run-pool.sh 65
```

### Cron reference

```cron
# Daily — SEC current year + cyber CVE delta
0 6 * * *   cd /path/to/govdata/scripts/parallel && ./run-pool.sh 1,23 && ./run-pool.sh 63

# Weekly — non-SEC refresh + cyber ATT&CK/standards refresh
0 2 * * 0   cd /path/to/govdata/scripts/parallel && ./run-pool.sh 18-22,40-41,60-61,64

# Hourly — cyber live IOC feeds
0 */2 * * * cd /path/to/govdata/scripts/parallel && CYBER_OTX_DELTA_DAYS=1 ./run-pool.sh 65
```

---

## Schema-by-schema Update Cadence

| Schema | Worker | Update cadence | Mechanism |
|---|---|---|---|
| SEC primary filings | 1 | Daily | Re-runs 2026–present window; skips already-materialized accessions |
| SEC secondary filings | 23 | Daily | Same as above for 8-K/proxy/insider/13F |
| SEC stock prices | 40 | Weekly | Full 2010–2026 re-run via Stooq; tracker deduplicates |
| Economic (BLS/FRED/BEA) | 18 | Weekly | Full 2010–2026 re-run; incremental by series/period |
| Census ACS | 19 | Weekly | Full re-run; ACS releases annually |
| Geographic (TIGER/HUD) | 20 | Annual | TIGER year is pinned; re-run when Census publishes a new vintage |
| Crime (FBI/BJS) | 21 | Weekly | Full re-run; FBI releases lag ~12 months |
| Weather (NWS/NOAA/EPA) | 22 | Weekly | Full re-run; picks up new observation periods |
| Reference (GLEIF/CIK/FIGI) | 41 | Weekly | GLEIF discovers current golden copy URL daily |
| FEC campaign finance | 60 | Weekly | Bulk file re-download; FEC updates files in-place |
| Federal Register | 61 | Daily | Auto-discovers current year; append-only |
| Cyber CVE (NVD/KEV) | 63 | Daily | 1-day delta window via NVD API `lastModStartDate` |
| Cyber standards/ATT&CK | 64 | Weekly | Full re-fetch; sources publish infrequently |
| Cyber IOC feeds | 65 | Hourly | URLhaus/MalwareBazaar/Feodo/ThreatFox/OTX fresh each run |
| Cyber static standards | 66 | On-demand | Re-run after NIST/CIS/OWASP publish new versions |

---

## Historical Backfill Notes

Workers 2–17 (SEC primary) and 24–39 (SEC secondary) each cover a single calendar year.
They are designed to run concurrently via the pool:

```
worker-02 → 2025 primary    worker-24 → 2025 secondary
worker-03 → 2024 primary    worker-25 → 2024 secondary
...
worker-17 → 2010 primary    worker-39 → 2010 secondary
```

Because primary and secondary workers are independent (different filing types, different table
partitions), running `./run-pool.sh 2-17,24-39` will execute them in parallel up to the memory
budget. If you want to load recent history faster, run recent years first:

```bash
# Most recent 3 years first, then older years
./run-pool.sh 2-4,24-26     # 2025, 2024, 2023
./run-pool.sh 5-17,27-39    # 2022 back to 2010
```

If the backfill is interrupted (Ctrl-C, machine restart, timeout), re-running the same
`run-pool.sh` command resumes safely — the tracker marks each filing individually, so completed
work is never re-processed.

---

## Monitoring

Each worker writes a timestamped log to `scripts/parallel/runs/<worker-id>/`:

```bash
# Follow a running worker
tail -f scripts/parallel/runs/worker-01/launch.log

# Pool-level log (all workers combined)
tail -f scripts/parallel/runs/pool-*.log | tail -1

# Check for errors across all workers
grep -r "ERROR\|FAILED\|Exception" scripts/parallel/runs/*/launch.log
```

The pool runner prints a live status line every 10 seconds showing running workers, memory
committed, and last activity from each worker's log.

---

## Tuning Concurrency

By default, `run-pool.sh` fills the pool based on the available memory budget (total RAM minus
1.5 GB OS reserve). To adjust:

```bash
# Hard cap at 4 concurrent workers
./run-pool.sh -j 4 2-17,24-39

# Reserve more memory for OS (useful on machines with many other processes)
./run-pool.sh -r 4000 2-17,24-39

# Run entity-level parallelism within each worker (4 threads per worker)
./run-pool.sh -p 4 2-17,24-39

# 90-minute inactivity timeout (default is 60 minutes)
./run-pool.sh -t 90 2-17,24-39
```

See [cyber-maintenance.md](cyber-maintenance.md) for cyber-specific operational details.
