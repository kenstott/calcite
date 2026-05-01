# GovData Pipeline Operations Guide

This guide explains how to set up and maintain the full GovData pipeline across all 12 schemas.
The core orchestrator is `scripts/parallel/run-pool.sh`, which manages memory-aware concurrent
execution of numbered worker scripts on a single machine.

---

## Worker Map

```
SEC Primary (10-K/10-Q)         workers  1 – 17    (worker-01 = 2026+, workers 02–17 = 2025–2010)
SEC Secondary (8-K/Proxy/etc.)  workers 23 – 39    (worker-23 = 2026+, workers 24–39 = 2025–2010)
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
CYBER_NVD_API_KEY=...                      # nvd.nist.gov/developers (5× faster NVD downloads)
CYBER_OTX_API_KEY=...                      # otx.alienvault.com
CYBER_THREATFOX_API_KEY=...
CYBER_GITHUB_TOKEN=ghp_...
CYBER_OSV_ECOSYSTEMS=PyPI,npm,Go,Maven
BLS_API_KEY=...
FRED_API_KEY=...
```

---

## Realistic Timing on a Single Machine

The pool is memory-bounded, not core-bounded. On a typical 32GB machine the OS reserve leaves
roughly 30GB budget. Worker heap sizes range from 3g to 6g, so 4–6 workers run concurrently.
On 16GB, expect 3 concurrent; on 64GB, 8–10.

Approximate per-worker runtimes (order of magnitude, varies significantly with network and I/O):

| Worker | What it loads | Heap | Est. runtime |
|---|---|---|---|
| 1 | SEC primary 2026+ | 3g | 1–3 h |
| 18 | Economic (BLS/FRED/BEA) | 3g | 3–6 h |
| 19 | Census ACS 2010–2026 | 3g | 1–3 h |
| 20 | Geographic (TIGER shapefiles) | 6g | 3–8 h |
| 21 | Crime (FBI/BJS) | 4g | 4–10 h |
| 22 | Weather (NWS/NOAA/EPA) | 3g | 2–5 h |
| 23 | SEC secondary 2026+ | 3g | 1–4 h |
| 40 | Stock prices 2010–2026 | 3g | 2–4 h |
| 41 | Reference (GLEIF/CIK/FIGI) | 4g | 1–2 h |
| 60 | FEC campaign finance | 5g | 4–8 h |
| 61 | Federal Register 2010+ | 3g | 3–6 h |
| 62 | Cyber initial (NVD full + standards + OTX) | 6g | 2–5 h |
| 2–17 | SEC primary per year (×16) | 3g | 2–8 h each |
| 24–39 | SEC secondary per year (×16) | 3g | 1–4 h each |

With 4 concurrent workers, the 12 Phase 1 workers form roughly 3 batches → **~1 day**.
The 32 historical SEC workers (2–17, 24–39) running 4 at a time → **3–5 days**.
Total wall-clock time for a complete load from scratch: **4–6 days**.

---

## Initial Setup Strategy

The SEC historical corpus (2010–2025) takes several days to fully materialize. The recommended
approach loads data in priority order so the pipeline is queryable within ~24 hours, with
historical depth filling in afterward.

### Step 1 — Highest priority: current-year SEC + reference data (queryable within hours)

These are fast workers that unlock the rest of the pipeline: company identifiers (ref) are
needed to cross-reference SEC filings, and current-year filings (2026+) are the most
immediately useful data.

```bash
cd scripts/parallel

# Worker 41 (ref) first — GLEIF/CIK/OpenFIGI identifiers used by SEC cross-references
# Workers 1, 23 — SEC primary and secondary for 2026+
# Workers are listed in priority order; pool fills slots as they complete
./run-pool.sh 41,1,23
```

Expected wall time: **2–6 hours** (workers 1 and 23 run concurrently once 41 finishes
or a slot opens; all three fit within 10g combined heap).

### Step 2 — Non-SEC schemas + cyber (run while Step 1 is finishing or immediately after)

These are independent of SEC and can run concurrently with each other. They cover full
historical ranges in a single run — they are not year-sharded.

Ordered by value / speed (fastest and highest-value first):

```bash
# Run in this order so the pool fills priority workers into early slots.
# Slow, large-heap workers (20, 60, 62) are placed last so they don't block
# fast workers from starting.
./run-pool.sh 19,22,40,61,18,21,62,60,20
```

| Worker | Schema | Est. runtime | Note |
|---|---|---|---|
| 19 | Census ACS | 1–3 h | Fast; full 2010–2026 in one run |
| 22 | Weather | 2–5 h | Moderate |
| 40 | Stock prices | 2–4 h | Useful for SEC cross-referencing |
| 61 | Federal Register | 3–6 h | Moderate |
| 18 | Economic | 3–6 h | BLS/FRED/BEA, many API calls |
| 21 | Crime | 4–10 h | Large dimension expansion (4g heap) |
| 62 | Cyber initial | 2–5 h | Full NVD catalog; faster with `CYBER_NVD_API_KEY` |
| 60 | FEC | 4–8 h | 3M+ rows/year (5g heap) |
| 20 | Geographic | 3–8 h | TIGER shapefiles (6g heap); placed last as it's the heaviest |

Expected wall time for this step: **~1 day** (pool keeps 4 slots busy through all 9 workers).

After Steps 1 and 2, all schemas have data. The pipeline is fully queryable with SEC coverage
from 2026 onward. Proceed to Step 3 while queries run against current data.

### Step 3 — Historical SEC backfill (runs in background over several days)

Workers 2–17 (primary) and 24–39 (secondary) each cover one calendar year, from 2025 back
to 2010. Run with the most recent years first so recent history is available sooner.

```bash
# Recent years first: 2025, 2024, 2023 primary + secondary
./run-pool.sh 2,24,3,25,4,26

# Then 2022–2019
./run-pool.sh 5,27,6,28,7,29,8,30

# Then 2018–2015
./run-pool.sh 9,31,10,32,11,33,12,34

# Then 2014–2010
./run-pool.sh 13,35,14,36,15,37,16,38,17,39
```

Interleaving primary and secondary workers for the same year (e.g., `2,24` for 2025) means
both filing types for a year are available at roughly the same time. Each group above takes
roughly 12–24 hours on a 32GB machine with 4 concurrent slots.

Alternatively, run all backfill workers at once and let the pool manage ordering:

```bash
./run-pool.sh 2-17,24-39
```

If the backfill is interrupted, re-running the same command is safe — the tracker marks each
filing individually and skips completed work.

### Summary timeline (32GB machine, 4 concurrent workers)

```
Day 0  Launch Steps 1 + 2 together
       ├── Step 1 (41,1,23) completes in ~6h → current-year SEC is queryable
       └── Step 2 (19,22,40,61,18,21,62,60,20) running in background
Day 1  Step 2 finishes → all schemas queryable; launch Step 3 (historical SEC backfill)
Day 2  2025, 2024, 2023 SEC complete
Day 4  2022–2019 SEC complete
Day 6  Full corpus 2010–2026 complete; start recurring update schedule
```

---

## Recurring Updates

Once the initial load is complete, set up the following recurring jobs. Re-running a worker
never duplicates data — the tracker marks completed rows and skips them on subsequent runs.

### Daily (recommended: 06:00 UTC)

```bash
cd scripts/parallel

# SEC current year: picks up new filings since last run
./run-pool.sh 1,23

# Cyber: delta NVD CVEs (last 1 day) + CISA KEV refresh
./run-pool.sh 63

# Federal Register: auto-discovers current year; append-only
./run-pool.sh 61
```

### Weekly (recommended: Sunday 02:00 UTC)

```bash
cd scripts/parallel

# Non-SEC full re-runs (idempotent; tracker skips already-complete rows)
./run-pool.sh 18-22,40-41,60

# Cyber: CWE, OSV, MITRE ATT&CK techniques, GitHub advisories, ATT&CK→NIST mappings
./run-pool.sh 64
```

### Hourly (recommended: every 2 hours)

```bash
cd scripts/parallel

# Cyber live IOC feeds + OTX delta (1-day window)
CYBER_OTX_DELTA_DAYS=1 ./run-pool.sh 65
```

### Cron reference

```cron
# Daily — SEC current year + federal register + cyber CVE delta
0 6 * * *   cd /path/to/govdata/scripts/parallel && ./run-pool.sh 1,23,61 && ./run-pool.sh 63

# Weekly — non-SEC refresh + cyber ATT&CK/standards refresh
0 2 * * 0   cd /path/to/govdata/scripts/parallel && ./run-pool.sh 18-22,40-41,60,64

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

## Monitoring

Each worker writes a timestamped log to `scripts/parallel/runs/<worker-id>/`:

```bash
# Follow a specific worker
tail -f scripts/parallel/runs/worker-01/launch.log

# Pool-level log (all workers interleaved)
ls -t scripts/parallel/runs/pool-*.log | head -1 | xargs tail -f

# Check for errors across all workers
grep -r "ERROR\|FAILED\|Exception" scripts/parallel/runs/*/launch.log
```

The pool runner prints a live status line every 10 seconds showing running workers, memory
committed vs budget, and the last logged activity line from each worker.

---

## Tuning Concurrency

By default the pool fills to the available memory budget (total RAM minus 1.5GB OS reserve).

```bash
# Hard cap at 4 concurrent workers regardless of memory
./run-pool.sh -j 4 2-17,24-39

# Reserve more memory for OS (useful when other processes are running)
./run-pool.sh -r 4000 2-17,24-39

# Entity-level parallelism within each worker (trades memory for speed)
./run-pool.sh -p 4 2-17,24-39

# Extend inactivity timeout for very large workers (default: 60 min)
./run-pool.sh -t 120 20,60,62
```

See [cyber-maintenance.md](cyber-maintenance.md) for cyber-specific operational details.
