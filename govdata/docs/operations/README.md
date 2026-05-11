# GovData Pipeline Operations Guide

This guide explains how to set up and maintain the full GovData pipeline across all 12 schemas.
The core orchestrator is `scripts/parallel/run-pool.sh`, which manages memory-aware concurrent
execution of numbered worker scripts on a single machine.

---

## ETL Orchestrator

`scripts/parallel/run-pool.sh` is the single entry point for all ETL operations.
It manages memory-aware concurrent worker execution and accepts named aliases:

```bash
cd scripts/parallel

./run-pool.sh daily               # Run all recurring workers + embeddings refresh (production server)
./run-pool.sh historical          # Run all initial/backfill workers (ingest device, run once)
./run-pool.sh stock-quotes        # Stock prices alone (Stooq rate limits; may take 1–3 days)

# Flags (combinable with any alias or range)
./run-pool.sh -j 4 daily          # Hard cap at 4 concurrent workers regardless of memory budget
                                  # Use when memory isn't the constraint: API rate limits, CPU
                                  # contention, shared machines, or cleaner sequential logs.
                                  # Prefer -r when the goal is tighter OOM protection.
./run-pool.sh -t 120 historical   # Extend inactivity timeout to 120 min
./run-pool.sh -r 4000 daily       # Reserve 4GB for OS so the budget is more conservative
                                  # Use when heap estimates understate real RSS (off-heap buffers,
                                  # native libs) or when other processes compete for memory.
                                  # Prefer -j when concurrency itself is the concern, not memory.
./run-pool.sh -p 4 daily          # 4 entity-level parallel threads per worker
./run-pool.sh --force daily       # Bypass all release-window checks (backfill / manual refresh)

# Schema filter — run one domain from the daily or historical set
./run-pool.sh --schema energy daily
./run-pool.sh --schema sec historical
./run-pool.sh --schema health daily
# Known schemas: sec, sec_secondary, stock, econ, census, geo, crime,
#                weather, ref, fec, fedregister, cyber, health, edu, energy

# Combine flags freely
./run-pool.sh --force --schema edu daily     # edu workers only, skip release-window checks
./run-pool.sh --force --schema energy daily  # energy workers only, skip release-window checks
```

## Worker Map

```
SEC Primary (10-K/10-Q)         workers  1 – 17    (worker-01 = 2026+, workers 02–17 = 2025–2010)
SEC Secondary (8-K/Proxy/etc.)  workers 23 – 39    (worker-23 = 2026+, workers 24–39 = 2025–2010)
SEC Stock Prices                worker  40
Non-SEC schemas                 workers 18 – 22, 41, 60, 61
Cyber                           workers 62 – 66    (worker-62 = initial, 63-66 = recurring)
Health                          workers 67 – 70    (worker-67 = initial, 68-70 = recurring)
Education                       workers 71 – 73    (worker-71 = initial, 72-73 = recurring)
Energy                          workers 74 – 77    (worker-74 = initial, 75-77 = recurring)
Patents                         worker  80 – 81    (worker-80 = initial, 81 = recurring)
Lands (public lands)            workers 82 – 83    (worker-82 = initial, 83 = recurring)
```

The `daily` alias covers workers: 1, 18–23, 40, 41, 60–61, 63–65, 68–70, 72–73, 75–77, 81, 83
The `historical` alias covers workers: 1–41, 60–62, 67, 71, 74, 80, 82

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

HEALTH_PARQUET_DIR=/data/health          # or s3://your-bucket/govdata/source=health
HEALTH_CACHE_DIR=/data/health-cache

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
| 40 | Stock prices 2010–2026 | 3g | 1–3 days ⚠️ |
| 41 | Reference (GLEIF/CIK/FIGI) | 4g | 1–2 h |
| 60 | FEC campaign finance | 5g | 4–8 h |
| 61 | Federal Register 2010+ | 3g | 3–6 h |
| 62 | Cyber initial (NVD full + standards + OTX) | 6g | 2–5 h |
| 67 | Health initial (all 15 tables, full fetch) | 6g | 3–8 h |
| 71 | Education initial (CCD, IPEDS, NAEP, CRDC, Scorecard) | 6g | 2–6 h |
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

### Step 2 — Non-SEC schemas + cyber + health (run while Step 1 is finishing or immediately after)

These are independent of SEC and cover their full historical ranges in a single run —
they are not year-sharded. Ordered so fast workers fill early slots and heavy-heap workers
(20, 60, 62, 67) land last.

```bash
./run-pool.sh 19,22,61,18,21,62,67,71,60,20
```

| Worker | Schema | Est. runtime | Note |
|---|---|---|---|
| 19 | Census ACS | 1–3 h | Fast; full 2010–2026 in one run |
| 22 | Weather | 2–5 h | Moderate |
| 61 | Federal Register | 3–6 h | Moderate |
| 18 | Economic | 3–6 h | BLS/FRED/BEA, many API calls |
| 21 | Crime | 4–10 h | Large dimension expansion (4g heap) |
| 62 | Cyber initial | 2–5 h | Full NVD catalog; faster with `CYBER_NVD_API_KEY` |
| 67 | Health initial | 3–8 h | All 15 health tables; clinical trials cursor pagination |
| 71 | Education initial | 2–6 h | CCD, IPEDS, NAEP, CRDC, College Scorecard; set `COLLEGE_SCORECARD_API_KEY` for scorecard tables |
| 60 | FEC | 4–8 h | 3M+ rows/year (5g heap) |
| 20 | Geographic | 3–8 h | TIGER shapefiles (6g heap); placed last as it's the heaviest |

Expected wall time: **~1–2 days**. After Steps 1 and 2 the pipeline is fully queryable with
SEC coverage from 2026 onward and all non-SEC schemas populated.

### Step 3 — SEC primary historical backfill (2025–2010)

Workers 2–17 each cover one calendar year of 10-K/10-Q filings. Run most-recent-first so
recent history is available as early as possible. Primary filings are the analytical backbone
(financial_line_items, balance sheets) and are completed before secondary to ensure a fully
usable SEC corpus exists.

```bash
./run-pool.sh 2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
```

Expected wall time: **~3–5 days** on a 32GB machine with 4 concurrent slots. Safe to restart
— the tracker marks each filing individually and skips completed work.

### Step 4 — SEC secondary historical backfill (2025–2010)

Workers 24–39 cover one calendar year each of 8-K, proxy, insider, and 13F filings. These
run after primary because secondary volume per year is substantially higher, and interleaving
with primary would cause slow secondary workers to stall primary-year progress.

```bash
./run-pool.sh 24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39
```

Expected wall time: **several days**; 8-K/proxy/13F filing counts dwarf 10-K/10-Q volumes.
Safe to restart at any point.

### Step 5 — Stock prices (run last, alone)

Worker 40 fetches daily prices for every ticker in `_ALL_EDGAR_FILERS` (thousands of symbols)
from Stooq, which enforces strict per-request rate limits. Run it alone so it does not compete
with other workers for pool slots or network bandwidth.

```bash
./run-pool.sh 40
```

The full 2010–2026 backfill takes **1–3 days** of throttled requests. The tracker persists
per-ticker completion state, so stopping and restarting is safe — it resumes from where it
left off. Stock prices enrich SEC cross-references but are not required for any other schema
to function.

### Summary timeline (32GB machine, 4 concurrent workers)

```
Day 0   ./run-pool.sh historical
        ├── Workers 41,1,23 complete in ~6h → 2026 SEC is queryable
        └── All other historical workers running in background via memory-aware pool
Day 1   Non-SEC schemas complete → all schemas queryable
        └── Run GPU embeddings for 2026: vss-gpu-runner.sh && vss.sh refresh 2026 && vss.sh upload
Day 4   SEC primary historical (2–17) finishes → full primary SEC 2010–2026 complete
        └── Run full embedding backfill: vss-gpu-runner.sh && vss-rebuild-full.sh && vss.sh upload
Day 8   SEC secondary historical (24–39) finishes → full secondary SEC 2010–2026 complete
Day 8   ./run-pool.sh stock-quotes      (runs alone; takes 1–3 more days)
Day 10  Complete corpus → switch to: ./run-pool.sh daily  (run every day going forward)
```

---

## Embeddings & VSS Cache

SEC 10-K/10-Q text chunks are stored in the `vectorized_chunks` Iceberg table and embedded
using the `snowflake-arctic-embed-xs` model (384 dimensions). A DuckDB HNSW index file
(`chunks_vss.duckdb`) is then built from those embeddings and uploaded to S3/R2 for
client-side semantic search.

This pipeline runs **independently of run-pool** — it requires no pool slots, has no
memory budget impact, and should be scheduled separately.

### Three-script pipeline

```bash
cd scripts

# 1. Generate embeddings on a GPU instance (Vultr; spins up, runs, terminates automatically)
#    Reads vectorized_chunks from Iceberg, writes 384-dim embeddings back per year.
./vss-gpu-runner.sh

# 2. Build (or rebuild) the DuckDB HNSW index from Iceberg embeddings
#    Full rebuild across all years:
./vss-rebuild-full.sh
#    Per-year refresh (faster; use after a daily ETL run):
./vss.sh refresh 2026

# 3. Upload the DuckDB file to S3/R2 for client distribution
./vss.sh upload
```

### When to run during initial setup

| After step | What to run | Why |
|---|---|---|
| Day 1 — non-SEC schemas done | `vss-gpu-runner.sh && ./vss.sh refresh 2026 && ./vss.sh upload` | Makes 2026 semantic search available immediately |
| Day 4 — SEC primary historical done | `vss-gpu-runner.sh && ./vss-rebuild-full.sh && ./vss.sh upload` | Full embedding backfill for all primary-filing years |

Secondary filing years do not produce `vectorized_chunks`, so no additional embedding run
is needed after the secondary historical backfill.

### Recurring embeddings

The daily `run-pool.sh daily` command automatically runs the embeddings pipeline after the
ETL pool completes — no separate cron entry needed. To control which years are embedded:

```bash
# Default: embeds current year only
./run-pool.sh daily

# Embed a specific year
VSS_YEARS=2025 ./run-pool.sh daily

# Skip embeddings (ETL only)
./run-pool.sh daily  # embeddings are skipped if vss-gpu-runner.sh is not found
```

> **Note:** `vss-gpu-runner.sh` spins up a Vultr GPU instance and terminates it on completion.
> Daily cost is proportional to new filing volume — typically a few minutes of GPU time for
> an incremental run.

---

## Recurring Updates

Once the initial load is complete, run this every day on the production server:

```bash
cd scripts/parallel
./run-pool.sh daily
```

That's it. All recurring workers run in a single memory-aware pool. Workers skip already-
materialized rows; each schema handles its own data-lag window internally. The embeddings
pipeline runs automatically after the pool completes.

### Stock prices

Stock prices are excluded from `daily` because Stooq rate limits make sharing pool slots
with other workers wasteful. Run it separately (it can overlap with `daily` on another terminal
or cron slot):

```bash
./run-pool.sh stock-quotes
```

### Re-running a single schema

```bash
./run-pool.sh --schema energy daily       # energy workers only
./run-pool.sh --schema sec daily          # SEC current-year workers only
./run-pool.sh --schema health daily       # health recurring workers only
```

### Bypassing release-window checks

Recurring workers (edu, energy, health, cyber) gate sub-runs to their source's known release
window — a sub-run outside its window exits in milliseconds with no network I/O. Pass
`--force` to bypass all window checks and run every sub-run regardless of today's date.
Year bounds (`GOVDATA_SINCE_YEAR`, `startYear`, `endYear`) are unaffected by `--force`.

```bash
./run-pool.sh --force daily                  # all recurring workers, no window checks
./run-pool.sh --force --schema edu daily     # edu only, no window checks
./run-pool.sh --force --schema energy daily  # energy only, no window checks
./run-pool.sh --force --schema health daily  # health only, no window checks
./run-pool.sh --force --schema cyber daily   # cyber only, no window checks
```

`--force` propagates automatically to all worker scripts via the environment — no changes
to individual worker invocations are needed.

### Cron reference

```cron
# Daily — all recurring workers + embeddings
0 6 * * *   cd /path/to/govdata/scripts/parallel && ./run-pool.sh daily

# Stock prices — run alone; may take 1–3 days; safe to overlap with daily
0 7 * * *   cd /path/to/govdata/scripts/parallel && ./run-pool.sh stock-quotes
```

---

## Schema-by-schema Update Cadence

All schemas in the table below run as part of `./run-pool.sh daily`. Each worker skips
already-complete rows, so running daily is always safe regardless of the source's release
cadence. Stock prices (40) are the only exception — run via `./run-pool.sh stock-quotes`.

| Schema | Worker | Source cadence | Mechanism |
|---|---|---|---|
| SEC primary filings | 1 | Daily | Re-runs 2026–present window; skips already-materialized accessions |
| SEC secondary filings | 23 | Daily | Same as above for 8-K/proxy/insider/13F |
| SEC stock prices | 40 | Daily (run alone) | Full 2010–2026 re-run via Stooq; rate-limited per ticker; tracker deduplicates |
| Economic (BLS/FRED/BEA) | 18 | Daily (data lags weekly) | Full re-run; incremental by series/period |
| Census ACS | 19 | Daily (data lags annually) | Full re-run; ACS releases annually |
| Geographic (TIGER/HUD) | 20 | Daily (data lags annually) | TIGER year is pinned; picks up new vintage when published |
| Crime (FBI/BJS) | 21 | Daily (data lags ~12 months) | Full re-run; FBI releases lag ~12 months |
| Weather (NWS/NOAA/EPA) | 22 | Daily | Full re-run; picks up new observation periods |
| Reference (GLEIF/CIK/FIGI) | 41 | Daily | GLEIF discovers current golden copy URL on each run |
| FEC campaign finance | 60 | Daily | Bulk file re-download; FEC updates files in-place |
| Federal Register | 61 | Daily | Auto-discovers current year; append-only |
| Cyber CVE (NVD/KEV) | 63 | Daily | 1-day delta window via NVD API `lastModStartDate` |
| Cyber standards/ATT&CK | 64 | Daily (data lags weekly) | Full re-fetch; sources publish infrequently; skips if unchanged |
| Cyber IOC feeds | 65 | Daily | URLhaus/MalwareBazaar/Feodo/ThreatFox/OTX fresh each run |
| Health clinical trials | 68 | Daily | `lastUpdatePostDate.gte` filter via `GOVDATA_SINCE_DATE` |
| Health CDC COVID/mortality | 69 | Daily (data lags weekly) | CDC vaccinations delta + mortality full refresh |
| Health BRFSS/Medicaid/CMS/FDA | 70 | Daily (data lags monthly) | Stable reference tables; incremental via `GOVDATA_SINCE_DATE`/`GOVDATA_SINCE_YEAR` |
| Education CCD/IPEDS/Scorecard | 72 | Daily (data lags annually) | Bulk releases; incremental via `GOVDATA_SINCE_YEAR` |
| Education NAEP/CRDC | 73 | Daily (data lags biennially) | Returns unchanged data in off-cycle years; safe to run daily |
| Energy electricity/refinery | 75,76 | Daily (data lags weekly/monthly) | EIA API; skips years with no new data |
| Energy surveys/coal | 77 | Daily (data lags annually) | EIA-861/EIA-860/MSHA; skips if no new release |
| Lands visitation/revenues | 83 | Daily (data lags 1–3 months) | NPS IRMA XML + ONRR bulk CSV; other tables gated by release window |

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
See [health-maintenance.md](health-maintenance.md) for health-specific operational details.
See [edu-maintenance.md](edu-maintenance.md) for education-specific operational details.
See [energy-maintenance.md](energy-maintenance.md) for energy-specific operational details.
See [lands-maintenance.md](lands-maintenance.md) for lands-specific operational details.
