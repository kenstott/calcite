# GovData Pipeline Operations Guide

The pipeline is fully automated. The only routine human action required is a daily review of
`runs/errors.log` to catch any worker failures.

---

## Normal operation: the perpetual runner

`run-scheduled.sh` is the production process. It runs continuously, cycling between `historical`
and `daily` run-pool windows. Each window runs for up to 12 hours. If run-pool crashes mid-window,
the runner waits 30 seconds and relaunches it for the remaining window time. When the window
ends the mode flips and the next window starts immediately.

Mode selection when no argument given: hours 08:00–19:59 → `daily`; 20:00–07:59 → `historical`.

**Install as a systemd user service (Linux production):**

```bash
systemd/install.sh                   # install and enable govdata-pool.service
systemd/install.sh --uninstall
```

```bash
systemctl --user status govdata-pool.service
systemctl --user stop govdata-pool.service
```

The service starts at login and restarts automatically on failure.

**Daily error review:**

```bash
tail -f runs/errors.log              # live errors
tail runs/errors.log                 # last N lines
grep "$(date +%Y-%m-%d)" runs/errors.log   # today only
```

**Logs:**
- Crash/error lines: `runs/errors.log`
- Per-window detail: `runs/scheduled-{mode}-{timestamp}.log`
- Pool output: `runs/pool-service.log` (when running via systemd)

**Direct launch (foreground, for testing only):**

```bash
./run-scheduled.sh            # auto-select mode by hour
./run-scheduled.sh daily
./run-scheduled.sh historical
```

---

## Separate scheduled jobs (TBD)

Two jobs run on their own schedules outside the perpetual runner. Both are deferred — schedules
and tooling are not yet finalized.

**Stock prices** (`sec_prices`) — fetches Stooq prices for all EDGAR tickers. Always does a
full 2010–present scan; takes 1–3 days. Whether this becomes part of the offering at all is
still under evaluation.

```bash
./run-pool.sh sec_prices:historical   # run manually for now
```

**Embeddings / VSS cache** — generates SEC filing embeddings and uploads the DuckDB HNSW index.
Likely runs weekly on a Vultr CPU instance. Schedule and automation not yet defined.
See [Embeddings & VSS Cache](#embeddings--vss-cache) for the manual pipeline.

**Cron reference (if not using systemd):**

```cron
# Perpetual runner — manages historical/daily cycling automatically
@reboot  cd /path/to/govdata/scripts/parallel && ./run-scheduled.sh
```

---

## Manual operations (run-pool.sh)

`run-pool.sh` is the underlying worker pool. The perpetual runner calls it continuously;
use it directly for one-off runs, schema-specific reruns, or troubleshooting.

```bash
./run-pool.sh daily               # all recurring schemas
./run-pool.sh historical          # all initial/backfill schemas (run once on first setup)

# Run one schema only
./run-pool.sh --schema fec daily
./run-pool.sh --schema sec_primary historical

# Explicit schema:mode slots
./run-pool.sh fec:daily econ:daily
./run-pool.sh sec_primary:2024

# Flags
./run-pool.sh -j 4 daily          # hard cap at 4 concurrent workers
./run-pool.sh -t 120 historical   # extend inactivity timeout to 120 min
./run-pool.sh -r 4000 daily       # reserve 4GB more for OS (useful when other processes compete)
./run-pool.sh -p 4 daily          # 4 entity-level parallel threads per worker
./run-pool.sh --force daily       # bypass release-window checks (backfill / manual refresh)
```

Valid schemas: `sec_primary`, `sec_secondary`, `sec_prices`, `econ`, `census`, `geo`, `crime`,
`weather`, `ref`, `fec`, `fedregister`, `econ_reference`, `cyber_threat`, `cyber_vuln`,
`health`, `edu`, `energy`, `patents`, `lands`

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

| Schema / Mode | What it loads | Heap | Est. runtime |
|---|---|---|---|
| sec_primary:current | SEC primary 2026+ | 3g | 1–3 h |
| econ:historical | Economic (BLS/FRED/BEA) | 3g | 3–6 h |
| census:historical | Census ACS 2010–2026 | 3g | 1–3 h |
| geo:historical | Geographic (TIGER shapefiles) | 6g | 3–8 h |
| crime:historical | Crime (FBI/BJS) | 4g | 4–10 h |
| weather:historical | Weather (NWS/NOAA/EPA) | 3g | 2–5 h |
| sec_secondary:current | SEC secondary 2026+ | 3g | 1–4 h |
| sec_prices:historical | Stock prices 2010–2026 | 3g | 1–3 days ⚠️ |
| ref:daily | Reference (GLEIF/CIK/FIGI) | 4g | 1–2 h |
| fec:historical | FEC campaign finance | 5g | 4–8 h |
| fedregister:historical | Federal Register 2010+ | 3g | 3–6 h |
| cyber_vuln:initial | NVD full + KEV + CWE catalog | 6g | 2–5 h |
| health:initial | All 15 tables, full fetch | 6g | 3–8 h |
| edu:initial | CCD, IPEDS, NAEP, CRDC, Scorecard | 6g | 2–6 h |
| sec_primary:historical (×16 years) | SEC primary per year | 3g | 2–8 h each |
| sec_secondary:historical (×16 years) | SEC secondary per year | 3g | 1–4 h each |

With 4 concurrent workers, the 12 Phase 1 workers form roughly 3 batches → **~1 day**.
The 32 historical SEC workers running 4 at a time → **3–5 days**.
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

# ref:daily first — GLEIF/CIK/OpenFIGI identifiers used by SEC cross-references
# sec_primary:current, sec_secondary:current — SEC primary and secondary for 2026+
# Workers are listed in priority order; pool fills slots as they complete
./run-pool.sh ref:daily sec_primary:current sec_secondary:current
```

Expected wall time: **2–6 hours** (sec_primary:current and sec_secondary:current run
concurrently once ref:daily finishes or a slot opens; all three fit within 10g combined heap).

### Step 2 — Non-SEC schemas + cyber + health (run while Step 1 is finishing or immediately after)

These are independent of SEC and cover their full historical ranges in a single run —
they are not year-sharded. Ordered so fast workers fill early slots and heavy-heap workers
(geo, fec, cyber_vuln:initial, health:initial) land last.

```bash
./run-pool.sh econ:historical census:historical geo:historical crime:historical weather:historical fec:historical fedregister:historical cyber_vuln:initial cyber_threat:initial health:initial edu:initial energy:initial lands:historical
```

| Schema / Mode | What it covers | Est. runtime | Note |
|---|---|---|---|
| census:historical | Census ACS | 1–3 h | Fast; full 2010–2026 in one run |
| weather:historical | Weather | 2–5 h | Moderate |
| fedregister:historical | Federal Register | 3–6 h | Moderate |
| econ:historical | Economic | 3–6 h | BLS/FRED/BEA, many API calls |
| crime:historical | Crime | 4–10 h | Large dimension expansion (4g heap) |
| cyber_vuln:initial | Cyber vulnerabilities | 2–5 h | Full NVD catalog; faster with `CYBER_NVD_API_KEY` |
| cyber_threat:initial | Cyber threats | 2–5 h | NIST/CIS/OWASP standards + OTX backfill |
| health:initial | Health | 3–8 h | All 15 health tables; clinical trials cursor pagination |
| edu:initial | Education | 2–6 h | CCD, IPEDS, NAEP, CRDC, College Scorecard |
| fec:historical | FEC | 4–8 h | 3M+ rows/year (5g heap) |
| geo:historical | Geographic | 3–8 h | TIGER shapefiles (6g heap); placed last as it's the heaviest |

Expected wall time: **~1–2 days**. After Steps 1 and 2 the pipeline is fully queryable with
SEC coverage from 2026 onward and all non-SEC schemas populated.

### Step 3 — SEC primary historical backfill (2025–2010)

SEC primary covers one calendar year of 10-K/10-Q filings per slot. Run most-recent-first so
recent history is available as early as possible.

```bash
./run-pool.sh --schema sec_primary historical
```

The `--schema` filter keeps only `sec_primary:*` slots from the historical queue. Expected
wall time: **~3–5 days** on a 32GB machine with 4 concurrent slots. Safe to restart — the
tracker marks each filing individually and skips completed work.

### Step 4 — SEC secondary historical backfill (2025–2010)

SEC secondary covers one calendar year each of 8-K, proxy, insider, and 13F filings. Run
after primary because secondary volume per year is substantially higher.

```bash
./run-pool.sh --schema sec_secondary historical
```

Expected wall time: **several days**; 8-K/proxy/13F filing counts dwarf 10-K/10-Q volumes.
Safe to restart at any point.

### Step 5 — Stock prices (run last, alone)

The `sec_prices:historical` worker fetches daily prices for every ticker in `_ALL_EDGAR_FILERS` (thousands
of symbols) from Stooq, which enforces strict per-request rate limits. Run it alone so it does
not compete with other workers for pool slots or network bandwidth.

```bash
./run-pool.sh sec_prices:historical
```

The full 2010–2026 backfill takes **1–3 days** of throttled requests. The tracker persists
per-ticker completion state, so stopping and restarting is safe — it resumes from where it
left off. Stock prices enrich SEC cross-references but are not required for any other schema
to function.

### Summary timeline (32GB machine, 4 concurrent workers)

```
Day 0   ./run-pool.sh historical
        ├── ref:daily, sec_primary:current, sec_secondary:current complete in ~6h → 2026 SEC is queryable
        └── All other historical workers running in background via memory-aware pool
Day 1   Non-SEC schemas complete → all schemas queryable
        └── Run GPU embeddings for 2026: vss-gpu-runner.sh && vss.sh refresh 2026 && vss.sh upload
Day 4   SEC primary historical finishes → full primary SEC 2010–2026 complete
        └── Run full embedding backfill: vss-gpu-runner.sh && vss-rebuild-full.sh && vss.sh upload
Day 8   SEC secondary historical finishes → full secondary SEC 2010–2026 complete
Day 8   ./run-pool.sh sec_prices:historical      (runs alone; takes 1–3 more days)
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

Schedule and automation are not yet defined — run manually for now. Likely cadence is weekly.

```bash
cd scripts

# Incremental: embed current year only, then upload
./vss-gpu-runner.sh && ./vss.sh refresh 2026 && ./vss.sh upload

# Embed a specific year
VSS_YEARS=2025 ./vss-gpu-runner.sh && ./vss.sh refresh 2025 && ./vss.sh upload
```

---

## Recurring Updates

Once the initial load is complete, three jobs run on separate schedules on the production server:

**1. Daily ETL (all schemas except stock prices)**
```bash
cd scripts/parallel
./run-pool.sh daily
```
All recurring workers run in a single memory-aware pool. Workers skip already-materialized rows;
each schema handles its own data-lag window internally.

**2. Stock prices (separate schedule)**

Stock prices run on their own cron entry because Stooq rate limits make them too slow to share
pool slots with other workers. A full run can take 1–3 days; the tracker deduplicates per-ticker
so stopping and restarting is safe.

```bash
./run-pool.sh sec_prices:historical
```

**3. Embeddings / VSS cache (separate schedule)**

The embeddings pipeline runs independently of the ETL pool — see the [Embeddings & VSS Cache](#embeddings--vss-cache) section for commands and timing.

### Re-running a single schema

```bash
./run-pool.sh --schema energy daily       # energy workers only
./run-pool.sh --schema sec_primary daily   # SEC primary current-year only
./run-pool.sh --schema health daily       # health recurring workers only
```

### Re-ingesting from scratch (data-fix.sh)

Use `data-fix.sh` when a schema or table has corrupt, missing, or stale data that a normal daily run won't fix. It deletes R2 data via `rclone purge`, invalidates the tracker, then re-ingests via run-pool.

    ./data-fix.sh <schema> <mode>                   # all tables in schema
    ./data-fix.sh <schema> <mode> <table>           # one table
    ./data-fix.sh <schema> <mode> <t1> <t2> ...     # multiple tables
    ./data-fix.sh fec daily --dry-run               # preview without deleting
    ./data-fix.sh weather daily --force             # bypass release-window checks

Both R2 data and the tracker must be cleared — clearing the tracker alone is insufficient because the ETL detects existing parquet and marks it complete without re-downloading.

### Running data quality checks (dq.sh)

`dq.sh` re-ingests the DQ year window for a schema, then runs `{schema}_dq.sql` through DuckDB and writes results to `s3://govdata-tracker-v1/dq-results/`.

    dq.sh --schema fec                        # current year + 1 prior year
    dq.sh --schema crime --lookback 2         # override year window
    dq.sh --schema edu --no-reingest          # re-run SQL only, skip data-fix
    dq.sh --schema econ --dry-run

Default lookback by schema: `census`=5, `edu`=4, `crime`=2, `fec`=2, all others=1. The lookback can also be set in the SQL file header with `-- dq-lookback: N`.

View results:
    dq-report.sh --schemas fec --run-date 2026-05-22
    run-all-dq.sh                             # run DQ for all schemas

### Bypassing release-window checks

Recurring workers (edu, energy, health, cyber_threat, cyber_vuln) gate sub-runs to their
source's known release window — a sub-run outside its window exits in milliseconds with no
network I/O. Pass `--force` to bypass all window checks and run every sub-run regardless of
today's date. Year bounds (`GOVDATA_SINCE_YEAR`, `startYear`, `endYear`) are unaffected by
`--force`.

```bash
./run-pool.sh --force daily                        # all recurring workers, no window checks
./run-pool.sh --force --schema edu daily           # edu only, no window checks
./run-pool.sh --force --schema energy daily        # energy only, no window checks
./run-pool.sh --force --schema health daily        # health only, no window checks
./run-pool.sh --force --schema cyber_threat daily  # cyber_threat only, no window checks
./run-pool.sh --force --schema cyber_vuln daily    # cyber_vuln only, no window checks
```

`--force` propagates automatically to all worker scripts via the environment — no changes
to individual worker invocations are needed.

### Cron reference

In production the preferred approach is the systemd perpetual runner (`run-scheduled.sh`) — it handles crash recovery and mode cycling automatically. If using cron instead:

```cron
# Daily ETL — all schemas (stock prices excluded)
0 6 * * *   cd /path/to/govdata/scripts/parallel && ./run-pool.sh daily

# Stock prices and embeddings — schedules TBD; run manually for now
```

---

## Schema-by-schema Update Cadence

All schemas in the table below run as part of `./run-pool.sh daily`. Each worker skips
already-complete rows, so running daily is always safe regardless of the source's release
cadence. Stock prices are the only exception — run via `./run-pool.sh sec_prices:historical`.

| Schema | Source cadence | Mechanism |
|---|---|---|
| SEC primary filings | Daily | Re-runs 2026–present window; skips already-materialized accessions |
| SEC secondary filings | Daily | Same as above for 8-K/proxy/insider/13F |
| SEC stock prices | Daily (run alone) | Full 2010–2026 re-run via Stooq; rate-limited per ticker; tracker deduplicates |
| Economic (BLS/FRED/BEA) | Daily (data lags weekly) | Full re-run; incremental by series/period |
| Census ACS | Daily (data lags annually) | Full re-run; ACS releases annually |
| Geographic (TIGER/HUD) | Daily (data lags annually) | TIGER year is pinned; picks up new vintage when published |
| Crime (FBI/BJS) | Daily (data lags ~12 months) | Full re-run; FBI releases lag ~12 months |
| Weather (NWS/NOAA/EPA) | Daily | Full re-run; picks up new observation periods |
| Reference (GLEIF/CIK/FIGI) | Daily | GLEIF discovers current golden copy URL on each run |
| FEC campaign finance | Daily | Bulk file re-download; FEC updates files in-place |
| Federal Register | Daily | Auto-discovers current year; append-only |
| cyber_vuln (NVD/KEV delta) | Daily | 1-day delta window via NVD API `lastModStartDate` |
| cyber_vuln (CWE/OSV/cross-refs) | Weekly | CWE/OSV/advisories; skips if unchanged |
| cyber_threat (ATT&CK/NIST) | Weekly | Full re-fetch; sources publish infrequently; skips if unchanged |
| cyber_threat (IOC feeds) | Hourly | URLhaus/MalwareBazaar/Feodo/ThreatFox/OTX fresh each run |
| cyber_threat (static standards) | On-demand | NIST 800-53, NIST CSF, CIS Controls, OWASP Top 10 |
| Health clinical trials | Daily | `lastUpdatePostDate.gte` filter via `GOVDATA_SINCE_DATE` |
| Health CDC COVID/mortality | Daily (data lags weekly) | CDC vaccinations delta + mortality full refresh |
| Health BRFSS/Medicaid/CMS/FDA | Daily (data lags monthly) | Stable reference tables; incremental via `GOVDATA_SINCE_DATE`/`GOVDATA_SINCE_YEAR` |
| Education CCD/IPEDS/Scorecard | Daily (data lags annually) | Bulk releases; incremental via `GOVDATA_SINCE_YEAR` |
| Education NAEP/CRDC | Daily (data lags biennially) | Returns unchanged data in off-cycle years; safe to run daily |
| Energy electricity/refinery | Daily (data lags weekly/monthly) | EIA API; skips years with no new data |
| Energy surveys/coal | Daily (data lags annually) | EIA-861/EIA-860/MSHA; skips if no new release |
| Lands visitation/revenues | Daily (data lags 1–3 months) | NPS IRMA XML + ONRR bulk CSV; other tables gated by release window |

---

## Monitoring

Each worker writes a timestamped log to `scripts/parallel/runs/<worker-id>/`:

```bash
# Follow a specific worker (log dir is worker-{schema}-{mode})
tail -f scripts/parallel/runs/worker-fec-daily/launch.log

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
./run-pool.sh -j 4 historical

# Reserve more memory for OS (useful when other processes are running)
./run-pool.sh -r 4000 historical

# Entity-level parallelism within each worker (trades memory for speed)
./run-pool.sh -p 4 historical

# Extend inactivity timeout for very large workers (default: 60 min)
./run-pool.sh -t 120 historical
```

See [cyber-maintenance.md](cyber-maintenance.md) for cyber-specific operational details.
See [health-maintenance.md](health-maintenance.md) for health-specific operational details.
See [edu-maintenance.md](edu-maintenance.md) for education-specific operational details.
See [energy-maintenance.md](energy-maintenance.md) for energy-specific operational details.
See [lands-maintenance.md](lands-maintenance.md) for lands-specific operational details.
