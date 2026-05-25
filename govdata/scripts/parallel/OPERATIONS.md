# GovData ETL Operations Runbook

---

## 1. Perpetual runner (run-scheduled.sh + systemd)

`run-scheduled.sh` is the long-running production process. It never exits on its own. It alternates between `historical` and `daily` run-pool windows, each lasting up to 12 hours (`WINDOW_SECS=43200`). When a window ends the mode flips and the next window starts immediately.

Within a window, if `run-pool.sh` exits with a non-zero code (crash, OOM), the runner logs the failure to `runs/errors.log`, waits 30 seconds, then relaunches the pool for the remaining window time. A clean exit (code 0) or a timeout signal (code 143) ends the window normally.

**Mode selection.** If no argument is given, the starting mode is determined by the current hour: 08:00–19:59 → `daily`, 20:00–07:59 → `historical`. Pass `daily` or `historical` explicitly to override.

**Log files:**
- PID file: `runs/pids/scheduled.pid`
- Crash and error lines: `runs/errors.log`
- Per-window detail: `runs/scheduled-{mode}-{timestamp}.log`

### Install as a systemd user service (Linux production server)

```bash
# Install and enable
systemd/install.sh

# Remove
systemd/install.sh --uninstall
```

Post-install status and control:

```bash
systemctl --user status govdata-pool.service
systemctl --user stop govdata-pool.service
tail -f runs/pool-service.log
tail -f runs/errors.log
```

The service starts automatically at login and restarts on failure.

### Direct launch (foreground, for testing)

```bash
# Auto-select mode by hour
./run-scheduled.sh

# Force a specific starting mode
./run-scheduled.sh daily
./run-scheduled.sh historical
```

Run in the foreground to watch output directly. Send `SIGTERM` or press `Ctrl-C` to stop; the runner forwards the signal to any active pool worker before exiting.

---

## 2. One-time runs (run-pool.sh)

`run-pool.sh` runs a queue of `schema:mode` slots in parallel, then exits. It does not loop. Concurrency is bounded by available memory: each worker declares a max heap size, and the pool holds new launches until the committed heap fits within the available budget (`total_mem - 1500 MB OS reserve`). Pass `-j N` to add a hard cap on top of the memory limit.

Worker logs land in `runs/worker-{schema}-{mode}/launch.log`. The pool prints a status line every 10 seconds showing running, done, failed, and queued counts.

### Aliases

```bash
./run-pool.sh daily        # all schemas, daily mode
./run-pool.sh historical   # all schemas, historical/backfill mode
./run-pool.sh all          # union of historical + daily
```

The `daily` alias also triggers an embeddings refresh after all workers finish, unless `--schema` filtering is active.

### Schema filter

Run one schema from the standard daily or historical queue:

```bash
./run-pool.sh --schema fec daily
./run-pool.sh --schema cyber_threat daily
./run-pool.sh --schema econ historical
```

### Explicit slots

Specify exact `schema:mode` pairs to skip the alias expansion:

```bash
./run-pool.sh fec:daily econ:daily
./run-pool.sh sec_primary:2024
```

### Options

| Flag | Purpose |
|------|---------|
| `-j N` | Hard cap on concurrent workers |
| `-t N` | Inactivity timeout per worker in minutes (default 60) |
| `-p N` | Parallel entity threads per worker |
| `--force` | Bypass release-window checks (use for backfill or testing) |
| `--schema NAME` | Restrict to one schema when combined with an alias |

### Valid schemas

`sec_primary`, `sec_secondary`, `sec_prices`, `econ`, `census`, `geo`, `crime`, `weather`, `ref`, `fec`, `fedregister`, `econ_reference`, `cyber_threat`, `cyber_vuln`, `health`, `edu`, `energy`, `patents`, `lands`

---

## 3. Data fix (data-fix.sh)

Use `data-fix.sh` when a schema or table needs to be re-ingested from scratch. It performs three steps in order:

1. Deletes Iceberg/parquet data in R2 via `rclone purge`.
2. Invalidates the tracker so the ETL treats the cleared data as unseen.
3. Launches `run-pool.sh` to re-ingest.

Both the R2 data and the tracker must be cleared. Clearing the tracker alone is insufficient — the ETL detects existing parquet on R2 and marks it complete without re-downloading.

### Usage

```bash
# Re-ingest all tables in a schema
./data-fix.sh <schema> <mode>

# Re-ingest one table
./data-fix.sh <schema> <mode> <table>

# Re-ingest multiple tables
./data-fix.sh <schema> <mode> <table1> <table2>
```

### Options

| Flag | Purpose |
|------|---------|
| `--dry-run` | Print what would be deleted and run without taking action |
| `--force` | Pass `FORCE=true` to run-pool (bypasses release-window checks) |

### Examples

```bash
# Re-ingest all FEC daily tables
./data-fix.sh fec daily

# Re-ingest one specific table
./data-fix.sh crime historical crimes_by_agency

# Preview what would be deleted without changing anything
./data-fix.sh econ daily --dry-run

# Force re-ingest bypassing the release window
./data-fix.sh weather daily --force
```

### Tracker mechanics

**Single table (or list):** Sets `GOVDATA_FORCE_REPROCESS_TABLES=<table>`, which causes `GovDataSchemaFactory` to call `tracker.invalidateTableCompletion()` for each named table before the ETL run.

**All tables:** Sets `FORCE_FRESH=true`, which passes `freshStart=true` into the model operand. `GovDataSchemaFactory` calls `tracker.clearAllCompletions()` for the schema, writing a `_all/cleared` sentinel. Every subsequent tracker lookup for that schema returns null (not complete) for the duration of the run.

---

## 4. Data quality (dq.sh)

`dq.sh` runs DQ checks for one schema. It:

1. Re-ingests the DQ year window via `data-fix.sh` (current year + lookback prior years).
2. Runs `{schema}_dq.sql` through DuckDB with credential and year-bound variables expanded via `envsubst`.
3. Writes a results parquet to `s3://govdata-tracker-v1/dq-results/schema={schema}/run_date={date}/type=daily/results.parquet`.

DQ SQL files live in `govdata/scripts/` and are named `{schema}_dq.sql`. A file may declare its default lookback in a header comment:

```sql
-- dq-lookback: 2
```

If the comment is absent the lookback defaults to 1 (current year + 1 prior year).

### Usage

```bash
dq.sh --schema fec
dq.sh --schema crime --lookback 2
dq.sh --schema edu --no-reingest    # skip data-fix, re-run SQL only
dq.sh --schema econ --dry-run
```

### Lookback values by schema

| Schema | Lookback | Reason |
|--------|----------|--------|
| `census` | 5 | ACS 5-year estimates; data lags |
| `edu` | 4 | IPEDS publication lag |
| `crime` | 2 | FBI UCR reporting lag |
| `fec` | 2 | Election cycle data |
| all others | 1 | Current + 1 prior year |

### Viewing results

```bash
# Report for a single schema on a specific date
dq-report.sh --schemas fec --run-date 2026-05-22

# Run DQ for all schemas (parallel workers, one per schema)
run-all-dq.sh
```

`dq-report.sh` reads from `s3://govdata-tracker-v1/dq-results/` and prints a schema-level verdict summary followed by failing test details. Add `--show-warns` or `--show-pass` to include lower-severity rows.
