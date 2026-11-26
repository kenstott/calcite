# GovData ETL Operations Runbook

---

## 1. Perpetual runner (run-scheduled.sh + systemd)

`run-scheduled.sh` is the long-running production process. It never exits on its own. It alternates between `historical` and `daily` run-pool windows, each lasting up to 12 hours (`WINDOW_SECS=43200`). When a window ends the mode flips and the next window starts immediately.

Within a window the runner classifies `run-pool.sh`'s exit code:

| Exit code | Meaning | Runner action |
|-----------|---------|---------------|
| `0` | Queue drained, all schemas OK | End window (or fill remaining time with the other mode) |
| `2` | Queue drained, some workers failed | End window — **no restart**; failed schemas retry next window via the tracker |
| `124` (or `143`) | `timeout` fired — the window/attempt elapsed | End window normally. If `run-pool`'s window log had gone stale > `POOL_STALL_SECS` (300s), a `run-pool.sh appears wedged` WARNING is logged |
| anything else | Genuine crash (OOM/137, `set -e` abort) — queue unfinished | Log to `runs/errors.log`, wait `RESTART_DELAY` (30s), relaunch to resume — bounded to `MAX_POOL_RESTARTS` (5) crashes per window, then abandon the mode |

Note: `timeout` reports **124** on expiry (not 143) — the historical backfill is a ~360-slot queue that legitimately spans many windows, so a mid-backfill 124 is the normal window boundary, not a failure. Restarting on it (the old behavior, which only recognized 143) was a bug.

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

# Bypass release-window checks for every pool run this process launches
./run-scheduled.sh daily --force
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

## 3. Data fix (force-reprocess.sh, data_purge.sh)

Run these from `govdata/scripts/parallel/`.

### Reprocess tables in place

`force-reprocess.sh` re-runs the named tables past their tracker-complete state. It deletes no Iceberg data and no tracker rows. Use it first.

```bash
./force-reprocess.sh --schema econ --tables industry_gdp --dry-run
./force-reprocess.sh --schema econ --tables industry_gdp
./force-reprocess.sh --schema census --tables cps_voting_supplement --start 1996
```

Add `--force-download` when the fix changes how a response is fetched or parsed, so cached raw responses are fetched again.

### Rebuild tables from empty

`data_purge.sh` deletes the whole Iceberg table, all years, and its tracker state. It does not start the ETL.

```bash
../data_purge.sh --schema fec --tables committee_contributions --dry-run
../data_purge.sh --schema fec --tables committee_contributions
./run-pool.sh --schema fec historical
./run-pool.sh --schema fec daily
```

It deletes on the rclone remote named by `GOVDATA_RCLONE_REMOTE` in `govdata/.env.prod` (`GOVDATA_DQ_RCLONE_REMOTE` with `--env dq`), the store the ETL reads and writes. The R2 mirror is updated by `sync-to-r2.sh`. `--raw` also deletes the table's raw response cache.

---

## 4. Data quality (worker-dq-run.sh)

`worker-dq-run.sh` runs `{schema}_dq.sql` through DuckDB against `GOVDATA_DQ_BUCKET`, which it reads from `govdata/.env.dq` and which defaults to the production bucket when unset. Check the resolved bucket before a run.

DQ SQL files live in `govdata/scripts/` and are named `{schema}_dq.sql`.

### Usage

```bash
worker-dq-run.sh edu                # run DQ SQL, upload results, update the DQ issue
worker-dq-run.sh edu --dry-run      # run DQ SQL only: no upload, no DQ issue update
worker-dq-run.sh edu --etl-resume   # run unfinished ETL partitions, then DQ
worker-dq-run.sh edu --rebuild      # purge the schema in the DQ bucket, re-run ETL, then DQ
```

### Viewing results

```bash
# Report for a single schema on a specific date
dq-report.sh --schemas fec --run-date 2026-05-22

# Run DQ for all schemas (parallel workers, one per schema)
run-all-dq.sh
```

`dq-report.sh` reads from `s3://govdata-tracker-v1-dq/dq-results/` (`GOVDATA_DQ_TRACKER_BUCKET`) and prints a schema-level verdict summary followed by failing test details. Add `--show-warns` or `--show-pass` to include lower-severity rows.

A whole-schema run writes `schema=<s>/run_date=<d>/type=<mode>/results.parquet`. A `--tables` run writes `.../type=<mode>/scope=<id>/results.parquet`, where `<id>` identifies the table set, and keeps the schema's earlier results, including under `--rebuild`. `dq-report.sh` reports the whole-schema files and lists the scoped ones.
