#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

# Tee all output to a timestamped log file while preserving terminal output
POOL_LOG_DIR="$SCRIPT_DIR/runs"
mkdir -p "$POOL_LOG_DIR"
POOL_LOG="$POOL_LOG_DIR/pool-$(date '+%Y%m%d-%H%M%S').log"
exec > >(tee -a "$POOL_LOG") 2>&1
echo "Pool log: $POOL_LOG"

# Ignore SIGPIPE: when Ctrl+C kills tee (above), subsequent writes to stdout
# would otherwise raise SIGPIPE and kill bash before the INT trap can run.
trap '' PIPE

MAX_WORKERS=99       # Effectively unlimited — memory budget is the real constraint
TIMEOUT_MINS=60
OS_RESERVE_MB=1500   # Memory reserved for OS, kernel buffers, and non-ETL processes
# Per-worker native footprint the heap budget can't see: each ETL JVM also holds
# the in-process DuckDB working set (DUCKDB_MEMORY_LIMIT, default 2GB) + NIO/S3A
# direct buffers (MaxDirectMemorySize 768m) + metaspace (256m). Admission must
# count this on top of -Xmx or it over-admits and the box pages (free→~180MB at
# 8 workers). Live jcmd sampling showed actual native ≈0.9-2.1GB/worker (avg
# ~1.5GB) under the old 1500m direct cap; with direct now 768m it drops further,
# so 2048 covers typical + peak headroom without the over-conservatism of the
# 3200MB absolute ceiling. Tunable via env.
WORKER_NATIVE_MB="${WORKER_NATIVE_MB:-2048}"
PARALLEL_THREADS=0   # 0 = not set (default sequential); >1 = parallel entity threads

# Parse flags
while [ $# -gt 0 ]; do
  case "${1:-}" in
    -j)
      if [ -z "${2:-}" ] || ! [[ "$2" =~ ^[0-9]+$ ]]; then
        echo "ERROR: -j requires a numeric argument" >&2; exit 1
      fi
      MAX_WORKERS=$2; shift 2 ;;
    -t)
      if [ -z "${2:-}" ] || ! [[ "$2" =~ ^[0-9]+$ ]]; then
        echo "ERROR: -t requires a numeric argument (minutes)" >&2; exit 1
      fi
      TIMEOUT_MINS=$2; shift 2 ;;
    -r)
      if [ -z "${2:-}" ] || ! [[ "$2" =~ ^[0-9]+$ ]]; then
        echo "ERROR: -r requires a numeric argument (MB reserved for OS)" >&2; exit 1
      fi
      OS_RESERVE_MB=$2; shift 2 ;;
    -p|--parallel)
      if [ -z "${2:-}" ] || ! [[ "$2" =~ ^[0-9]+$ ]]; then
        echo "ERROR: -p requires a numeric argument (parallel entity threads)" >&2; exit 1
      fi
      PARALLEL_THREADS=$2; shift 2 ;;
    --force)
      export FORCE=true; shift ;;
    --schema)
      if [ -z "${2:-}" ]; then
        echo "ERROR: --schema requires a schema name" >&2; exit 1
      fi
      SCHEMA_FILTER=$2; shift 2 ;;
    *) break ;;
  esac
done

SCHEMA_FILTER="${SCHEMA_FILTER:-}"
RUN_EMBEDDINGS=false

TIMEOUT_SECS=$((TIMEOUT_MINS * 60))

# Export flags so worker scripts pass them to EtlRunner
if [ "$PARALLEL_THREADS" -gt 0 ]; then
  export ETL_PARALLEL_THREADS=$PARALLEL_THREADS
fi

if [ $# -eq 0 ]; then
  echo "Usage: $0 [-j max_concurrent] [-t timeout_mins] [-r os_reserve_mb] [-p threads] [--schema name] <alias|schema:mode...>"
  echo "  $0 daily                   — all recurring workers"
  echo "  $0 historical              — all initial/backfill workers (run once)"
  echo "  $0 all                     — everything (historical + daily)"
  echo "  $0 --schema fec daily      — run only fec from the daily set"
  echo "  $0 sec_primary:current     — single job"
  echo "  $0 sec_primary:2025 fec:daily econ:daily   — explicit list"
  echo "  $0 -j 4 daily              — hard cap at 4 concurrent"
  echo "  $0 -p 4 historical         — 4 parallel entity threads per worker"
  echo "  $0 --force daily           — bypass release-window checks (backfill/testing)"
  echo ""
  echo "  Aliases:"
  echo "    daily      — recurring workers: one SEC year (current), all non-SEC schemas (daily mode)"
  echo "    historical — backfill workers: all SEC years (2010→current), all schemas (historical/initial)"
  echo "    all        — union of historical + daily"
  echo ""
  echo "  Valid schemas: sec_primary, sec_secondary, sec_13f, sec_prices, sec, econ, census, geo, crime, weather,"
  echo "                 ref, fec, fedregister, econ_reference, cyber_threat, cyber_vuln, health, edu, energy, patents, lands, cftc, ag, disasters"
  echo ""
  echo "  DQ aliases (only schemas with *_dq.sql scripts):"
  echo "    dq         — DQ checks only for all 17 DQ schemas (data must already be in R2)  [ag: PENDING until first ETL run]"
  echo "    dq-rebuild — full ETL re-ingest + DQ for all 17 DQ schemas (memory-managed)"
  exit 1
fi

# Build queue of "schema:mode" slots
queue=()

# Compact every queued schema's tracker SERIALLY before the pool fans out. SIGKILL/OOM bypass
# the JVM shutdown-hook compaction, stranding per-combo markers as individual files in the
# current-year partition; over killed runs they accumulate into tens of thousands of tiny files
# that make every tracker read download the whole pile (e.g. crime: 65k files, 38s/read). The
# per-worker compaction (worker-dq-run.sh) would clear them too, but it runs once PER worker —
# i.e. up to -j in parallel, each a JVM — which is the memory spike we are avoiding. Doing it
# once, serially, up front keeps the footprint to a single JVM. On by default; --no-compact-first
# skips it.
COMPACT_FIRST=1

# Helper: append historical SEC primary year slots (current year - 1 down to 2010).
# The current year is daily's slot — historical backfills completed years only, so
# the two never collide on the same year demarcation.
_add_sec_primary_years() {
  local cy
  cy=$(date +%Y)
  local y=$((cy - 1))
  while [ "$y" -ge 2010 ]; do
    queue+=("sec_primary:${y}")
    y=$((y - 1))
  done
}

# Helper: append historical SEC secondary year slots (current year - 1 down to 2010).
_add_sec_secondary_years() {
  local cy
  cy=$(date +%Y)
  local y=$((cy - 1))
  while [ "$y" -ge 2010 ]; do
    queue+=("sec_secondary:${y}")
    y=$((y - 1))
  done
}

# Helper: append historical SEC 13F year slots (separate parse-heavy slot; current year - 1 down to 2010).
_add_sec_13f_years() {
  local cy
  cy=$(date +%Y)
  local y=$((cy - 1))
  while [ "$y" -ge 2010 ]; do
    queue+=("sec_13f:${y}")
    y=$((y - 1))
  done
}

for arg in "$@"; do
  case "$arg" in

    all)
      # All workers: full historical pass + all recurring modes
      _add_sec_primary_years
      queue+=(econ:historical census:historical geo:historical crime:historical weather:historical)
      _add_sec_secondary_years
      _add_sec_13f_years
      queue+=(sec_prices:historical ref:daily fec:historical fedregister:historical)
      queue+=(cyber_vuln:historical cyber_threat:historical cyber_vuln:daily cyber_threat:daily)
      queue+=(health:historical health:daily)
      queue+=(edu:historical edu:daily)
      queue+=(energy:historical energy:daily)
      queue+=(patents:historical patents:daily lands:historical lands:daily cftc:historical cftc:daily)
      queue+=(ag:historical ag:daily)
      queue+=(disasters:historical disasters:daily)
      queue+=(econ_reference:daily)
      ;;

    historical)
      # Initial/backfill workers — run once on the ingest device.
      # Year-major: process every year-partitioned schema for one year (start=end=Y),
      # newest completed year down to 2010, before moving to the older year — so a full
      # year's data lands together instead of one schema's whole range at a time.
      export GOVDATA_RUN_MODE="historical"
      _cy=$(date +%Y)
      # Specialty schemas that don't partition by calendar year — run once.
      # Daily-only schemas are intentionally NOT here — they run only in the daily window:
      #   • ref, econ_reference — current-snapshot reference; no point-in-time history to backfill,
      #     freshness-gated in their YAML so daily re-ingests only when the source changed.
      #   • sec_prices — single bulk feed + top-up; its worker ignores mode and always loads the
      #     full range, so a historical slot would just duplicate the daily run.
      # cyber_threat is NOT here — all its tables are current-snapshot/delta feeds with no year
      # axis (daily-only). cyber_vuln:historical backfills only its NVD publish-dated tables in a
      # single windowed pass (NVD resolver spans the full pub-year range), so it isn't sliced
      # per-year here; per-year cyber would need worker-cyber.sh to accept a year (follow-up).
      queue+=(cyber_vuln:historical)
      # lands has NO year axis for its FIA/static tables — the download is the full {state}_CSV.zip
      # archive (inventory_year is a column). Slicing those per-year re-downloads all ~51 state
      # archives on every year slot, so ingest them ONCE here; the per-year lands:${_y} slots below
      # cover only its year-partitioned tables (timber_sales, nps_visitation, onrr_revenues).
      queue+=(lands:once)
      # ag runs as a single historical slot (not year-sliced): ers_farm_income is a
      # no-year full-CSV fetch that would re-download on every per-year slot; the
      # NASS/RMA/FSA year-partitioned tables backfill their full range in one pass.
      queue+=(ag:historical)
      # disasters runs as a single historical slot: its year-partitioned FEMA/NOAA tables
      # backfill their full range in one pass (year dimension iterates 2010→cy-1), and the
      # static wildfire_perimeters snapshot ingests once — avoiding the per-year re-fetch of
      # the 39k-feature WFIGS layer that year-slicing would cause.
      queue+=(disasters:historical)
      # Year loop (current year is daily's slot, so start at cy-1).
      _y=$((_cy - 1))
      while [ "$_y" -ge 2010 ]; do
        queue+=("sec_primary:${_y}" "sec_secondary:${_y}" "sec_13f:${_y}")
        queue+=("econ:${_y}" "census:${_y}" "geo:${_y}" "crime:${_y}" "weather:${_y}" "energy:${_y}")
        queue+=("fec:${_y}" "fedregister:${_y}" "cftc:${_y}")
        queue+=("health:${_y}" "edu:${_y}" "patents:${_y}" "lands:${_y}")
        _y=$((_y - 1))
      done
      ;;

    dq)
      # DQ-only: run DuckDB checks against existing R2 data (no ETL).
      # Constrained to the 17 schemas that have *_dq.sql scripts.
      queue+=(
        sec:dq weather:dq edu:dq census:dq econ:dq crime:dq geo:dq
        fec:dq fedregister:dq lands:dq health:dq patents:dq ref:dq
        energy:dq econ_reference:dq cyber_threat:dq cyber_vuln:dq
        cftc:dq disasters:dq
      )
      ;;

    dq-rebuild)
      # Full re-ingest + DQ: ETL rebuild followed by DQ for each schema.
      # Memory-managed by run-pool.sh heap budget (same as historical ETL).
      export GOVDATA_RUN_MODE="historical"
      queue+=(
        sec:dq-rebuild sec_secondary:dq-rebuild sec_prices:dq-rebuild weather:dq-rebuild edu:dq-rebuild census:dq-rebuild econ:dq-rebuild
        crime:dq-rebuild geo:dq-rebuild fec:dq-rebuild fedregister:dq-rebuild
        lands:dq-rebuild health:dq-rebuild patents:dq-rebuild ref:dq-rebuild
        energy:dq-rebuild econ_reference:dq-rebuild cyber_threat:dq-rebuild cyber_vuln:dq-rebuild
        cftc:dq-rebuild disasters:dq-rebuild
      )
      ;;

    dq-etl-resume)
      # ETL resume + DQ: continue ETL (tracker skips completed partitions), then DQ.
      # No teardown — useful for iterating on ETL fixes without paying full rebuild cost.
      export GOVDATA_RUN_MODE="historical"
      queue+=(
        sec:dq-etl-resume sec_secondary:dq-etl-resume sec_prices:dq-etl-resume weather:dq-etl-resume edu:dq-etl-resume census:dq-etl-resume econ:dq-etl-resume
        crime:dq-etl-resume geo:dq-etl-resume fec:dq-etl-resume fedregister:dq-etl-resume
        lands:dq-etl-resume health:dq-etl-resume patents:dq-etl-resume ref:dq-etl-resume
        energy:dq-etl-resume econ_reference:dq-etl-resume cyber_threat:dq-etl-resume cyber_vuln:dq-etl-resume
        cftc:dq-etl-resume disasters:dq-etl-resume
      )
      ;;

    daily)
      # Recurring workers — run every day on the production server.
      export GOVDATA_RUN_MODE="daily"
      _cy=$(date +%Y)
      queue+=(
        "sec_primary:${_cy}"
        econ:daily census:daily geo:daily crime:daily weather:daily
        "sec_secondary:${_cy}"
        "sec_13f:${_cy}"
        "sec_prices:daily"
        ref:daily
        fec:daily fedregister:daily
        cyber_vuln:daily cyber_threat:daily
        health:daily
        edu:daily
        energy:daily
        patents:daily lands:daily cftc:daily
        ag:daily
        disasters:daily
        econ_reference:daily
      )
      [ -z "$SCHEMA_FILTER" ] && RUN_EMBEDDINGS=true
      ;;

    --compact-first)
      COMPACT_FIRST=1
      ;;

    --no-compact-first)
      COMPACT_FIRST=0
      ;;

    *:*)
      # Explicit "schema:mode" slot
      queue+=("$arg")
      ;;

    *)
      echo "ERROR: invalid argument '$arg'" >&2
      echo "  Use an alias (daily, historical, all) or 'schema:mode' (e.g. fec:daily)" >&2
      exit 1
      ;;
  esac
done

# Apply --schema filter: keep only slots whose schema matches
if [ -n "$SCHEMA_FILTER" ]; then
  filtered=()
  for slot in "${queue[@]}"; do
    if [ "${slot%%:*}" = "$SCHEMA_FILTER" ]; then
      filtered+=("$slot")
    fi
  done
  log_info "Schema filter '$SCHEMA_FILTER': ${#queue[@]} → ${#filtered[@]} slots (${filtered[*]:-none})"
  queue=("${filtered[@]+"${filtered[@]}"}")
fi

# Serial pre-pool tracker compaction (default on; --no-compact-first to skip). For each UNIQUE
# queued schema, compact the FULL tracker range — year=0 (table-complete/freshness markers) plus
# every data year 2000..current — because SIGKILL/OOM strand individual markers in whichever year
# was active (per-year for backfill schemas like census, the current year for daily ones). The
# floor is 2000, NOT the publish start (2010): dataLag stores markers under effective_year =
# year - dataLag, so census ACS markers live under year=2007/2008/2009 — below 2010, and missed
# entirely by a 2010 floor. A historical-mode model with START_YEAR=2000 and
# INCREMENTAL_START_YEAR=current+1 yields startYear=2000, endYear=current so compactYearRange covers
# them all. Runs EtlRunner --compact-only one JVM at a time. Reuses
# generate_single_schema_model (handles every schema, incl. DQ-only ones); the per-schema tracker
# bucket is namespaced, so each scan touches only its own schema. Best-effort: a failure is logged
# and never blocks the pool.
compact_trackers_first() {
  local seen=" " slot schema model log_file compacted=0 failed=0 jar _cy
  # Compaction merges tiny S3 marker files; it is meaningless for the Postgres backend (a real DB
  # with upserts — no marker files). EtlRunner --compact-only already no-ops for pg, but only after
  # spawning one JVM per schema. Skip the whole serial sweep so a pg pool run does not waste ~1 JVM
  # startup per schema on every launch. Also disables the per-worker pre-ETL compaction.
  case "${CALCITE_TRACKER_BACKEND:-s3}" in
    pg|postgres)
      log_info "Pre-pool tracker compaction: skipped (tracker backend '${CALCITE_TRACKER_BACKEND}' has no marker files)"
      export GOVDATA_DQ_SKIP_PRECOMPACT=true
      return 0
      ;;
  esac
  jar=$(resolve_classpath)
  _cy=$(date +%Y)
  # In a DQ context (run-all-dq sourced .env.dq) the plain model build does not point the tracker
  # at the -dq bucket — set it here, mirroring worker-dq-run.sh. Outside DQ these stay unset and
  # the model falls back to the prod buckets.
  if [ -n "${GOVDATA_DQ_TRACKER_BUCKET:-}" ]; then
    export CALCITE_TRACKER_S3_BUCKET="s3://${GOVDATA_DQ_TRACKER_BUCKET}"
    [ -n "${GOVDATA_DQ_BUCKET:-}" ] && export GOVDATA_PARQUET_DIR="s3://${GOVDATA_DQ_BUCKET}"
  fi
  log_info "Pre-pool tracker compaction: sweeping schemas serially (year=0 + 2000..${_cy})…"
  for slot in "${queue[@]}"; do
    schema="${slot%%:*}"
    # sec_primary/sec_secondary/sec_13f/sec_prices are workload splits of the single "sec"
    # schema sharing one "sec" tracker — collapse them to "sec" so the tracker is compacted
    # exactly once (not 4x, each a heavy 38k-file merge that can OOM) with a model that
    # generate_single_schema_model actually recognizes.
    case "$schema" in sec_*) schema="sec" ;; esac
    case "$seen" in *" $schema "*) continue ;; esac
    seen="$seen$schema "
    model=$(mktemp "/tmp/compact-${schema}-XXXXXX.json" 2>/dev/null) \
      || { log_info "  skip $schema (mktemp failed)"; failed=$((failed + 1)); continue; }
    log_file="$SCRIPT_DIR/runs/compact-${schema}.log"
    if GOVDATA_RUN_MODE=historical GOVDATA_START_YEAR=2000 GOVDATA_INCREMENTAL_START_YEAR=$((_cy + 1)) \
        generate_single_schema_model "$schema" "$model" 2>/dev/null; then
      log_info "  compacting tracker: $schema -> $log_file"
      if java -cp "$jar" org.apache.calcite.adapter.govdata.etl.EtlRunner \
          --compact-only --model "$model" > "$log_file" 2>&1; then
        compacted=$((compacted + 1))
      else
        failed=$((failed + 1))
        log_info "  WARN: compaction for $schema failed (non-fatal) — see $log_file"
      fi
    else
      failed=$((failed + 1))
      log_info "  WARN: model generation for $schema failed — skipping"
    fi
    rm -f "$model" 2>/dev/null || true
  done
  # The per-worker PRE-ETL compaction (worker-dq-run.sh) is now redundant and would re-introduce
  # the parallel memory spike this serial sweep exists to avoid — skip it. The per-worker POST-ETL
  # compaction still runs, draining whatever each worker writes during this run.
  export GOVDATA_DQ_SKIP_PRECOMPACT=true
  log_info "Pre-pool compaction done: $compacted compacted, $failed failed"
}

if [ "$COMPACT_FIRST" = "1" ] && [ "${#queue[@]}" -gt 0 ]; then
  compact_trackers_first
fi

# No pool-level "completed" checkpoint. The pool always runs every queued slot; --etl-resume
# (the ETL tracker + Iceberg self-heal) decides what to skip WITHIN each schema. A separate
# session-completion list duplicated that and silently skipped whole schemas on re-invocation
# (the same broken "force"-style state, requiring --force to undo) — removed.

# Verify shadow JAR before launching
resolve_classpath > /dev/null

# Record the resolved jar's signature (mtime-size) so the monitor loop can detect a mid-run
# rebuild (build-jar restage) and gracefully recycle in-flight workers onto the new code.
POOL_JAR="$(resolve_classpath)"
POOL_JAR_SIG="$(stat -c '%Y-%s' "$POOL_JAR" 2>/dev/null || echo "")"

PID_DIR="$SCRIPT_DIR/runs/pids"
mkdir -p "$PID_DIR"

# All cleanup output goes to /dev/tty (direct terminal) AND the pool log.
# This is necessary because Ctrl+C kills tee (which is in the same foreground
# process group as run-pool.sh). After tee dies, stdout is a broken pipe; any
# echo to stdout would raise SIGPIPE and kill bash before the kills run.
_cleanup_log() {
  local msg="[CLEANUP] $*"
  echo "$msg" >> "$POOL_LOG" 2>/dev/null || true
  echo "$msg" > /dev/tty 2>/dev/null || true
}

# Kill all processes associated with a worker.
# On Linux (setsid available): $pid is the session leader; pkill -s kills by SID.
# On macOS (no setsid): $pid is the bash -c wrapper child; use pkill -P (by
# parent PID) to reach the actual worker subtree, then pgid kill as a backstop.
_kill_worker_session() {
  local pid=$1
  local procs pgid

  if command -v setsid >/dev/null 2>&1; then
    # Linux path: $pid is a real session leader; kill by session ID.
    procs=$(ps -s "$pid" -o pid=,comm= 2>/dev/null | tr '\n' ' ' || true)
    if [ -z "$procs" ]; then
      _cleanup_log "  session $pid: no processes found (already dead or invalid SID)"
      return
    fi
    _cleanup_log "  session $pid: killing PIDs — $procs"
    pkill -TERM -s "$pid" 2>/dev/null || true
    sleep 2
    procs=$(ps -s "$pid" -o pid=,comm= 2>/dev/null | tr '\n' ' ' || true)
    if [ -n "$procs" ]; then
      _cleanup_log "  session $pid: still alive after TERM, sending KILL — $procs"
      pkill -KILL -s "$pid" 2>/dev/null || true
    else
      _cleanup_log "  session $pid: all processes terminated after TERM"
    fi
  else
    # macOS path: no setsid, no new session. $pid is the bash -c wrapper PID.
    # Kill descendants by parent PID, then the wrapper itself, then its pgid.
    procs=$(ps -o pid=,comm= -p "$pid" 2>/dev/null | tr '\n' ' ' || true)
    procs+=$(pgrep -P "$pid" 2>/dev/null | xargs -r ps -o pid=,comm= -p 2>/dev/null | tr '\n' ' ' || true)
    _cleanup_log "  worker $pid (macOS): killing — ${procs:-<none found>}"

    pkill -TERM -P "$pid" 2>/dev/null || true   # children of the wrapper
    kill -TERM "$pid" 2>/dev/null || true        # the wrapper itself

    pgid=$(ps -o pgid= -p "$pid" 2>/dev/null | tr -d ' ' || true)
    if [ -n "$pgid" ] && [ "$pgid" != "0" ]; then
      kill -TERM -- -"$pgid" 2>/dev/null || true  # whole process group
    fi

    sleep 2

    procs=$(ps -o pid=,comm= -p "$pid" 2>/dev/null | tr '\n' ' ' || true)
    procs+=$(pgrep -P "$pid" 2>/dev/null | tr '\n' ' ' || true)
    if [ -n "${procs// /}" ]; then
      _cleanup_log "  worker $pid (macOS): still alive after TERM, sending KILL"
      pkill -KILL -P "$pid" 2>/dev/null || true
      kill -KILL "$pid" 2>/dev/null || true
      [ -n "$pgid" ] && [ "$pgid" != "0" ] && kill -KILL -- -"$pgid" 2>/dev/null || true
    else
      _cleanup_log "  worker $pid (macOS): all processes terminated after TERM"
    fi
  fi
}

# On Ctrl-C or SIGTERM, kill all active workers before exiting
cleanup() {
  _cleanup_log "=== INT/TERM trapped — killing ${#active_pids[@]} active worker(s) ==="
  _cleanup_log "  active_pids=(${active_pids[*]:-<none>})"
  _cleanup_log "  active_labels=(${active_labels[*]:-<none>})"
  local _i
  for _i in "${!active_pids[@]}"; do
    local pid="${active_pids[$_i]}"
    local id="${active_labels[$_i]}"
    _cleanup_log "  Killing $id (session leader PID=$pid)"
    _kill_worker_session "$pid"
  done
  _cleanup_log "=== All workers terminated ==="
  exit 130
}
trap cleanup INT TERM

total=${#queue[@]}
if [ "$(uname)" = "Darwin" ]; then
  total_mem_mb=$(( $(sysctl -n hw.memsize) / 1024 / 1024 ))
else
  total_mem_mb=$(awk '/MemTotal/ {print int($2/1024)}' /proc/meminfo)
fi
budget_mb=$((total_mem_mb - OS_RESERVE_MB))
echo "=== Pool Runner: $total workers, ${total_mem_mb}MB total, ${OS_RESERVE_MB}MB reserved, ${budget_mb}MB budget ==="
if [ "$MAX_WORKERS" -lt 99 ]; then
  echo "    Hard cap: max $MAX_WORKERS concurrent"
fi
echo "    Timeout: ${TIMEOUT_MINS}min inactivity (default; large schemas use per-schema override)"
echo ""

# Active worker tracking: parallel arrays
active_pids=()
active_labels=()
active_starts=()
active_slots=()         # "schema:mode" for each active worker (used for re-queuing)
active_heap_mb=()       # Max heap in MB for each active worker
active_timeout_secs=()  # Per-worker idle timeout in seconds
committed_mb=0          # Sum of max heaps of all active workers

# Counters
queue_idx=0
done_count=0
failed_count=0
failed_list=()
restart_count=0

# Convert a heap size string (e.g., "3g", "2048m") to MB
heap_to_mb() {
  local val=$1
  val=$(echo "$val" | tr '[:upper:]' '[:lower:]')
  if [[ "$val" =~ ^([0-9]+)g$ ]]; then
    echo $(( ${BASH_REMATCH[1]} * 1024 ))
  elif [[ "$val" =~ ^([0-9]+)m$ ]]; then
    echo "${BASH_REMATCH[1]}"
  else
    echo "3072"  # Fallback: assume 3g
  fi
}

# Get max heap in MB for a schema:mode slot
get_worker_heap_mb() {
  local slot="$1"
  local schema="${slot%%:*}"
  local mode="${slot#*:}"
  local id="worker-${schema}-${mode}"
  local _HEAP_MIN _HEAP_MAX
  get_heap_config "$id"
  heap_to_mb "$_HEAP_MAX"
}

# Launch a single schema:mode slot, append to active arrays
launch_worker() {
  local slot="$1"
  local schema="${slot%%:*}"
  local mode="${slot#*:}"
  local id="worker-${schema}-${mode}"
  local script="$SCRIPT_DIR/worker.sh"

  if [ ! -f "$script" ]; then
    echo "ERROR: $script not found" >&2
    return 1
  fi

  local heap_mb
  heap_mb=$(get_worker_heap_mb "$slot")

  local log_dir="$SCRIPT_DIR/runs/${id}"
  local launch_ts
  launch_ts=$(date +%Y%m%d_%H%M%S)
  local log_file="$log_dir/launch_${launch_ts}.log"
  local pid_file="$PID_DIR/${id}.pid"
  local exit_file="$PID_DIR/${id}.exit"
  mkdir -p "$log_dir"
  rm -f "$pid_file" "$exit_file"
  # Point launch.log at the current run so monitoring tools always read the
  # latest invocation only (prevents stale-content-across-runs confusion).
  ln -sfn "launch_${launch_ts}.log" "$log_dir/launch.log"

  # Launch in a new session so workers survive terminal disconnect.
  # The wrapper writes its own $$ before exec'ing so cleanup() gets the real
  # PGID — setsid may fork internally when the caller is a session leader,
  # making $! the dead parent rather than the actual worker.
  if command -v setsid >/dev/null 2>&1; then
    setsid bash -c '
      echo $$ > "$1"
      nohup bash "$2" "$3" "$4" >> "$5" 2>&1
      echo $? > "$6"
    ' -- "$pid_file" "$script" "$schema" "$mode" "$log_file" "$exit_file" &
  else
    # macOS / no setsid: use bash -c so $$ is the child's own PID (BASHPID is bash 4+ only).
    # No nohup here — without setsid there is no new session, so nohup only prevents
    # _kill_worker_session from collecting the orphan via pgid/parent-PID kill.
    bash -c '
      echo $$ > "$1"
      bash "$2" "$3" "$4" >> "$5" 2>&1
      echo $? > "$6"
    ' -- "$pid_file" "$script" "$schema" "$mode" "$log_file" "$exit_file" &
  fi

  # Wait up to 2s for the worker to write its actual PID
  local pid="" _i=0
  while [ -z "$pid" ] && [ "$_i" -lt 20 ]; do
    sleep 0.1
    pid=$(cat "$pid_file" 2>/dev/null | head -1 | tr -d '[:space:]' || true)
    [[ "$pid" =~ ^[0-9]+$ ]] || pid=""
    _i=$((_i + 1))
  done
  if [ -z "$pid" ]; then
    log_info "WARNING: could not read PID for $id — falling back to \$!"
    pid=$!
  fi

  local timeout_mins timeout_secs
  timeout_mins=$(get_timeout_config "$id")
  timeout_secs=$((timeout_mins * 60))

  active_pids+=("$pid")
  active_labels+=("$id")
  active_starts+=("$(date +%s)")
  active_slots+=("$slot")
  # Budget on full footprint (heap + native), not -Xmx alone. active_heap_mb holds
  # the footprint so remove_active credits the same amount back on exit.
  local foot_mb=$((heap_mb + WORKER_NATIVE_MB))
  active_heap_mb+=("$foot_mb")
  active_timeout_secs+=("$timeout_secs")
  committed_mb=$((committed_mb + foot_mb))

  log_info "Launched $id (PID $pid, heap ${heap_mb}MB +${WORKER_NATIVE_MB}MB native = ${foot_mb}MB, timeout ${timeout_mins}min) — committed: ${committed_mb}MB / ${budget_mb}MB budget"
  return 0
}

# Check available system memory in MB
get_available_mb() {
  if [ "$(uname)" = "Darwin" ]; then
    local page_size=$(sysctl -n hw.pagesize)
    local free_pages=$(vm_stat | awk '/Pages free/ {gsub(/\./,"",$3); print $3}')
    local inactive_pages=$(vm_stat | awk '/Pages inactive/ {gsub(/\./,"",$3); print $3}')
    echo $(( (free_pages + inactive_pages) * page_size / 1024 / 1024 ))
  else
    awk '/MemAvailable/ {print int($2/1024)}' /proc/meminfo
  fi
}

# Fill the pool up to MAX_WORKERS, respecting the memory budget.
fill_pool() {
  local scan_idx=$queue_idx
  while [ "${#active_pids[@]}" -lt "$MAX_WORKERS" ] && [ "$scan_idx" -lt "$total" ]; do
    local next_slot="${queue[$scan_idx]}"
    local next_heap_mb next_foot_mb
    next_heap_mb=$(get_worker_heap_mb "$next_slot")
    next_foot_mb=$((next_heap_mb + WORKER_NATIVE_MB))   # heap + native footprint
    local next_schema="${next_slot%%:*}"
    local next_mode="${next_slot#*:}"
    local next_id="worker-${next_schema}-${next_mode}"

    # Skip if this worker's footprint exceeds total budget — can never run on this machine
    if [ "$next_foot_mb" -gt "$budget_mb" ]; then
      log_info "SKIPPING ${next_id}: needs ${next_foot_mb}MB (heap ${next_heap_mb}MB + ${WORKER_NATIVE_MB}MB native) but budget is only ${budget_mb}MB"
      ((done_count++)) || true
      ((failed_count++)) || true
      if [ "$scan_idx" -eq "$queue_idx" ]; then
        ((queue_idx++)) || true
      fi
      ((scan_idx++)) || true
      continue
    fi

    # Check 1: committed budget (full footprint, not -Xmx alone)
    local projected=$((committed_mb + next_foot_mb))
    if [ "$projected" -gt "$budget_mb" ]; then
      log_info "Memory budget: ${next_id} needs ${next_foot_mb}MB, committed=${committed_mb}MB, budget=${budget_mb}MB — holding"
      break
    fi

    # Check 2: actual available memory (belt + suspenders, skip when no workers active)
    if [ "${#active_pids[@]}" -gt 0 ]; then
      local avail_mb
      avail_mb=$(get_available_mb)
      if [ "$avail_mb" -lt "$((next_foot_mb + OS_RESERVE_MB / 2))" ]; then
        log_info "Memory pressure: ${avail_mb}MB available, ${next_id} needs ${next_foot_mb}MB — holding"
        break
      fi
    fi

    queue_idx=$scan_idx
    launch_worker "${queue[$queue_idx]}" || true
    ((queue_idx++)) || true
    scan_idx=$queue_idx
  done
}

# Remove a finished worker from active arrays by index
remove_active() {
  local idx=$1
  committed_mb=$((committed_mb - active_heap_mb[$idx]))
  if [ "$committed_mb" -lt 0 ]; then committed_mb=0; fi

  local new_pids=() new_labels=() new_starts=() new_slots=() new_heaps=() new_timeouts=()
  for i in "${!active_pids[@]}"; do
    if [ "$i" -ne "$idx" ]; then
      new_pids+=("${active_pids[$i]}")
      new_labels+=("${active_labels[$i]}")
      new_starts+=("${active_starts[$i]}")
      new_slots+=("${active_slots[$i]}")
      new_heaps+=("${active_heap_mb[$i]}")
      new_timeouts+=("${active_timeout_secs[$i]}")
    fi
  done
  active_pids=("${new_pids[@]+"${new_pids[@]}"}")
  active_labels=("${new_labels[@]+"${new_labels[@]}"}")
  active_starts=("${new_starts[@]+"${new_starts[@]}"}")
  active_slots=("${new_slots[@]+"${new_slots[@]}"}")
  active_heap_mb=("${new_heaps[@]+"${new_heaps[@]}"}")
  active_timeout_secs=("${new_timeouts[@]+"${new_timeouts[@]}"}")
}

# Kill a stuck worker and re-queue its slot
kill_and_requeue() {
  local idx=$1
  local pid="${active_pids[$idx]}"
  local id="${active_labels[$idx]}"
  local slot="${active_slots[$idx]}"
  local elapsed_mins=$(( ($(date +%s) - active_starts[$idx]) / 60 ))
  # Report the worker's ACTUAL per-schema idle limit (active_timeout_secs), not the global
  # default — otherwise every restart line reads "60m limit" even for 240/360m schemas,
  # which masks whether a kill was a too-short timeout vs a genuine long idle/hang.
  local timeout_mins=$(( ${active_timeout_secs[$idx]:-${TIMEOUT_SECS:-3600}} / 60 ))

  log_info "$id idle ${elapsed_mins}m (limit ${timeout_mins}m) — killing session $pid and re-queuing"
  _kill_worker_session "$pid"

  remove_active "$idx"
  ((restart_count++)) || true

  queue+=("$slot")
  ((total++)) || true
}

# Initial fill
fill_pool

# Monitor loop
while [ "${#active_pids[@]}" -gt 0 ] || [ "$queue_idx" -lt "$total" ]; do
  # Check for finished workers
  i=0
  while [ "$i" -lt "${#active_pids[@]}" ]; do
    pid="${active_pids[$i]}"
    id="${active_labels[$i]}"
    start="${active_starts[$i]}"

    # A worker is finished once its wrapper writes the .exit file (its last action) — that is
    # authoritative. Do NOT rely on `kill -0 $pid` alone: the tracked pid is the wrapper's $$,
    # which dies immediately after writing .exit, and on a busy host that pid is quickly reused by
    # another process. A reused pid keeps `kill -0` succeeding, so the pool would show a long-since
    # finished worker as "running" forever (and never terminate after all schemas complete). The
    # .exit file is removed before each launch, so its presence always reflects the current run.
    if [ -f "$PID_DIR/${id}.exit" ] || ! kill -0 "$pid" 2>/dev/null; then
      now=$(date +%s)
      elapsed=$(( now - start ))
      mins=$((elapsed / 60))

      # Use exit code file rather than wait() — the worker may not be a direct
      # child of this shell when setsid forked internally.
      # NOTE: no 'local' here — 'local' is invalid outside a function and causes
      # bash to exit under set -e, silently killing the pool.
      exit_code=$(cat "$PID_DIR/${id}.exit" 2>/dev/null | head -1 | tr -d '[:space:]' || true)
      [[ "$exit_code" =~ ^[0-9]+$ ]] || exit_code=1

      if [ "$exit_code" -eq 0 ]; then
        ((done_count++)) || true
        log_info "$id finished OK (${mins}m)"
      else
        ((failed_count++)) || true
        failed_list+=("$id")
        log_info "$id FAILED (${mins}m): check launch.log for details"
      fi

      remove_active "$i"
      continue
    fi
    ((i++)) || true
  done

  # Detect a rebuilt jar (build-jar restage) and gracefully recycle in-flight workers onto it.
  # _kill_worker_session sends SIGTERM to each worker's session first, so the worker JVM's
  # LocalStagingStorageProvider shutdown hook flushes its staged batches before exit; the slot
  # is then re-queued and relaunched reading the new (unversioned) jar bytes. Counts as a
  # restart, not a failure.
  if [ -n "${POOL_JAR_SIG:-}" ] && [ "${#active_pids[@]}" -gt 0 ]; then
    cur_jar_sig=$(stat -c '%Y-%s' "$POOL_JAR" 2>/dev/null || echo "$POOL_JAR_SIG")
    if [ "$cur_jar_sig" != "$POOL_JAR_SIG" ]; then
      log_info "JAR CHANGED ($POOL_JAR) — gracefully recycling ${#active_pids[@]} in-flight worker(s) onto new code"
      POOL_JAR_SIG="$cur_jar_sig"
      for (( ji=${#active_pids[@]}-1; ji>=0; ji-- )); do
        kill_and_requeue "$ji"
      done
    fi
  fi

  # Check for stuck workers
  i=0
  while [ "$i" -lt "${#active_pids[@]}" ]; do
    id="${active_labels[$i]}"
    now=$(date +%s)
    worker_timeout_secs="${active_timeout_secs[$i]:-$TIMEOUT_SECS}"
    uptime_secs=$(( now - active_starts[$i] ))
    if [ "$uptime_secs" -lt "$worker_timeout_secs" ]; then
      ((i++)) || true
      continue
    fi
    log_file="$SCRIPT_DIR/runs/${id}/launch.log"
    if [ -f "$log_file" ]; then
      last_modified=$(stat -c '%Y' "$log_file" 2>/dev/null || echo "$now")
      idle_secs=$(( now - last_modified ))
    else
      idle_secs=$uptime_secs
    fi
    if [ "$idle_secs" -ge "$worker_timeout_secs" ]; then
      kill_and_requeue "$i"
      continue
    fi
    ((i++)) || true
  done

  # Fill any open slots
  fill_pool

  # Status line
  remaining=$((total - done_count - failed_count - ${#active_pids[@]}))
  active_str=""
  if [ "${#active_labels[@]}" -gt 0 ]; then
    active_str="| ${active_labels[*]}"
  fi
  mem_avail=$(get_available_mb)
  printf "\n[%s] Running: %d  Done: %d  Failed: %d  Queued: %d  Restarts: %d  Heap: %s/%sMB  Free: %sMB  %s\n" \
    "$(date '+%H:%M:%S')" "${#active_pids[@]}" "$done_count" "$failed_count" "$remaining" "$restart_count" \
    "$committed_mb" "$budget_mb" "$mem_avail" "$active_str"

  # Per-worker detail
  now=$(date +%s)
  for idx in "${!active_pids[@]}"; do
    id="${active_labels[$idx]}"
    elapsed=$(( now - active_starts[$idx] ))
    hrs=$((elapsed / 3600))
    mins=$(( (elapsed % 3600) / 60 ))
    secs=$((elapsed % 60))
    if [ "$hrs" -gt 0 ]; then
      elapsed_str="${hrs}h${mins}m"
    elif [ "$mins" -gt 0 ]; then
      elapsed_str="${mins}m${secs}s"
    else
      elapsed_str="${secs}s"
    fi

    log_file="$SCRIPT_DIR/runs/${id}/launch.log"
    activity=""
    if [ -f "$log_file" ]; then
      activity=$(grep -E " (INFO|WARN|ERROR) " "$log_file" 2>/dev/null | tail -1 | sed 's/^.*INFO  [^ ]* - //; s/^.*WARN  [^ ]* - //; s/^.*ERROR [^ ]* - //; s/^\[[0-9-]* [0-9:]*\] //' | cut -c1-120 || true)
    fi
    printf "  %-28s [%s] %s\n" "$id" "$elapsed_str" "${activity:-starting...}"
  done

  sleep 10
done

echo ""
echo ""
echo "=== Pool Complete ==="
echo "Total: $total  Done: $done_count  Failed: $failed_count  Restarts: $restart_count"
if [ "$failed_count" -gt 0 ]; then
  echo "Failed workers: ${failed_list[*]}"
  exit 1
fi

# ── Embeddings (daily only) ───────────────────────────────────────────────────
# Best-effort post-ETL step, now LOCAL + CPU (vss-local.sh) — the Vultr remote path
# (vss-gpu-runner.sh) is retired. A failure here must NOT poison the pool exit code:
# otherwise run-scheduled.sh reads the non-zero exit as an ETL crash and restarts
# daily forever instead of moving on to the historical fill. Log loudly, never abort.
if $RUN_EMBEDDINGS; then
  VSS_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
  # Delta-driven, time-boxed: embed the un-embedded backlog across all years (newest
  # first), capped by VSS_MAX_SECONDS (~2h) / VSS_MAX_ROWS. Drains over successive days.
  log_info "Embeddings (local CPU): coding un-coded backlog (newest-first, time-boxed) → lake"
  if [ -f "$VSS_DIR/vss-local.sh" ]; then
    bash "$VSS_DIR/vss-local.sh" backlog \
      || log_info "WARNING: vss-local backlog failed (non-fatal)"
  else
    log_info "WARNING: vss-local.sh not found — embeddings skipped"
  fi
  log_info "Embeddings: complete"
fi

exit 0
