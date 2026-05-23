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
  echo "  Valid schemas: sec_primary, sec_secondary, sec_prices, econ, census, geo, crime, weather,"
  echo "                 ref, fec, fedregister, econ_reference, cyber_threat, cyber_vuln, health, edu, energy, patents, lands"
  echo ""
  echo "  DQ aliases (only schemas with *_dq.sql scripts):"
  echo "    dq         — DQ checks only for all 16 DQ schemas (data must already be in R2)"
  echo "    dq-rebuild — full ETL re-ingest + DQ for all 16 DQ schemas (memory-managed)"
  exit 1
fi

# Build queue of "schema:mode" slots
queue=()

# Helper: append all historical SEC primary year slots (current year down to 2010)
_add_sec_primary_years() {
  local cy
  cy=$(date +%Y)
  queue+=("sec_primary:${cy}")
  local y=$((cy - 1))
  while [ "$y" -ge 2010 ]; do
    queue+=("sec_primary:${y}")
    y=$((y - 1))
  done
}

# Helper: append all historical SEC secondary year slots
_add_sec_secondary_years() {
  local cy
  cy=$(date +%Y)
  queue+=("sec_secondary:${cy}")
  local y=$((cy - 1))
  while [ "$y" -ge 2010 ]; do
    queue+=("sec_secondary:${y}")
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
      queue+=(sec_prices:historical ref:daily fec:historical fedregister:historical)
      queue+=(cyber_vuln:initial cyber_threat:initial cyber_vuln:daily cyber_vuln:weekly cyber_threat:weekly cyber_threat:hourly cyber_threat:static)
      queue+=(health:initial health:daily health:weekly health:monthly)
      queue+=(edu:initial edu:annual edu:biennial)
      queue+=(energy:initial energy:weekly energy:monthly energy:annual)
      queue+=(patents:historical patents:daily lands:historical lands:daily)
      queue+=(econ_reference:daily)
      ;;

    historical)
      # Initial/backfill workers — run once on the ingest device.
      export GOVDATA_RUN_MODE="historical"
      _add_sec_primary_years
      queue+=(econ:historical census:historical geo:historical crime:historical weather:historical)
      _add_sec_secondary_years
      queue+=(sec_prices:historical ref:historical fec:historical fedregister:historical)
      queue+=(cyber_vuln:initial cyber_threat:initial health:initial edu:initial energy:initial)
      queue+=(patents:historical lands:historical)
      ;;

    dq)
      # DQ-only: run DuckDB checks against existing R2 data (no ETL).
      # Constrained to the 16 schemas that have *_dq.sql scripts.
      queue+=(
        weather:dq edu:dq census:dq econ:dq crime:dq geo:dq
        fec:dq fedregister:dq lands:dq health:dq patents:dq ref:dq
        energy:dq econ_reference:dq cyber_threat:dq cyber_vuln:dq
      )
      ;;

    dq-rebuild)
      # Full re-ingest + DQ: ETL rebuild followed by DQ for each schema.
      # Memory-managed by run-pool.sh heap budget (same as historical ETL).
      export GOVDATA_RUN_MODE="historical"
      queue+=(
        weather:dq-rebuild edu:dq-rebuild census:dq-rebuild econ:dq-rebuild
        crime:dq-rebuild geo:dq-rebuild fec:dq-rebuild fedregister:dq-rebuild
        lands:dq-rebuild health:dq-rebuild patents:dq-rebuild ref:dq-rebuild
        energy:dq-rebuild econ_reference:dq-rebuild cyber_threat:dq-rebuild cyber_vuln:dq-rebuild
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
        ref:daily
        fec:daily fedregister:daily
        cyber_vuln:daily cyber_vuln:weekly cyber_threat:weekly cyber_threat:hourly
        health:daily health:weekly health:monthly
        edu:annual edu:biennial
        energy:weekly energy:monthly energy:annual
        patents:daily lands:daily
        econ_reference:daily
      )
      [ -z "$SCHEMA_FILTER" ] && RUN_EMBEDDINGS=true
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

# Verify shadow JAR before launching
resolve_classpath > /dev/null

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

# Kill all processes in a setsid'd worker session.
# $pid is the session leader PID (== SID, written by the setsid wrapper).
# pkill -s kills by session ID — catches all processes regardless of process
# group, including java (which ignores SIGTERM and may be in a pipeline sub-group)
# and rclone. Falls back to process-group and individual kills on systems that
# lack pkill.
_kill_worker_session() {
  local pid=$1
  local procs
  # List what's in this session before killing (diagnostic)
  procs=$(ps -s "$pid" -o pid=,comm= 2>/dev/null | tr '\n' ' ' || true)
  if [ -z "$procs" ]; then
    _cleanup_log "  session $pid: no processes found (already dead or invalid SID)"
  else
    _cleanup_log "  session $pid: killing PIDs — $procs"
    if command -v pkill >/dev/null 2>&1; then
      pkill -TERM -s "$pid" 2>/dev/null || true
    else
      kill -TERM -"$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
    fi
    sleep 2
    procs=$(ps -s "$pid" -o pid=,comm= 2>/dev/null | tr '\n' ' ' || true)
    if [ -n "$procs" ]; then
      _cleanup_log "  session $pid: still alive after TERM, sending KILL — $procs"
      if command -v pkill >/dev/null 2>&1; then
        pkill -KILL -s "$pid" 2>/dev/null || true
      else
        kill -KILL -"$pid" 2>/dev/null || true
      fi
    else
      _cleanup_log "  session $pid: all processes terminated after TERM"
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
echo "    Timeout: ${TIMEOUT_MINS}min inactivity"
echo ""

# Active worker tracking: parallel arrays
active_pids=()
active_labels=()
active_starts=()
active_slots=()      # "schema:mode" for each active worker (used for re-queuing)
active_heap_mb=()    # Max heap in MB for each active worker
committed_mb=0       # Sum of max heaps of all active workers

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
  local log_file="$log_dir/launch.log"
  local pid_file="$PID_DIR/${id}.pid"
  local exit_file="$PID_DIR/${id}.exit"
  mkdir -p "$log_dir"
  rm -f "$pid_file" "$exit_file"

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
    # macOS / no setsid: subshell gives its own PGID via job control
    ( echo $BASHPID > "$pid_file"
      exec nohup bash "$script" "$schema" "$mode" >> "$log_file" 2>&1
    ) &
    echo $? > "$exit_file"
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

  active_pids+=("$pid")
  active_labels+=("$id")
  active_starts+=("$(date +%s)")
  active_slots+=("$slot")
  active_heap_mb+=("$heap_mb")
  committed_mb=$((committed_mb + heap_mb))

  log_info "Launched $id (PID $pid, heap ${heap_mb}MB) — committed: ${committed_mb}MB / ${budget_mb}MB budget"
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
    local next_heap_mb
    next_heap_mb=$(get_worker_heap_mb "$next_slot")
    local next_schema="${next_slot%%:*}"
    local next_mode="${next_slot#*:}"
    local next_id="worker-${next_schema}-${next_mode}"

    # Skip if this worker's heap exceeds total budget — can never run on this machine
    if [ "$next_heap_mb" -gt "$budget_mb" ]; then
      log_info "SKIPPING ${next_id}: needs ${next_heap_mb}MB but budget is only ${budget_mb}MB"
      ((done_count++)) || true
      ((failed_count++)) || true
      if [ "$scan_idx" -eq "$queue_idx" ]; then
        ((queue_idx++)) || true
      fi
      ((scan_idx++)) || true
      continue
    fi

    # Check 1: committed budget
    local projected=$((committed_mb + next_heap_mb))
    if [ "$projected" -gt "$budget_mb" ]; then
      log_info "Memory budget: ${next_id} needs ${next_heap_mb}MB, committed=${committed_mb}MB, budget=${budget_mb}MB — holding"
      break
    fi

    # Check 2: actual available memory (belt + suspenders, skip when no workers active)
    if [ "${#active_pids[@]}" -gt 0 ]; then
      local avail_mb
      avail_mb=$(get_available_mb)
      if [ "$avail_mb" -lt "$((next_heap_mb + OS_RESERVE_MB / 2))" ]; then
        log_info "Memory pressure: ${avail_mb}MB available, ${next_id} needs ${next_heap_mb}MB — holding"
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

  local new_pids=() new_labels=() new_starts=() new_slots=() new_heaps=()
  for i in "${!active_pids[@]}"; do
    if [ "$i" -ne "$idx" ]; then
      new_pids+=("${active_pids[$i]}")
      new_labels+=("${active_labels[$i]}")
      new_starts+=("${active_starts[$i]}")
      new_slots+=("${active_slots[$i]}")
      new_heaps+=("${active_heap_mb[$i]}")
    fi
  done
  active_pids=("${new_pids[@]+"${new_pids[@]}"}")
  active_labels=("${new_labels[@]+"${new_labels[@]}"}")
  active_starts=("${new_starts[@]+"${new_starts[@]}"}")
  active_slots=("${new_slots[@]+"${new_slots[@]}"}")
  active_heap_mb=("${new_heaps[@]+"${new_heaps[@]}"}")
}

# Kill a stuck worker and re-queue its slot
kill_and_requeue() {
  local idx=$1
  local pid="${active_pids[$idx]}"
  local id="${active_labels[$idx]}"
  local slot="${active_slots[$idx]}"
  local elapsed_mins=$(( ($(date +%s) - active_starts[$idx]) / 60 ))

  log_info "$id inactive (${elapsed_mins}m > ${TIMEOUT_MINS}m limit) — killing session $pid and re-queuing"
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

    if ! kill -0 "$pid" 2>/dev/null; then
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
        _log_file="$SCRIPT_DIR/runs/${id}/launch.log"
        last_err=$(grep -i -E "error|exception|fatal" "$_log_file" 2>/dev/null | tail -1 | cut -c1-120 || true)
        log_info "$id FAILED (${mins}m): ${last_err:-check log}"
      fi

      remove_active "$i"
      continue
    fi
    ((i++)) || true
  done

  # Check for stuck workers
  i=0
  while [ "$i" -lt "${#active_pids[@]}" ]; do
    id="${active_labels[$i]}"
    now=$(date +%s)
    uptime_secs=$(( now - active_starts[$i] ))
    if [ "$uptime_secs" -lt "$TIMEOUT_SECS" ]; then
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
    if [ "$idle_secs" -ge "$TIMEOUT_SECS" ]; then
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
      activity=$(grep -E "File chunk [0-9]+/[0-9]+|Chunk commit:|File chunking completed:|File chunking:|File-list optimization:|Tracked [0-9]+ newly materialized|Fast skip:|Row batch [0-9]+:|Row batching enabled|Processed entity|Converted|Processing [0-9]+ CIKs|Downloaded|INLINE CONVERSION|Filing (skipped|needs)|Writing Iceberg chunk|Processing batch|Expanded .* dimensions|Streaming from|Fetched [0-9]+ records|phase .* items processed|Downloading .* from|Processing table [0-9]+/[0-9]+:|ETL pipeline .* complete:|Bulk filtering:.*cached|SKIPPED \(table complete\)|Iceberg commit complete:|Materialization complete:|Streaming compaction:|Processing [0-9]+ unprocessed batches|Processing [0-9]+ bulk downloads|marked complete but no data found|Preload.* table completion|Preloaded tracker state|Bulk load|getCachedCompletion|Initialized S3 httpfs|Building accession list|Collected .* accessions|Loaded .* filings from|Phase [0-9]|Starting schema lifecycle|complete \(fast-path|Scanning full tracker|Scanned tracker year|Compacted tracker year|Narrowed CIK list|Processing 13F-HR|Converted 13F-HR|institutional holdings|Processing SC 13[DG]|Converted SC 13|beneficial ownership|13D/G filing detected|13F filing detected|GLEIF:|FIGI:|Extracted [0-9]+ records from|Extracted [0-9]+ holdings|Extracted [0-9]+ ownership|Extracted [0-9]+ instrument|OpenFIGI|gleif_entities|gleif_cik_mapping|figi_instruments|vectorized chunks from 13D|No data returned for|Skipping ticker|Marked ticker|appears to have no data|--rebuild:|--include-daily:|running DQ for schema|SCHEMA RESULT:|pre-flight checks|uploading results|results written to|DQ script:|purging r2:|removing tracker|ETL complete|DQ issue|created DQ|commented on DQ|Script exited|gh not|DuckDB exited|dq_[0-9]" "$log_file" 2>/dev/null | tail -1 | sed 's/^.*INFO  [^ ]* - //; s/^.*WARN  [^ ]* - //; s/^\[[0-9-]* [0-9:]*\] //' | cut -c1-120 || true)
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
if $RUN_EMBEDDINGS; then
  VSS_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
  CURRENT_YEAR=$(date +%Y)
  export VSS_YEARS="${VSS_YEARS:-$CURRENT_YEAR}"
  log_info "Embeddings: refreshing year(s) $VSS_YEARS"
  if [ -f "$VSS_DIR/vss-gpu-runner.sh" ]; then
    bash "$VSS_DIR/vss-gpu-runner.sh"
  fi
  if [ -f "$VSS_DIR/vss.sh" ]; then
    bash "$VSS_DIR/vss.sh" refresh "$VSS_YEARS"
    bash "$VSS_DIR/vss.sh" upload
  fi
  log_info "Embeddings: complete"
fi

exit 0
