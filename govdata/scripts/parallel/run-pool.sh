#!/usr/bin/env bash
# ============================================================================
# Run workers with a memory-aware concurrency pool
#
# Automatically determines how many workers can run concurrently based on
# available system memory. Each worker's max heap is reserved from the memory
# budget before launch, preventing OOM kills from over-subscription.
#
# Usage: ./run-pool.sh 18-26                  — auto-fit workers to available memory
#        ./run-pool.sh -j 4 1-20              — hard cap at 4 concurrent (+ memory gate)
#        ./run-pool.sh -t 90 1 5 10-15        — 90min inactivity timeout
#        ./run-pool.sh -r 1500 18-26          — reserve 1.5GB for OS/other processes
#        ./run-pool.sh -p 4 18-26             — 4 parallel entity threads per worker
#        ./run-pool.sh 1-10                   — default: auto-fit, 60min timeout
# ============================================================================
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
    *) break ;;
  esac
done

TIMEOUT_SECS=$((TIMEOUT_MINS * 60))

# Export flags so worker scripts pass them to EtlRunner
if [ "$PARALLEL_THREADS" -gt 0 ]; then
  export ETL_PARALLEL_THREADS=$PARALLEL_THREADS
fi

if [ $# -eq 0 ]; then
  echo "Usage: $0 [-j max_concurrent] [-t timeout_mins] [-r os_reserve_mb] [-p threads] [-c] <worker-numbers...>"
  echo "  $0 18-26                  — auto-fit workers to available memory"
  echo "  $0 -j 4 1-20              — hard cap at 4 concurrent (+ memory gate)"
  echo "  $0 -t 90 1 5 10-15        — 90min inactivity timeout"
  echo "  $0 -r 2000 18-26          — reserve 2GB for OS (default: ${OS_RESERVE_MB}MB)"
  echo "  $0 -p 4 18-26             — 4 parallel entity threads per worker"
  echo "  $0 1-10                   — default: auto-fit, ${TIMEOUT_MINS}min timeout"
  echo "  $0 1-7,23-40              — discontinuous ranges (SEC primary + secondary)"
  exit 1
fi

# Expand arguments into worker numbers (supports: 5, 1-10, 1-10,15-20, all)
queue=()
for arg in "$@"; do
  # Split on commas to support discontinuous ranges like "1-10,15-20"
  IFS=',' read -ra parts <<< "$arg"
  for part in "${parts[@]}"; do
    if [ "$part" = "all" ]; then
      for i in $(seq 1 58); do queue+=("$i"); done
    elif [[ "$part" =~ ^([0-9]+)-([0-9]+)$ ]]; then
      for i in $(seq "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}"); do queue+=("$i"); done
    elif [[ "$part" =~ ^[0-9]+$ ]]; then
      queue+=("$part")
    else
      echo "ERROR: invalid argument '$part' (from '$arg')" >&2
      exit 1
    fi
  done
done

# Verify shadow JAR before launching
resolve_classpath > /dev/null

PID_DIR="$SCRIPT_DIR/runs/pids"
mkdir -p "$PID_DIR"

# On Ctrl-C or SIGTERM, kill all active workers before exiting
cleanup() {
  echo ""
  echo "=== Interrupted — killing active workers ==="
  for i in "${!active_pids[@]}"; do
    local pid="${active_pids[$i]}"
    local id="${active_labels[$i]}"
    echo "  Killing $id (PID $pid)"
    kill -TERM -"$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
    sleep 0.5
    kill -KILL -"$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  echo "=== All workers terminated ==="
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
active_nums=()
active_heap_mb=()     # Max heap in MB for each active worker
committed_mb=0        # Sum of max heaps of all active workers

# Counters
queue_idx=0
done_count=0
failed_count=0
failed_list=()
restart_count=0

# Convert a heap size string (e.g., "3g", "2048m", "2g") to MB
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

# Get the max heap in MB for a given worker number
get_worker_heap_mb() {
  local num=$1
  local id
  id=$(printf "worker-%02d" "$num")
  local _HEAP_MIN _HEAP_MAX
  get_heap_config "$id"
  heap_to_mb "$_HEAP_MAX"
}

# Launch a single worker by number, append to active arrays
launch_worker() {
  local num=$1
  local id
  id=$(printf "worker-%02d" "$num")
  local script="$SCRIPT_DIR/${id}.sh"

  if [ ! -f "$script" ]; then
    echo "WARNING: $script not found, skipping" >&2
    return 1
  fi

  local heap_mb
  heap_mb=$(get_worker_heap_mb "$num")

  local log_dir="$SCRIPT_DIR/runs/${id}"
  local log_file="$log_dir/launch.log"
  mkdir -p "$log_dir"

  # Give each worker its own process group so 'kill 0' in the worker
  # trap only kills that worker's processes, not the pool runner or other workers.
  # macOS lacks setsid; use a subshell trick to create a new process group instead.
  if command -v setsid >/dev/null 2>&1; then
    setsid nohup bash "$script" >> "$log_file" 2>&1 &
  else
    (exec nohup bash "$script" >> "$log_file" 2>&1) &
  fi
  local pid=$!
  echo "$pid" > "$PID_DIR/${id}.pid"

  active_pids+=("$pid")
  active_labels+=("$id")
  active_starts+=("$(date +%s)")
  active_nums+=("$num")
  active_heap_mb+=("$heap_mb")
  committed_mb=$((committed_mb + heap_mb))

  log_info "Launched $id (PID $pid, heap ${heap_mb}MB) — committed: ${committed_mb}MB / ${budget_mb}MB budget"
  return 0
}

# Check available system memory in MB
get_available_mb() {
  if [ "$(uname)" = "Darwin" ]; then
    # Parse vm_stat pages free + inactive, multiply by page size
    local page_size=$(sysctl -n hw.pagesize)
    local free_pages=$(vm_stat | awk '/Pages free/ {gsub(/\./,"",$3); print $3}')
    local inactive_pages=$(vm_stat | awk '/Pages inactive/ {gsub(/\./,"",$3); print $3}')
    echo $(( (free_pages + inactive_pages) * page_size / 1024 / 1024 ))
  else
    awk '/MemAvailable/ {print int($2/1024)}' /proc/meminfo
  fi
}

# Fill the pool up to MAX_WORKERS, respecting the memory budget.
# A worker is only launched if its max heap fits within the remaining budget.
# Workers that can never fit (heap > budget) are skipped with a warning.
fill_pool() {
  local scan_idx=$queue_idx
  while [ "${#active_pids[@]}" -lt "$MAX_WORKERS" ] && [ "$scan_idx" -lt "$total" ]; do
    local next_num="${queue[$scan_idx]}"
    local next_heap_mb
    next_heap_mb=$(get_worker_heap_mb "$next_num")
    local next_id
    next_id=$(printf "worker-%02d" "$next_num")

    # Check: Would this worker's heap exceed the total memory budget even alone?
    # If so, skip permanently — it can never run on this machine.
    if [ "$next_heap_mb" -gt "$budget_mb" ]; then
      log_info "SKIPPING ${next_id}: needs ${next_heap_mb}MB but budget is only ${budget_mb}MB — cannot run on this machine"
      ((done_count++)) || true
      ((failed_count++)) || true
      # Advance queue past this worker
      if [ "$scan_idx" -eq "$queue_idx" ]; then
        ((queue_idx++)) || true
      fi
      ((scan_idx++)) || true
      continue
    fi

    # Check 1: Would this worker's heap exceed remaining budget?
    local projected=$((committed_mb + next_heap_mb))
    if [ "$projected" -gt "$budget_mb" ]; then
      log_info "Memory budget: ${next_id} needs ${next_heap_mb}MB, committed=${committed_mb}MB, budget=${budget_mb}MB — holding"
      break
    fi

    # Check 2: Is actual available memory sufficient? (belt + suspenders)
    # When no workers are running, trust the budget check — JVM doesn't allocate
    # max heap immediately and Linux reclaims page cache under pressure.
    if [ "${#active_pids[@]}" -gt 0 ]; then
      local avail_mb
      avail_mb=$(get_available_mb)
      if [ "$avail_mb" -lt "$((next_heap_mb + OS_RESERVE_MB / 2))" ]; then
        log_info "Memory pressure: ${avail_mb}MB available, ${next_id} needs ${next_heap_mb}MB — holding"
        break
      fi
    fi

    # Advance queue_idx to match scan_idx if we skipped any
    queue_idx=$scan_idx
    launch_worker "${queue[$queue_idx]}" || true
    ((queue_idx++)) || true
    scan_idx=$queue_idx
  done
}

# Remove a finished worker from active arrays by index
remove_active() {
  local idx=$1
  # Release this worker's heap reservation
  committed_mb=$((committed_mb - active_heap_mb[$idx]))
  if [ "$committed_mb" -lt 0 ]; then committed_mb=0; fi

  local new_pids=() new_labels=() new_starts=() new_nums=() new_heaps=()
  for i in "${!active_pids[@]}"; do
    if [ "$i" -ne "$idx" ]; then
      new_pids+=("${active_pids[$i]}")
      new_labels+=("${active_labels[$i]}")
      new_starts+=("${active_starts[$i]}")
      new_nums+=("${active_nums[$i]}")
      new_heaps+=("${active_heap_mb[$i]}")
    fi
  done
  active_pids=("${new_pids[@]+"${new_pids[@]}"}")
  active_labels=("${new_labels[@]+"${new_labels[@]}"}")
  active_starts=("${new_starts[@]+"${new_starts[@]}"}")
  active_nums=("${new_nums[@]+"${new_nums[@]}"}")
  active_heap_mb=("${new_heaps[@]+"${new_heaps[@]}"}")
}

# Kill a stuck worker and re-queue it
kill_and_requeue() {
  local idx=$1
  local pid="${active_pids[$idx]}"
  local id="${active_labels[$idx]}"
  local num="${active_nums[$idx]}"
  local elapsed_mins=$(( ($(date +%s) - active_starts[$idx]) / 60 ))

  log_info "$id inactive (${elapsed_mins}m since last log output > ${TIMEOUT_MINS}m limit) — killing PID $pid and re-queuing"

  # Kill the worker's process group (setsid gives each worker its own group)
  kill -TERM -"$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
  sleep 2
  kill -KILL -"$pid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true

  remove_active "$idx"
  ((restart_count++)) || true

  # Re-queue: append worker number back to the queue
  queue+=("$num")
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

      if wait "$pid" 2>/dev/null; then
        ((done_count++)) || true
        log_info "$id finished OK (${mins}m)"
      else
        ((failed_count++)) || true
        failed_list+=("$id")
        log_file="$SCRIPT_DIR/runs/${id}/launch.log"
        last_err=$(grep -i -E "error|exception|fatal" "$log_file" 2>/dev/null | tail -1 | cut -c1-120 || true)
        log_info "$id FAILED (${mins}m): ${last_err:-check log}"
      fi

      remove_active "$i"
      # Don't increment i — array shifted
      continue
    fi
    ((i++)) || true
  done

  # Check for stuck workers — kill if log has no new output for TIMEOUT_MINS
  # Grace period: don't check until the worker has been running for at least TIMEOUT_SECS,
  # since the log file may have a stale mtime from a previous run.
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
      # Don't increment i — array shifted
      continue
    fi
    ((i++)) || true
  done

  # Fill any open slots
  fill_pool

  # Print status — use \n so output is visible in logs, nohup, and pipes
  remaining=$((total - done_count - failed_count - ${#active_pids[@]}))
  active_str=""
  if [ "${#active_labels[@]}" -gt 0 ]; then
    active_str="| ${active_labels[*]}"
  fi
  mem_avail=$(get_available_mb)
  printf "\n[%s] Running: %d  Done: %d  Failed: %d  Queued: %d  Restarts: %d  Heap: %s/%sMB  Free: %sMB  %s\n" \
    "$(date '+%H:%M:%S')" "${#active_pids[@]}" "$done_count" "$failed_count" "$remaining" "$restart_count" \
    "$committed_mb" "$budget_mb" "$mem_avail" "$active_str"

  # Per-worker detail: elapsed time + last activity from log
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

    # Extract last meaningful activity from the worker's log
    log_file="$SCRIPT_DIR/runs/${id}/launch.log"
    activity=""
    if [ -f "$log_file" ]; then
      # Match progress patterns across all worker types (SEC, econ, census, geo, crime)
      activity=$(grep -E "Processed entity|Converted|Processing [0-9]+ CIKs|Downloaded|INLINE CONVERSION|Filing (skipped|needs)|Writing Iceberg chunk|Processing batch|Expanded .* dimensions|Streaming from|Fetched [0-9]+ records|phase .* items processed|Downloading .* from|Processing table [0-9]+/[0-9]+:|ETL pipeline .* complete:|Bulk filtering:.*cached|SKIPPED \(table complete\)|Iceberg commit complete:|Materialization complete:|Streaming compaction:|Processing [0-9]+ unprocessed batches|Processing [0-9]+ bulk downloads|marked complete but no data found|Preload.* table completion|Preloaded tracker state|Bulk load|getCachedCompletion|Initialized S3 httpfs|Building accession list|Collected .* accessions|Loaded .* filings from|Phase [0-9]|Starting schema lifecycle|complete \(fast-path|Scanning full tracker|Scanned tracker year|Compacted tracker year|Narrowed CIK list|Processing 13F-HR|Converted 13F-HR|institutional holdings|Processing SC 13[DG]|Converted SC 13|beneficial ownership|13D/G filing detected|13F filing detected|GLEIF:|FIGI:|Extracted [0-9]+ records from|Extracted [0-9]+ holdings|Extracted [0-9]+ ownership|Extracted [0-9]+ instrument|OpenFIGI|gleif_entities|gleif_cik_mapping|figi_instruments|vectorized chunks from 13D|No data returned for|Skipping ticker|Marked ticker|appears to have no data" "$log_file" 2>/dev/null | tail -1 | sed 's/^.*INFO  [^ ]* - //; s/^.*WARN  [^ ]* - //' | cut -c1-120 || true)
    fi
    printf "  %-12s [%s] %s\n" "$id" "$elapsed_str" "${activity:-starting...}"
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
exit 0
