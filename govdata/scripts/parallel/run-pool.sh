#!/usr/bin/env bash
# ============================================================================
# Run workers with a concurrency pool (max N at a time)
# Usage: ./run-pool.sh -j 4 1-20              — run workers 01-20, max 4 at a time
#        ./run-pool.sh -j 3 -t 90 1 5 10-15   — max 3 concurrent, 90min inactivity timeout
#        ./run-pool.sh -j 6 all                — all workers, max 6 concurrent
#        ./run-pool.sh 1-10                    — default: max 4 concurrent, 60min inactivity timeout
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

MAX_WORKERS=4
TIMEOUT_MINS=60

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
    *) break ;;
  esac
done

TIMEOUT_SECS=$((TIMEOUT_MINS * 60))

if [ $# -eq 0 ]; then
  echo "Usage: $0 [-j max_concurrent] [-t timeout_mins] <worker-numbers...>"
  echo "  $0 -j 4 1-20              — run workers 01-20, max 4 at a time"
  echo "  $0 -j 3 -t 90 1 5 10-15   — max 3 concurrent, 90min timeout"
  echo "  $0 -j 6 all               — all workers, max 6 concurrent"
  echo "  $0 1-10                   — default: max $MAX_WORKERS concurrent, ${TIMEOUT_MINS}min timeout"
  exit 1
fi

# Expand arguments into worker numbers
queue=()
for arg in "$@"; do
  if [ "$arg" = "all" ]; then
    for i in $(seq 1 40); do queue+=("$i"); done
  elif [[ "$arg" =~ ^([0-9]+)-([0-9]+)$ ]]; then
    for i in $(seq "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}"); do queue+=("$i"); done
  elif [[ "$arg" =~ ^[0-9]+$ ]]; then
    queue+=("$arg")
  else
    echo "ERROR: invalid argument '$arg'" >&2
    exit 1
  fi
done

# Verify shadow JAR before launching
resolve_classpath > /dev/null

PID_DIR="$SCRIPT_DIR/runs/pids"
mkdir -p "$PID_DIR"

total=${#queue[@]}
echo "=== Pool Runner: $total workers, max $MAX_WORKERS concurrent, ${TIMEOUT_MINS}min inactivity timeout ==="
echo ""

# Active worker tracking: parallel arrays
active_pids=()
active_labels=()
active_starts=()
active_nums=()

# Counters
queue_idx=0
done_count=0
failed_count=0
failed_list=()
restart_count=0

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

  local log_dir="$SCRIPT_DIR/runs/${id}"
  local log_file="$log_dir/launch.log"
  mkdir -p "$log_dir"

  # setsid gives each worker its own process group so 'kill 0' in the worker
  # trap only kills that worker's processes, not the pool runner or other workers
  setsid nohup bash "$script" >> "$log_file" 2>&1 &
  local pid=$!
  echo "$pid" > "$PID_DIR/${id}.pid"

  active_pids+=("$pid")
  active_labels+=("$id")
  active_starts+=("$(date +%s)")
  active_nums+=("$num")
  return 0
}

# Fill the pool up to MAX_WORKERS
fill_pool() {
  while [ "${#active_pids[@]}" -lt "$MAX_WORKERS" ] && [ "$queue_idx" -lt "$total" ]; do
    launch_worker "${queue[$queue_idx]}" || true
    ((queue_idx++)) || true
  done
}

# Remove a finished worker from active arrays by index
remove_active() {
  local idx=$1
  local new_pids=() new_labels=() new_starts=() new_nums=()
  for i in "${!active_pids[@]}"; do
    if [ "$i" -ne "$idx" ]; then
      new_pids+=("${active_pids[$i]}")
      new_labels+=("${active_labels[$i]}")
      new_starts+=("${active_starts[$i]}")
      new_nums+=("${active_nums[$i]}")
    fi
  done
  active_pids=("${new_pids[@]+"${new_pids[@]}"}")
  active_labels=("${new_labels[@]+"${new_labels[@]}"}")
  active_starts=("${new_starts[@]+"${new_starts[@]}"}")
  active_nums=("${new_nums[@]+"${new_nums[@]}"}")
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
  printf "\n[%s] Running: %d  Done: %d  Failed: %d  Queued: %d  Restarts: %d  %s\n" \
    "$(date '+%H:%M:%S')" "${#active_pids[@]}" "$done_count" "$failed_count" "$remaining" "$restart_count" "$active_str"

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
      # Look for the last Processed/Converted/Downloaded/Processing line
      # Match progress patterns across all worker types (SEC, econ, census, geo, crime)
      activity=$(grep -E "Processed entity|Converted|Processing [0-9]+ CIKs|Downloaded|INLINE CONVERSION|Filing (skipped|needs)|Writing Iceberg chunk|Processing batch|Expanded .* dimensions|Streaming from|Fetched [0-9]+ records|phase .* items processed|Downloading .* from" "$log_file" 2>/dev/null | tail -1 | sed 's/^.*INFO  [^ ]* - //; s/^.*WARN  [^ ]* - //' | cut -c1-120 || true)
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
