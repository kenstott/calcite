#!/usr/bin/env bash
# ============================================================================
# Run workers with a concurrency pool (max N at a time)
# Usage: ./run-pool.sh -j 4 1-20       — run workers 01-20, max 4 at a time
#        ./run-pool.sh -j 3 1 5 10-15   — mix of individual and ranges
#        ./run-pool.sh -j 6 all         — all workers, max 6 concurrent
#        ./run-pool.sh 1-10             — default: max 4 concurrent
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

MAX_WORKERS=4

# Parse -j flag
if [ "${1:-}" = "-j" ]; then
  if [ -z "${2:-}" ] || ! [[ "$2" =~ ^[0-9]+$ ]]; then
    echo "ERROR: -j requires a numeric argument" >&2
    exit 1
  fi
  MAX_WORKERS=$2
  shift 2
fi

if [ $# -eq 0 ]; then
  echo "Usage: $0 [-j max_concurrent] <worker-numbers...>"
  echo "  $0 -j 4 1-20        — run workers 01-20, max 4 at a time"
  echo "  $0 -j 3 1 5 10-15   — mix of individual and ranges"
  echo "  $0 -j 6 all         — all workers, max 6 concurrent"
  echo "  $0 1-10             — default: max $MAX_WORKERS concurrent"
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
echo "=== Pool Runner: $total workers, max $MAX_WORKERS concurrent ==="
echo ""

# Active worker tracking: parallel arrays
active_pids=()
active_labels=()
active_starts=()

# Counters
queue_idx=0
done_count=0
failed_count=0
failed_list=()

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

  local log_file="$SCRIPT_DIR/runs/${id}/launch.log"
  mkdir -p "$(dirname "$log_file")"

  nohup bash "$script" > "$log_file" 2>&1 &
  local pid=$!
  echo "$pid" > "$PID_DIR/${id}.pid"

  active_pids+=("$pid")
  active_labels+=("$id")
  active_starts+=("$(date +%s)")
  return 0
}

# Fill the pool up to MAX_WORKERS
fill_pool() {
  while [ "${#active_pids[@]}" -lt "$MAX_WORKERS" ] && [ "$queue_idx" -lt "$total" ]; do
    launch_worker "${queue[$queue_idx]}" || true
    ((queue_idx++))
  done
}

# Remove a finished worker from active arrays by index
remove_active() {
  local idx=$1
  local new_pids=() new_labels=() new_starts=()
  for i in "${!active_pids[@]}"; do
    if [ "$i" -ne "$idx" ]; then
      new_pids+=("${active_pids[$i]}")
      new_labels+=("${active_labels[$i]}")
      new_starts+=("${active_starts[$i]}")
    fi
  done
  active_pids=("${new_pids[@]+"${new_pids[@]}"}")
  active_labels=("${new_labels[@]+"${new_labels[@]}"}")
  active_starts=("${new_starts[@]+"${new_starts[@]}"}")
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
        last_err=$(grep -i -E "error|exception|fatal" "$log_file" 2>/dev/null | tail -1 | cut -c1-120)
        log_info "$id FAILED (${mins}m): ${last_err:-check log}"
      fi

      remove_active "$i"
      # Don't increment i — array shifted
      continue
    fi
    ((i++))
  done

  # Fill any open slots
  fill_pool

  # Print status
  remaining=$((total - done_count - failed_count - ${#active_pids[@]}))
  printf "\r[%s] Running: %d  Done: %d  Failed: %d  Queued: %d  " \
    "$(date '+%H:%M:%S')" "${#active_pids[@]}" "$done_count" "$failed_count" "$remaining"

  # Show active worker names
  if [ "${#active_labels[@]}" -gt 0 ]; then
    printf "| "
    for lbl in "${active_labels[@]}"; do
      printf "%s " "$lbl"
    done
  fi

  sleep 10
done

echo ""
echo ""
echo "=== Pool Complete ==="
echo "Total: $total  Done: $done_count  Failed: $failed_count"
if [ "$failed_count" -gt 0 ]; then
  echo "Failed workers: ${failed_list[*]}"
  exit 1
fi
exit 0
