#!/usr/bin/env bash
# ============================================================================
# Run one or more workers in parallel by number
# Usage: ./run.sh 5 10 17    — runs workers 05, 10, 17 in parallel
#        ./run.sh 1-5         — runs workers 01 through 05
#        ./run.sh 3 7-9 15    — mix of individual and ranges
#        ./run.sh all          — runs all 20 workers
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

if [ $# -eq 0 ]; then
  echo "Usage: $0 <worker-numbers...>"
  echo "  $0 5 10 17       — run workers 05, 10, 17 in parallel"
  echo "  $0 1-5            — run workers 01 through 05"
  echo "  $0 3 7-9 15       — mix of individual and ranges"
  echo "  $0 all             — run all 20 workers"
  exit 1
fi

# Expand arguments into worker numbers
workers=()
for arg in "$@"; do
  if [ "$arg" = "all" ]; then
    for i in $(seq 1 20); do workers+=("$i"); done
  elif [[ "$arg" =~ ^([0-9]+)-([0-9]+)$ ]]; then
    for i in $(seq "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}"); do workers+=("$i"); done
  elif [[ "$arg" =~ ^[0-9]+$ ]]; then
    workers+=("$arg")
  else
    echo "ERROR: invalid argument '$arg'" >&2
    exit 1
  fi
done

# Verify shadow JAR before launching
resolve_classpath > /dev/null

PID_DIR="$SCRIPT_DIR/runs/pids"
mkdir -p "$PID_DIR"

pids=()
labels=()

for num in "${workers[@]}"; do
  id=$(printf "worker-%02d" "$num")
  script="$SCRIPT_DIR/${id}.sh"

  if [ ! -f "$script" ]; then
    echo "ERROR: $script not found (worker $num)" >&2
    exit 1
  fi

  log_file="$SCRIPT_DIR/runs/${id}/launch.log"
  mkdir -p "$(dirname "$log_file")"

  echo "Starting ${id}..."
  nohup bash "$script" > "$log_file" 2>&1 &
  pid=$!
  echo "$pid" > "$PID_DIR/${id}.pid"
  echo "  PID: $pid  Log: $log_file"

  pids+=("$pid")
  labels+=("$id")
done

echo ""
echo "Monitoring ${#pids[@]} worker(s)... (Ctrl+C to stop monitoring; workers continue in background)"
echo ""

# Monitor loop — poll every 30s until all workers finish
while true; do
  running=0
  done_count=0
  failed=0
  status_lines=()

  for i in "${!pids[@]}"; do
    id="${labels[$i]}"
    pid="${pids[$i]}"
    log_file="$SCRIPT_DIR/runs/${id}/launch.log"

    if kill -0 "$pid" 2>/dev/null; then
      ((running++)) || true
      last_line=$(tail -1 "$log_file" 2>/dev/null | cut -c1-120)
      elapsed=$(ps -o etimes= -p "$pid" 2>/dev/null | tr -d ' ')
      if [ -n "$elapsed" ]; then
        mins=$((elapsed / 60))
        secs=$((elapsed % 60))
        status_lines+=("  ${id}: running ${mins}m${secs}s  | ${last_line}")
      else
        status_lines+=("  ${id}: running  | ${last_line}")
      fi
    else
      # Process finished — check exit code
      if wait "$pid" 2>/dev/null; then
        ((done_count++)) || true
        status_lines+=("  ${id}: done")
      else
        ((failed++)) || true
        last_err=$(grep -i -E "error|exception|fatal" "$log_file" 2>/dev/null | tail -1 | cut -c1-120)
        status_lines+=("  ${id}: FAILED  | ${last_err:-check log}")
      fi
    fi
  done

  # Print status
  printf "\033[2J\033[H"  # clear screen
  echo "=== ETL Worker Status  $(date '+%H:%M:%S') ==="
  echo "Running: $running  Done: $done_count  Failed: $failed"
  echo ""
  for line in "${status_lines[@]}"; do
    echo "$line"
  done

  if [ "$running" -eq 0 ]; then
    echo ""
    if [ "$failed" -gt 0 ]; then
      echo "$failed worker(s) failed"
      exit 1
    else
      echo "All workers complete"
      exit 0
    fi
  fi

  sleep 30
done
