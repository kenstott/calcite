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

if [ $# -eq 0 ]; then
  echo "Usage: $0 <worker-numbers...>"
  echo "  $0 5 10 17       — run workers 05, 10, 17 in parallel"
  echo "  $0 1-5            — run workers 01 through 05"
  echo "  $0 3 7-9 15       — mix of individual and ranges"
  echo "  $0 all             — run all 39 workers"
  exit 1
fi

# Expand arguments into worker numbers
workers=()
for arg in "$@"; do
  if [ "$arg" = "all" ]; then
    for i in $(seq 1 40); do workers+=("$i"); done
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
start_times=()

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
  start_times+=("$(date +%s)")
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
      now=$(date +%s)
      elapsed=$((now - start_times[i]))
      mins=$((elapsed / 60))
      secs=$((elapsed % 60))
      status_lines+=("  ${id}: running ${mins}m${secs}s  | ${last_line}")
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
