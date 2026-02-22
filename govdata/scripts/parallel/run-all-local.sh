#!/usr/bin/env bash
# ============================================================================
# Launch all 10 workers locally in background
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_DIR="$SCRIPT_DIR/runs/pids"
mkdir -p "$PID_DIR"

# Verify shadow JAR exists
source "$SCRIPT_DIR/common.sh"
load_env
resolve_classpath > /dev/null

echo "=== Launching 20 parallel ETL workers ==="
echo ""

for i in $(seq -w 1 20); do
  worker_script="$SCRIPT_DIR/worker-${i}.sh"
  log_file="$SCRIPT_DIR/runs/worker-${i}/launch.log"
  mkdir -p "$(dirname "$log_file")"

  echo "Starting worker-${i}..."
  nohup bash "$worker_script" > "$log_file" 2>&1 &
  pid=$!
  echo "$pid" > "$PID_DIR/worker-${i}.pid"
  echo "  PID: $pid  Log: $log_file"
done

echo ""
echo "=== All workers launched ==="
echo "PIDs written to: $PID_DIR/"
echo ""
echo "Monitor progress:"
echo "  tail -f $SCRIPT_DIR/runs/worker-*/launch.log"
echo "  bash $SCRIPT_DIR/status.sh"
echo ""

# Show summary
echo "Worker status:"
for i in $(seq -w 1 20); do
  pid_file="$PID_DIR/worker-${i}.pid"
  if [ -f "$pid_file" ]; then
    pid=$(cat "$pid_file")
    if kill -0 "$pid" 2>/dev/null; then
      echo "  worker-${i}: running (PID $pid)"
    else
      echo "  worker-${i}: exited (PID $pid)"
    fi
  fi
done
