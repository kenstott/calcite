#!/usr/bin/env bash
# Wait for the edu historical pool to finish, then run the daily workers.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "[$(date)] Waiting for edu historical pool (worker-71) to finish..."
while pgrep -f "worker-71.sh" > /dev/null 2>&1; do
  sleep 30
done
echo "[$(date)] worker-71 finished. Starting daily pool (workers 72+73)..."
cd "$SCRIPT_DIR"
./run-pool.sh --schema edu daily
