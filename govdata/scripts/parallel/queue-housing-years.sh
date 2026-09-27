#!/usr/bin/env bash
# Queue remaining housing:2019-2025 years after 2018 completes
# This runs autonomously to keep remediation-ledger-runner #315 progressing

cd "$(dirname "$(dirname "$(dirname "${BASH_SOURCE[0]}")")")"

# Wait for housing:2018 to complete. run-pool.sh suffixes each exit file it writes with its own
# PID (see POOL_RUN_ID, #741), so match either the legacy bare name or the suffixed one rather
# than a single hardcoded path.
echo "[housing-queue] Waiting for housing:2018 to complete..."
HOUSING_2018_EXIT=""
while [ -z "$HOUSING_2018_EXIT" ]; do
  HOUSING_2018_EXIT=$(ls ./govdata/scripts/parallel/runs/pids/worker-housing-2018.exit \
                         ./govdata/scripts/parallel/runs/pids/worker-housing-2018.[0-9]*.exit \
                         2>/dev/null | head -1)
  [ -n "$HOUSING_2018_EXIT" ] || sleep 5
done

EXIT_CODE=$(cat "$HOUSING_2018_EXIT" 2>/dev/null || echo "1")
echo "[housing-queue] housing:2018 completed with exit code: $EXIT_CODE"

if [ "$EXIT_CODE" != "0" ]; then
  echo "[housing-queue] ERROR: 2018 failed with code $EXIT_CODE — aborting queue"
  exit 1
fi

# Queue years 2019-2025
echo "[housing-queue] Queuing years 2019-2025..."
for year in 2019 2020 2021 2022 2023 2024 2025; do
  echo "[housing-queue] Submitting housing:$year..."
  rm -f ./govdata/scripts/parallel/runs/pids/worker-housing-${year}.*
  nohup bash ./govdata/scripts/parallel/run-pool.sh housing:${year} > ./govdata/scripts/parallel/runs/reprocess-315-housing-${year}.log 2>&1 < /dev/null &
  sleep 1
done

echo "[housing-queue] All years queued. Jobs will run sequentially via pool."
