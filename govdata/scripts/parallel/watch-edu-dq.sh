#!/usr/bin/env bash
# Watches worker-71 launch.log and runs iceberg_scan DQ checks as each pipeline completes.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG="$SCRIPT_DIR/runs/worker-71/launch.log"
DQ_OUT="$SCRIPT_DIR/runs/worker-71/dq-results.txt"
source "$(dirname "$SCRIPT_DIR")/.env.prod"

echo "DQ watcher started at $(date)" | tee "$DQ_OUT"

declare -A done_tables

run_dq() {
  local table=$1
  echo "" | tee -a "$DQ_OUT"
  echo "=== DQ: $table at $(date) ===" | tee -a "$DQ_OUT"
  duckdb << SQL 2>&1 | tee -a "$DQ_OUT"
INSTALL iceberg; LOAD iceberg;
INSTALL httpfs; LOAD httpfs;
SET s3_access_key_id='${AWS_ACCESS_KEY_ID}';
SET s3_secret_access_key='${AWS_SECRET_ACCESS_KEY}';
SET s3_endpoint='21cd637936a05913431a608f3f6d73bb.r2.cloudflarestorage.com';
SET s3_region='auto';
SET s3_url_style='path';
SELECT COUNT(*) AS row_count, MIN(year) AS min_year, MAX(year) AS max_year
FROM iceberg_scan('s3://govdata-parquet-v1/edu/${table}', allow_moved_paths=true);
SQL
}

while true; do
  while IFS= read -r line; do
    if echo "$line" | grep -q "complete: EtlResult{pipeline='\([^']*\)'"; then
      table=$(echo "$line" | sed "s/.*pipeline='\([^']*\)'.*/\1/")
      skipped=$(echo "$line" | grep -c "SKIPPED" || true)
      if [ -z "${done_tables[$table]+x}" ] && [ "$skipped" -eq 0 ]; then
        done_tables[$table]=1
        run_dq "$table"
      fi
    fi
  done < <(tail -F "$LOG" 2>/dev/null)
  sleep 5
done
