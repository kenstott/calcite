#!/usr/bin/env bash
# ONE-TIME FIX (parallel): Clear stale _no_xbrl flags for insider forms (3/4/5)
# across years 2010-2019, one JVM per year running concurrently.
# 2020-2026 already completed by the sequential worker-fix.sh run.
# Run once after deploying the DocumentETLProcessor fix. Do not schedule.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

YEARS=(2010 2011 2012 2013 2014 2015 2016 2017 2018 2019)
PIDS=()
YEAR_FOR_PID=()

for YEAR in "${YEARS[@]}"; do
  WORKER_ID="worker-fix-$YEAR"
  MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
  mkdir -p "$MODEL_DIR"
  MODEL_FILE="$MODEL_DIR/sec-insider-$YEAR.json"

  cat > "$MODEL_FILE" <<EOF
{
  "version": "1.0",
  "defaultSchema": "sec",
  "schemas": [{
    "name": "sec",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
    "operand": {
      "dataSource": "sec",
      "ciks": "_ALL_EDGAR_FILERS",
      "filingTypes": ["3", "3/A", "4", "4/A", "5", "5/A"],
      "startYear": ${YEAR},
      "endYear": ${YEAR},
      "autoDownload": true,
      "directory": "${GOVDATA_PARQUET_DIR}",
      "cacheDirectory": "${GOVDATA_CACHE_DIR}",
      "trackerBackend": "s3",
      "trackerConfig": {
        "bucket": "${CALCITE_TRACKER_S3_BUCKET}",
        "endpoint": "${AWS_ENDPOINT_OVERRIDE}"
      },
      "s3Config": {
        "accessKeyId": "${AWS_ACCESS_KEY_ID}",
        "secretAccessKey": "${AWS_SECRET_ACCESS_KEY}",
        "endpoint": "${AWS_ENDPOINT_OVERRIDE}"
      }
    }
  }]
}
EOF

  log_info "Launching $WORKER_ID"
  (run_etl "$MODEL_FILE" "$WORKER_ID") &
  PIDS+=($!)
  YEAR_FOR_PID+=($YEAR)
done

log_info "All ${#PIDS[@]} workers launched: PIDs ${PIDS[*]}"

FAILED=0
for i in "${!PIDS[@]}"; do
  PID="${PIDS[$i]}"
  YEAR="${YEAR_FOR_PID[$i]}"
  if wait "$PID"; then
    log_info "worker-fix-$YEAR (PID $PID) complete"
  else
    log_info "WARNING: worker-fix-$YEAR (PID $PID) exited with error"
    FAILED=$((FAILED + 1))
  fi
done

if [ "$FAILED" -eq 0 ]; then
  log_info "All years 2010-2019 complete successfully"
else
  log_info "FAILED: $FAILED year(s) had errors — check logs in runs/worker-fix-YEAR/"
  exit 1
fi
