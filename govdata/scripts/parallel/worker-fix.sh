#!/usr/bin/env bash
# ONE-TIME FIX: Clear stale _no_xbrl flags for insider forms (3/4/5) across historical years 2010-2026.
# Run once after deploying the DocumentETLProcessor fix. Do not add to recurring schedules.
# Split into 3 year-range chunks to avoid EdgarFullIndexCache OOM at startup.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

WORKER_ID="worker-fix"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

run_year_range() {
  local START_YEAR=$1
  local END_YEAR=$2
  local MODEL_FILE="$MODEL_DIR/sec-insider-${START_YEAR}-${END_YEAR}.json"

  log_info "$WORKER_ID running year range $START_YEAR-$END_YEAR"

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
      "startYear": ${START_YEAR},
      "endYear": ${END_YEAR},
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

  run_etl "$MODEL_FILE" "$WORKER_ID"
  log_info "$WORKER_ID year range $START_YEAR-$END_YEAR complete"
}

run_year_range 2020 2026
run_year_range 2015 2019
run_year_range 2010 2014

log_info "$WORKER_ID all ranges complete"
