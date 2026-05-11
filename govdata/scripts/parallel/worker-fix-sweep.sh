#!/usr/bin/env bash
# ONE-TIME SWEEP: Clear stale _no_xbrl flags for insider forms (3/4/5) 2010-2019.
# Uses StaleInsiderFlagSweeper — reads EDGAR index cache only, no per-filing EDGAR HTTP calls.
# Run once. Completes in minutes. After this, the next regular ETL will reprocess cleared filings.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

WORKER_ID="worker-fix-sweep"
RUN_DIR="$SCRIPT_DIR/runs/$WORKER_ID"
MODEL_DIR="$RUN_DIR/models"
mkdir -p "$MODEL_DIR"

MODEL_FILE="$MODEL_DIR/sec-insider-sweep.json"

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
      "startYear": 2010,
      "endYear": 2026,
      "autoDownload": false,
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

JAR=$(resolve_classpath)

timestamp=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$RUN_DIR/sweep_${timestamp}.log"

log_info "[$WORKER_ID] Starting stale flag sweep years 2010-2026"
log_info "[$WORKER_ID] Log: $LOG_FILE"

java \
  -Xms512m \
  -Xmx2g \
  -cp "$JAR" \
  org.apache.calcite.adapter.govdata.etl.StaleInsiderFlagSweeper \
  --model "$MODEL_FILE" \
  --start-year 2010 \
  --end-year 2026 \
  2>&1 | stdbuf -oL tee "$LOG_FILE"

EXIT_CODE=${PIPESTATUS[0]}
if [ "$EXIT_CODE" -eq 0 ]; then
  log_info "[$WORKER_ID] Sweep complete successfully"
else
  log_info "[$WORKER_ID] Sweep exited with code $EXIT_CODE — check $LOG_FILE"
  exit "$EXIT_CODE"
fi
