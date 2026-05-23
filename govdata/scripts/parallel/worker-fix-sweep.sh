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
