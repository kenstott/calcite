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
