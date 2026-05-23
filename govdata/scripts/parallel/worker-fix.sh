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
