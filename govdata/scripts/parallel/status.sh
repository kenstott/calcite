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

TRACKER_BUCKET="${CALCITE_TRACKER_S3_BUCKET:-}"
if [ -z "$TRACKER_BUCKET" ]; then
  echo "ERROR: CALCITE_TRACKER_S3_BUCKET not set. Source env.sh or export it."
  exit 1
fi

# Check for duckdb CLI
if ! command -v duckdb &> /dev/null; then
  echo "ERROR: duckdb CLI not found. Install it from https://duckdb.org"
  exit 1
fi

# Build S3 config for DuckDB
S3_OPTS=""
if [ -n "${AWS_ACCESS_KEY_ID:-}" ]; then
  S3_OPTS="SET s3_access_key_id='${AWS_ACCESS_KEY_ID}'; SET s3_secret_access_key='${AWS_SECRET_ACCESS_KEY}';"
fi
if [ -n "${AWS_DEFAULT_REGION:-}" ]; then
  S3_OPTS="${S3_OPTS} SET s3_region='${AWS_DEFAULT_REGION}';"
fi
if [ -n "${AWS_ENDPOINT_OVERRIDE:-}" ]; then
  S3_OPTS="${S3_OPTS} SET s3_endpoint='${AWS_ENDPOINT_OVERRIDE}'; SET s3_url_style='path';"
fi

echo "=== Pipeline Tracker Status ==="
echo "Bucket: $TRACKER_BUCKET"
echo ""

# Summary by phase and state
echo "--- By Phase / State ---"
duckdb -c "
  INSTALL httpfs; LOAD httpfs;
  ${S3_OPTS}
  SELECT phase, state, count(*) AS cnt, sum(row_count) AS total_rows
  FROM read_parquet('${TRACKER_BUCKET}/**/*.parquet', hive_partitioning=true)
  GROUP BY phase, state
  ORDER BY phase, state;
" 2>/dev/null || echo "(no tracker data found)"

echo ""

# Summary by year (from source_key partitioning)
echo "--- By Year ---"
duckdb -c "
  INSTALL httpfs; LOAD httpfs;
  ${S3_OPTS}
  SELECT year, phase, state, count(*) AS cnt
  FROM read_parquet('${TRACKER_BUCKET}/**/*.parquet', hive_partitioning=true)
  GROUP BY year, phase, state
  ORDER BY year, phase, state;
" 2>/dev/null || echo "(no tracker data found)"

echo ""

# Errors
echo "--- Errors ---"
duckdb -c "
  INSTALL httpfs; LOAD httpfs;
  ${S3_OPTS}
  SELECT year, source_key, table_name, phase, error_message
  FROM read_parquet('${TRACKER_BUCKET}/**/*.parquet', hive_partitioning=true)
  WHERE state = 'error'
  ORDER BY as_of DESC
  LIMIT 20;
" 2>/dev/null || echo "(no errors)"

echo ""

# Local worker PID status
PID_DIR="$SCRIPT_DIR/runs/pids"
if [ -d "$PID_DIR" ]; then
  echo "--- Local Worker Processes ---"
  for i in $(seq -w 1 20); do
    pid_file="$PID_DIR/worker-${i}.pid"
    if [ -f "$pid_file" ]; then
      pid=$(cat "$pid_file")
      if kill -0 "$pid" 2>/dev/null; then
        echo "  worker-${i}: running (PID $pid)"
      else
        echo "  worker-${i}: exited  (PID $pid)"
      fi
    fi
  done
fi
