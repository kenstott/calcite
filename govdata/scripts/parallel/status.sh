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

# Postgres is the canonical ETL state store; the tracker tables live in a schema derived from
# the parquet bucket (PGPipelineTracker.sanitizeNamespace), so dq and prod never mix.
# shellcheck source=/dev/null
source "$SCRIPT_DIR/../tracker_pg.sh"
PG_NS="$(pg_ns_from_bucket "${GOVDATA_PARQUET_DIR:?GOVDATA_PARQUET_DIR not set}")" || exit 2

echo "=== Pipeline Tracker Status ==="
echo "Tracker schema: ${PG_NS} (postgres)"
echo ""

echo "--- By Phase / State ---"
pg_tracker_exec "
  SELECT phase, state, count(*) AS cnt, sum(row_count) AS total_rows
    FROM \"${PG_NS}\".pipeline_tracker
   GROUP BY phase, state
   ORDER BY phase, state;" || exit 2

echo ""

# Year lives inside source_key ("...__year=YYYY" or "year=YYYY"), not as its own column.
echo "--- By Year ---"
pg_tracker_exec "
  SELECT substring(source_key from 'year=([0-9]{4})') AS year, phase, state, count(*) AS cnt
    FROM \"${PG_NS}\".pipeline_tracker
   GROUP BY 1, phase, state
   ORDER BY 1, phase, state;" || exit 2

echo ""

echo "--- Errors ---"
pg_tracker_exec "
  SELECT source_key, table_name, phase, error_message
    FROM \"${PG_NS}\".pipeline_tracker
   WHERE state = 'error'
   ORDER BY as_of DESC
   LIMIT 20;" || exit 2

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
