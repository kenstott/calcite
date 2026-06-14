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

# ── argument parsing ──────────────────────────────────────────────────────────
RUN_DATE=$(date +%Y-%m-%d)
MODE_FILTER=""
SCHEMA_FILTER=""
SHOW_WARNS=false
SHOW_PASS=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-date)
      shift
      RUN_DATE="${1:?--run-date requires YYYY-MM-DD}"
      ;;
    --mode)
      shift
      MODE_FILTER="${1:?--mode requires daily or historical}"
      ;;
    --schemas)
      shift
      SCHEMA_FILTER="${1:?--schemas requires a comma-separated list}"
      ;;
    --show-warns)
      SHOW_WARNS=true
      ;;
    --show-pass)
      SHOW_PASS=true
      ;;
    --help|-h)
      sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
  shift
done

# ── S3 config — DQ results live on the MinIO DQ instance, never R2 ────────────
# Require the MinIO endpoint from AWS_ENDPOINT_OVERRIDE (govdata/.env.dq) and
# derive the scheme-stripped host + SSL flag DuckDB needs (MinIO = path-style).
if [ -z "${AWS_ENDPOINT_OVERRIDE:-}" ]; then
  echo "ERROR: AWS_ENDPOINT_OVERRIDE is not set — DQ report requires the MinIO endpoint (source govdata/.env.dq)" >&2
  exit 1
fi
S3_ENDPOINT="$AWS_ENDPOINT_OVERRIDE"
[[ "$S3_ENDPOINT" != http* ]] && S3_ENDPOINT="https://${S3_ENDPOINT}"
ENDPOINT_ARGS="--endpoint-url $S3_ENDPOINT"
S3_ENDPOINT_HOST="${AWS_ENDPOINT_OVERRIDE#http://}"; S3_ENDPOINT_HOST="${S3_ENDPOINT_HOST#https://}"; S3_ENDPOINT_HOST="${S3_ENDPOINT_HOST%/}"
case "$AWS_ENDPOINT_OVERRIDE" in https://*) S3_USE_SSL=true ;; *) S3_USE_SSL=false ;; esac
S3_REGION="${AWS_REGION:-us-east-1}"

# ── Mirror result parquets locally via rclone, then read from disk ────────────
# DuckDB cannot read the s3://.../schema=*/run_date=*/type=*/ partition glob on
# MinIO: path-style addressing URL-encodes the '=' in the keys → HTTP 404. rclone
# handles '=' correctly, so copy the matching run_date partitions to a temp dir
# and read with a plain filesystem path. No S3 read, no '=' glob.
DQ_REMOTE="${GOVDATA_DQ_RCLONE_REMOTE:-${GOVDATA_RCLONE_REMOTE:-minio}}"
RESULT_BUCKET="${GOVDATA_DQ_TRACKER_BUCKET:-govdata-tracker-v1-dq}"
LOCAL_RESULTS="$(mktemp -d)"
trap 'rm -rf "$LOCAL_RESULTS"' EXIT
rclone copy "${DQ_REMOTE}:${RESULT_BUCKET}/dq-results/" "$LOCAL_RESULTS" \
  --include "/schema=*/run_date=${RUN_DATE}/type=*/results.parquet"
if ! find "$LOCAL_RESULTS" -name 'results.parquet' -print -quit | grep -q .; then
  echo "ERROR: no DQ results found for run_date=${RUN_DATE} in ${DQ_REMOTE}:${RESULT_BUCKET}/dq-results/" >&2
  exit 1
fi
GLOB_PATH="${LOCAL_RESULTS}/schema=*/run_date=${RUN_DATE}/type=*/results.parquet"

# ── build WHERE clause fragments ──────────────────────────────────────────────
WHERE_CLAUSES=()
[ -n "$MODE_FILTER" ]   && WHERE_CLAUSES+=("run_type = '${MODE_FILTER}'")
[ -n "$SCHEMA_FILTER" ] && WHERE_CLAUSES+=("schema_name IN ($(echo "$SCHEMA_FILTER" | sed "s/\([^,]*\)/'\1'/g"))")

WHERE_SQL=""
if [ ${#WHERE_CLAUSES[@]} -gt 0 ]; then
  WHERE_SQL="WHERE $(IFS=' AND '; echo "${WHERE_CLAUSES[*]}")"
fi

DETAIL_STATUS_FILTER="status IN ('fail'"
$SHOW_WARNS && DETAIL_STATUS_FILTER="${DETAIL_STATUS_FILTER}, 'warn'"
$SHOW_PASS  && DETAIL_STATUS_FILTER="${DETAIL_STATUS_FILTER}, 'pass'"
DETAIL_STATUS_FILTER="${DETAIL_STATUS_FILTER})"

# ── run report via DuckDB ─────────────────────────────────────────────────────
duckdb <<SQL
INSTALL httpfs; LOAD httpfs;
INSTALL iceberg; LOAD iceberg;

SET s3_access_key_id     = '${AWS_ACCESS_KEY_ID}';
SET s3_secret_access_key = '${AWS_SECRET_ACCESS_KEY}';
SET s3_endpoint          = '${S3_ENDPOINT_HOST}';
SET s3_url_style         = 'path';
SET s3_use_ssl           = ${S3_USE_SSL};
SET s3_region            = '${S3_REGION}';
SET memory_limit         = '${DUCKDB_MEMORY_LIMIT:-2GB}';
SET temp_directory       = '/tmp/duckdb-dqreport';

-- ── schema-level verdict ─────────────────────────────────────────────────────
.print ''
.print '=== DQ Schema Summary  run_date=${RUN_DATE} ==='

SELECT
  schema_name                                                  AS schema,
  run_type                                                     AS mode,
  COUNT(*)                                                     AS total_tests,
  SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END)            AS pass,
  SUM(CASE WHEN status = 'warn' THEN 1 ELSE 0 END)            AS warn,
  SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END)            AS fail,
  CASE
    WHEN SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) > 0 THEN 'FAIL'
    WHEN SUM(CASE WHEN status = 'warn' THEN 1 ELSE 0 END) > 0 THEN 'WARN'
    ELSE 'PASS'
  END                                                          AS verdict
FROM read_parquet('${GLOB_PATH}')
${WHERE_SQL}
GROUP BY schema_name, run_type
ORDER BY verdict DESC, schema_name;

-- ── failing (and optionally warning/passing) test details ────────────────────
.print ''
.print '=== DQ Test Details ==='

SELECT
  schema_name  AS schema,
  table_name,
  test,
  status,
  value,
  threshold,
  detail
FROM read_parquet('${GLOB_PATH}')
${WHERE_SQL}
  $( [ -n "$WHERE_SQL" ] && echo "AND ${DETAIL_STATUS_FILTER}" || echo "WHERE ${DETAIL_STATUS_FILTER}" )
ORDER BY schema_name, table_name, test;
SQL
