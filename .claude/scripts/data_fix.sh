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
# data_fix.sh — Reset a schema/table for re-ingestion.
#
# Drops the Iceberg table, invalidates the tracker completion marker, and
# optionally deletes raw cache files for a year range.  Run the schema ETL
# afterward to re-ingest from scratch.
#
# Usage:
#   data_fix.sh --schema <schema> --table <table> \
#               [--env prod|dq] \
#               [--start YYYY] [--stop YYYY] \
#               [--raw true|false] [--dry-run]
#
# --env prod (default): targets R2 prod buckets via rclone remote `r2:`.
# --env dq:             targets MinIO DQ buckets via the rclone remote named in
#                       $GOVDATA_DQ_RCLONE_REMOTE (typically `minio`).
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Script lives in <project>/.claude/scripts; the .env.* files and scripts/ live in <project>/govdata.
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
GOVDATA_HOME="${GOVDATA_HOME:-$PROJECT_ROOT/govdata}"

# ── Argument parsing ──────────────────────────────────────────────────────────
SCHEMA=""
TABLE=""
START_YEAR=""
STOP_YEAR=""
RAW_FLAG="false"
DRY_RUN=false
ENV_NAME="prod"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --schema)  SCHEMA="$2";     shift 2 ;;
    --table)   TABLE="$2";      shift 2 ;;
    --env)     ENV_NAME="$2";   shift 2 ;;
    --start)   START_YEAR="$2"; shift 2 ;;
    --stop)    STOP_YEAR="$2";  shift 2 ;;
    --raw)     RAW_FLAG="$2";   shift 2 ;;
    --dry-run) DRY_RUN=true;    shift   ;;
    *) echo "Unknown argument: $1"; exit 1 ;;
  esac
done

if [[ -z "$SCHEMA" || -z "$TABLE" ]]; then
  echo "Usage: data_fix.sh --schema <schema> --table <table> [--env prod|dq] [--start YYYY] [--stop YYYY] [--raw true|false] [--dry-run]"
  exit 1
fi

if [[ "$ENV_NAME" != "prod" && "$ENV_NAME" != "dq" ]]; then
  echo "Error: --env must be 'prod' or 'dq' (got '$ENV_NAME')"
  exit 1
fi

DELETE_RAW=false
if [[ "$RAW_FLAG" == "true" ]]; then
  DELETE_RAW=true
fi

if $DELETE_RAW; then
  : "${START_YEAR:=2000}"
  : "${STOP_YEAR:=$(date +%Y)}"
fi

# ── Load environment ──────────────────────────────────────────────────────────
# Always source .env.prod first to get base config (bucket names, etc.).
# When --env dq, also source .env.dq to override AWS creds + endpoint for MinIO.
if [[ -f "$GOVDATA_HOME/.env.prod" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "$GOVDATA_HOME/.env.prod"
  set +a
elif [[ -f "$GOVDATA_HOME/.env.test" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "$GOVDATA_HOME/.env.test"
  set +a
fi

if [[ "$ENV_NAME" == "dq" ]]; then
  if [[ ! -f "$GOVDATA_HOME/.env.dq" ]]; then
    echo "Error: --env dq requested but $GOVDATA_HOME/.env.dq not found"
    exit 1
  fi
  set -a
  # shellcheck source=/dev/null
  source "$GOVDATA_HOME/.env.dq"
  set +a
fi

if [[ -z "${AWS_ACCESS_KEY_ID:-}" ]]; then
  echo "Error: AWS credentials not set. Source .env.prod or .env.test first."
  exit 1
fi

# ── Derived variables ─────────────────────────────────────────────────────────
if [[ "$ENV_NAME" == "dq" ]]; then
  : "${GOVDATA_DQ_BUCKET:?GOVDATA_DQ_BUCKET not set (check .env.dq)}"
  : "${GOVDATA_DQ_TRACKER_BUCKET:?GOVDATA_DQ_TRACKER_BUCKET not set (check .env.dq)}"
  : "${GOVDATA_DQ_RCLONE_REMOTE:?GOVDATA_DQ_RCLONE_REMOTE not set (check .env.dq)}"
  PARQUET_BUCKET="s3://${GOVDATA_DQ_BUCKET}"
  TRACKER_BUCKET="s3://${GOVDATA_DQ_TRACKER_BUCKET}"
  RAW_BUCKET="${GOVDATA_RAW_DIR:-s3://govdata-raw-v1}"
  RCLONE_REMOTE="${GOVDATA_DQ_RCLONE_REMOTE}"
else
  PARQUET_BUCKET="${GOVDATA_PARQUET_DIR:-s3://govdata-parquet-v1}"
  RAW_BUCKET="${GOVDATA_RAW_DIR:-s3://govdata-raw-v1}"
  TRACKER_BUCKET="${CALCITE_TRACKER_S3_BUCKET:-s3://govdata-tracker-v1}"
  RCLONE_REMOTE="r2"
fi

s3_to_rclone() {
  local path="${1#s3://}"
  path="${path%/}/"
  echo "${RCLONE_REMOTE}:${path}"
}
RCLONE_PARQUET="$(s3_to_rclone "$PARQUET_BUCKET")"
RCLONE_RAW="$(s3_to_rclone "$RAW_BUCKET")"
ICEBERG_PATH="${RCLONE_PARQUET}${SCHEMA}/${TABLE}"

DUCKDB_ENDPOINT_RAW="${S3_ENDPOINT:-${AWS_ENDPOINT_OVERRIDE:-}}"
DUCKDB_USE_SSL="true"
case "$DUCKDB_ENDPOINT_RAW" in
  http://*)  DUCKDB_USE_SSL="false" ;;
  https://*) DUCKDB_USE_SSL="true" ;;
esac
DUCKDB_ENDPOINT="${DUCKDB_ENDPOINT_RAW#https://}"
DUCKDB_ENDPOINT="${DUCKDB_ENDPOINT#http://}"
S3_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-auto}}"

S3_CONFIG="INSTALL httpfs; LOAD httpfs;"
S3_CONFIG="${S3_CONFIG} SET s3_access_key_id='${AWS_ACCESS_KEY_ID}';"
S3_CONFIG="${S3_CONFIG} SET s3_secret_access_key='${AWS_SECRET_ACCESS_KEY}';"
if [[ -n "$DUCKDB_ENDPOINT" ]]; then
  S3_CONFIG="${S3_CONFIG} SET s3_url_style='path'; SET s3_endpoint='${DUCKDB_ENDPOINT}'; SET s3_use_ssl=${DUCKDB_USE_SSL};"
fi
S3_CONFIG="${S3_CONFIG} SET s3_region='${S3_REGION}';"

# ── Banner ────────────────────────────────────────────────────────────────────
echo "=================================================="
echo "data_fix.sh"
echo "=================================================="
echo "  Env:       $ENV_NAME (rclone remote: ${RCLONE_REMOTE})"
echo "  Schema:    $SCHEMA"
echo "  Table:     $TABLE"
echo "  Iceberg:   ${PARQUET_BUCKET}/${SCHEMA}/${TABLE}"
echo "  Tracker:   $TRACKER_BUCKET"
if $DELETE_RAW; then
  echo "  Raw cache: ${RAW_BUCKET}/objects/${SCHEMA}/${TABLE}/ (years ${START_YEAR}-${STOP_YEAR})"
fi
$DRY_RUN && echo "  *** DRY RUN — no changes will be made ***"
echo ""

# ── Step 1: Drop Iceberg table directory ──────────────────────────────────────
echo "Step 1: Dropping Iceberg table directory..."
if $DRY_RUN; then
  echo "  [DRY RUN] Would purge: ${PARQUET_BUCKET}/${SCHEMA}/${TABLE}"
else
  rclone_ls_out=$(rclone ls "$ICEBERG_PATH" 2>/dev/null || true)
  FILE_COUNT=$(echo -n "$rclone_ls_out" | wc -l | tr -d ' ')
  if [[ -z "$rclone_ls_out" || "$FILE_COUNT" -eq 0 ]]; then
    echo "  No files found — skipping purge"
  else
    echo "  Purging $FILE_COUNT files from $ICEBERG_PATH ..."
    rclone purge "$ICEBERG_PATH" 2>/dev/null || true
    echo "  Done"
  fi
fi
echo ""

# ── Step 2: Invalidate tracker completion marker ──────────────────────────────
echo "Step 2: Invalidating tracker completion marker for '${TABLE}'..."
if $DRY_RUN; then
  echo "  [DRY RUN] Would write cleared marker to ${TRACKER_BUCKET}/year=0/source_key=_table_complete/"
else
  UUID_VAL=$(python3 -c "import uuid; print(uuid.uuid4())" 2>/dev/null \
             || uuidgen | tr '[:upper:]' '[:lower:]')
  AS_OF=$(python3 -c "import time; print(int(time.time() * 1000))" 2>/dev/null \
          || date +%s000)
  COMPLETION_PATH="${TRACKER_BUCKET}/year=0/source_key=_table_complete/${UUID_VAL}.parquet"
  duckdb -c "${S3_CONFIG}
    COPY (
      SELECT
        '_table_complete'::VARCHAR  AS source_key,
        '${TABLE}'::VARCHAR         AS table_name,
        'table_completion'::VARCHAR AS phase,
        'cleared'::VARCHAR          AS state,
        0::BIGINT                   AS row_count,
        NULL::VARCHAR               AS config_hash,
        NULL::VARCHAR               AS signature,
        NULL::VARCHAR               AS error_message,
        ${AS_OF}::BIGINT            AS as_of
    ) TO '${COMPLETION_PATH}' (FORMAT PARQUET);"
  echo "  Written: $COMPLETION_PATH"
fi
echo ""

# ── Step 3: Delete raw cache for year range (optional) ───────────────────────
if $DELETE_RAW; then
  echo "Step 3: Deleting raw cache for ${SCHEMA}/${TABLE} years ${START_YEAR}-${STOP_YEAR}..."
  ERRORS=0
  for (( year=START_YEAR; year<=STOP_YEAR; year++ )); do
    RAW_PATH="${RCLONE_RAW}objects/${SCHEMA}/${TABLE}/type=${TABLE}/year=${year}"
    if $DRY_RUN; then
      echo "  [DRY RUN] Would purge: ${RAW_BUCKET}/objects/${SCHEMA}/${TABLE}/type=${TABLE}/year=${year}"
    else
      raw_ls_out=$(rclone ls "$RAW_PATH" 2>/dev/null || true)
      COUNT=$(echo -n "$raw_ls_out" | wc -l | tr -d ' ')
      if [[ -z "$raw_ls_out" || "$COUNT" -eq 0 ]]; then
        echo "  year=${year}: no files — skipped"
      else
        rclone purge "$RAW_PATH" 2>/dev/null && echo "  year=${year}: deleted $COUNT files" \
          || { echo "  ERROR: failed to purge year=${year}"; ERRORS=$((ERRORS+1)); }
      fi
    fi
  done
  if [[ $ERRORS -gt 0 ]]; then
    echo "Completed with $ERRORS error(s)."
    exit 1
  fi
  echo ""
fi

# ── Step 4: Trigger ETL ───────────────────────────────────────────────────────
echo "Step 4: Triggering ETL for schema '${SCHEMA}'..."
if $DRY_RUN; then
  echo "  [DRY RUN] Would run: run-pool.sh --schema ${SCHEMA} daily"
else
  POOL_SCRIPT="${GOVDATA_HOME}/scripts/parallel/run-pool.sh"
  if [[ ! -f "$POOL_SCRIPT" ]]; then
    echo "  ERROR: run-pool.sh not found at $POOL_SCRIPT — run ETL manually"
    exit 1
  fi
  export GOVDATA_AUTO_DOWNLOAD=true
  bash "$POOL_SCRIPT" --schema "$SCHEMA" daily
fi
echo ""

echo "=================================================="
$DRY_RUN && echo "Dry run complete — no changes made" || echo "Fix and re-ingest complete"
echo "=================================================="
