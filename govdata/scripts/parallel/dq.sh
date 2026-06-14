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
# Run data quality checks for a schema:
#   1. Reingest the DQ year window via data-fix.sh (current year + lookback)
#   2. Execute {schema}_dq.sql via DuckDB with envsubst variable expansion
#   3. Write results parquet to s3://${GOVDATA_DQ_TRACKER_BUCKET:-govdata-tracker-v1-dq}/dq-results/
#
# The DQ year window defaults to current year + 1 prior year (lookback=1).
# SQL files may override the default with a header comment:
#   -- dq-lookback: 2
# Longer lookbacks cover schemas with data lags or multi-year cadences.
#
# Usage:
#   dq.sh --schema fec
#   dq.sh --schema crime --lookback 2
#   dq.sh --schema edu   --no-reingest   # skip data-fix, re-run SQL only
#   dq.sh --schema econ  --dry-run
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DQ_SQL_DIR="$(cd "$SCRIPT_DIR/.." && pwd)/scripts"
source "$SCRIPT_DIR/common.sh"
load_env

# ── Argument parsing ──────────────────────────────────────────────────────────

SCHEMA=""
LOOKBACK_OVERRIDE=""
NO_REINGEST=false
DRY_RUN=false

while [ $# -gt 0 ]; do
  case "$1" in
    --schema)
      SCHEMA="${2:?--schema requires a value}"; shift 2 ;;
    --lookback)
      LOOKBACK_OVERRIDE="${2:?--lookback requires a number}"; shift 2 ;;
    --no-reingest)
      NO_REINGEST=true; shift ;;
    --dry-run)
      DRY_RUN=true; shift ;;
    *)
      echo "ERROR: unknown argument '$1'" >&2
      echo "Usage: $0 --schema <name> [--lookback N] [--no-reingest] [--dry-run]" >&2
      exit 1 ;;
  esac
done

if [ -z "$SCHEMA" ]; then
  echo "ERROR: --schema is required" >&2
  echo "Usage: $0 --schema <name> [--lookback N] [--no-reingest] [--dry-run]" >&2
  exit 1
fi

# ── Locate SQL file ───────────────────────────────────────────────────────────

SQL_FILE="${DQ_SQL_DIR}/${SCHEMA}_dq.sql"
if [ ! -f "$SQL_FILE" ]; then
  echo "ERROR: DQ SQL file not found: $SQL_FILE" >&2
  exit 1
fi

# ── Determine year window ─────────────────────────────────────────────────────
# Default lookback from SQL file header comment: -- dq-lookback: N
# CLI --lookback overrides the file default.

FILE_LOOKBACK=$(grep -m1 '^-- dq-lookback:' "$SQL_FILE" 2>/dev/null | awk '{print $3}' || true)
LOOKBACK="${LOOKBACK_OVERRIDE:-${FILE_LOOKBACK:-1}}"

DQ_YEAR_END=$(date +%Y)
DQ_YEAR_START=$((DQ_YEAR_END - LOOKBACK))

log_info "dq: schema=$SCHEMA window=${DQ_YEAR_START}–${DQ_YEAR_END} lookback=${LOOKBACK}${DRY_RUN:+ DRY_RUN}${NO_REINGEST:+ NO_REINGEST}"

# ── Step 1: Reingest DQ window ────────────────────────────────────────────────
# Daily mode covers the current (incremental) year.
# Historical mode covers prior years in the lookback window.
# data-fix.sh is called as a subprocess (not exec) so dq.sh continues after.

if ! $NO_REINGEST; then
  log_info "dq: reingest current year ($DQ_YEAR_END) via daily mode"
  if $DRY_RUN; then
    log_info "  [dry-run] bash data-fix.sh $SCHEMA daily"
  else
    bash "$SCRIPT_DIR/data-fix.sh" "$SCHEMA" daily
  fi

  if [ "$DQ_YEAR_START" -lt "$DQ_YEAR_END" ]; then
    log_info "dq: reingest prior years ($DQ_YEAR_START–$((DQ_YEAR_END - 1))) via historical mode"
    if $DRY_RUN; then
      log_info "  [dry-run] GOVDATA_START_YEAR=$DQ_YEAR_START bash data-fix.sh $SCHEMA historical"
    else
      export GOVDATA_START_YEAR="$DQ_YEAR_START"
      bash "$SCRIPT_DIR/data-fix.sh" "$SCHEMA" historical
    fi
  fi
fi

# ── Step 2: Execute DQ SQL ────────────────────────────────────────────────────
# Expand shell variables in the SQL file (credentials, DQ year bounds),
# append a COPY statement to persist results, then pipe to DuckDB.

RUN_DATE=$(date +%Y-%m-%d)
RUN_MODE="daily"
RESULT_PATH="s3://${GOVDATA_DQ_TRACKER_BUCKET:-govdata-tracker-v1-dq}/dq-results/schema=${SCHEMA}/run_date=${RUN_DATE}/type=${RUN_MODE}/results.parquet"

export DQ_YEAR_START DQ_YEAR_END

COPY_SQL="COPY dq_results TO '${RESULT_PATH}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE);"

log_info "dq: running SQL — results → $RESULT_PATH"

if $DRY_RUN; then
  log_info "  [dry-run] envsubst < $SQL_FILE | duckdb  (+ COPY to $RESULT_PATH)"
  exit 0
fi

# ── S3 endpoint config — DQ always targets the MinIO DQ instance, never R2 ─────
# The endpoint must come from AWS_ENDPOINT_OVERRIDE (set in govdata/.env.dq).
# Derive the scheme-stripped host and the SSL flag for DuckDB, then inject one
# correct S3 config block while stripping each SQL file's own (R2-hardcoded)
# SET s3_* lines — so the unchanged per-schema scripts run against MinIO.
if [ -z "${AWS_ENDPOINT_OVERRIDE:-}" ]; then
  echo "ERROR: AWS_ENDPOINT_OVERRIDE is not set — DQ requires the MinIO endpoint (source govdata/.env.dq)" >&2
  exit 1
fi
S3_ENDPOINT_HOST="${AWS_ENDPOINT_OVERRIDE#http://}"; S3_ENDPOINT_HOST="${S3_ENDPOINT_HOST#https://}"; S3_ENDPOINT_HOST="${S3_ENDPOINT_HOST%/}"
case "$AWS_ENDPOINT_OVERRIDE" in https://*) S3_USE_SSL=true ;; *) S3_USE_SSL=false ;; esac
S3_REGION="${AWS_REGION:-us-east-1}"

{
  cat <<S3CONF
INSTALL httpfs; LOAD httpfs;
INSTALL iceberg; LOAD iceberg;
SET s3_access_key_id='${AWS_ACCESS_KEY_ID}';
SET s3_secret_access_key='${AWS_SECRET_ACCESS_KEY}';
SET s3_region='${S3_REGION}';
SET s3_endpoint='${S3_ENDPOINT_HOST}';
SET s3_url_style='path';
SET s3_use_ssl=${S3_USE_SSL};
SET memory_limit='${DUCKDB_MEMORY_LIMIT:-2GB}';
SET temp_directory='/tmp/duckdb-dq-${SCHEMA}';
SET max_temp_directory_size='50GB';
S3CONF
  envsubst '$AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY $AWS_ENDPOINT_OVERRIDE $DQ_YEAR_START $DQ_YEAR_END' \
    < "$SQL_FILE" \
    | grep -ivE '^[[:space:]]*SET[[:space:]]+s3_(access_key_id|secret_access_key|region|endpoint|url_style|use_ssl)[[:space:]]*='
  echo ""
  echo "$COPY_SQL"
} | duckdb

log_info "dq: complete — schema=$SCHEMA run_date=$RUN_DATE"
log_info "dq: view results with: dq-report.sh --schemas $SCHEMA --run-date $RUN_DATE"
