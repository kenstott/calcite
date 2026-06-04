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
# data_purge.sh — Purge tracker entries and Iceberg tables for a set of tables.
#
# Unlike data_fix.sh (which writes a "cleared" tracker marker and triggers ETL),
# data_purge.sh fully deletes:
#   1. The Iceberg table directory at <parquet bucket>/<schema>/<table>
#   2. All tracker entries matching year=*/source_key=<table>/* (both per-year
#      incremental markers and the _table_complete completion marker for that table)
#   3. With --raw: the local HTTP raw cache at /tmp/etl-raw-cache/<table>/
#      (use this when a prior run cached a corrupt/truncated upstream response
#      and the HttpSource keeps re-using it instead of refetching).
#
# It does NOT trigger ETL. The next run-pool invocation will re-ingest from
# scratch.
#
# Usage:
#   data_purge.sh --schema <schema> --tables <t1,t2,...> \
#                 [--env prod|dq] [--raw] [--dry-run]
#
# --env prod (default): targets R2 prod buckets via rclone remote `r2:`.
# --env dq:             targets MinIO DQ buckets via the rclone remote named in
#                       $GOVDATA_DQ_RCLONE_REMOTE (typically `minio`).
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GOVDATA_HOME="${GOVDATA_HOME:-$(dirname "$SCRIPT_DIR")}"

# ── Argument parsing ──────────────────────────────────────────────────────────
SCHEMA=""
TABLES_CSV=""
DRY_RUN=false
ENV_NAME="prod"
PURGE_RAW=false
RAW_CACHE_DIR="${ETL_RAW_CACHE_DIR:-/tmp/etl-raw-cache}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --schema)  SCHEMA="$2";     shift 2 ;;
    --tables)  TABLES_CSV="$2"; shift 2 ;;
    --table)   TABLES_CSV="$2"; shift 2 ;;  # alias for single table
    --env)     ENV_NAME="$2";   shift 2 ;;
    --raw)     PURGE_RAW=true;  shift   ;;
    --dry-run) DRY_RUN=true;    shift   ;;
    *) echo "Unknown argument: $1"; exit 1 ;;
  esac
done

if [[ -z "$SCHEMA" || -z "$TABLES_CSV" ]]; then
  cat <<EOF
Usage: data_purge.sh --schema <schema> --tables <t1,t2,...> [--env prod|dq] [--raw] [--dry-run]

Examples:
  data_purge.sh --schema patents --tables patent_claims,patent_summaries --env dq
  data_purge.sh --schema lands   --tables national_forests --env dq --raw
  data_purge.sh --schema fec     --tables committee_contributions --dry-run

Flags:
  --raw     Also delete the local HTTP raw cache at \$ETL_RAW_CACHE_DIR/<table>/
            (default: /tmp/etl-raw-cache). Use when a cached response is
            corrupt or stale and is being reused instead of refetching.
EOF
  exit 1
fi

if [[ "$ENV_NAME" != "prod" && "$ENV_NAME" != "dq" ]]; then
  echo "Error: --env must be 'prod' or 'dq' (got '$ENV_NAME')"
  exit 1
fi

# Split tables CSV into array
IFS=',' read -ra TABLES <<< "$TABLES_CSV"

# ── Load environment ──────────────────────────────────────────────────────────
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
  echo "Error: AWS credentials not set. Source .env.prod or .env.dq first."
  exit 1
fi

# ── Derived variables ─────────────────────────────────────────────────────────
if [[ "$ENV_NAME" == "dq" ]]; then
  : "${GOVDATA_DQ_BUCKET:?GOVDATA_DQ_BUCKET not set (check .env.dq)}"
  : "${GOVDATA_DQ_TRACKER_BUCKET:?GOVDATA_DQ_TRACKER_BUCKET not set (check .env.dq)}"
  : "${GOVDATA_DQ_RCLONE_REMOTE:?GOVDATA_DQ_RCLONE_REMOTE not set (check .env.dq)}"
  PARQUET_BUCKET="s3://${GOVDATA_DQ_BUCKET}"
  TRACKER_BUCKET="s3://${GOVDATA_DQ_TRACKER_BUCKET}"
  RCLONE_REMOTE="${GOVDATA_DQ_RCLONE_REMOTE}"
else
  PARQUET_BUCKET="${GOVDATA_PARQUET_DIR:-s3://govdata-parquet-v1}"
  TRACKER_BUCKET="${CALCITE_TRACKER_S3_BUCKET:-s3://govdata-tracker-v1}"
  RCLONE_REMOTE="r2"
fi

s3_to_rclone() {
  local path="${1#s3://}"
  path="${path%/}/"
  echo "${RCLONE_REMOTE}:${path}"
}
RCLONE_PARQUET="$(s3_to_rclone "$PARQUET_BUCKET")"
RCLONE_TRACKER="$(s3_to_rclone "$TRACKER_BUCKET")"

# ── Banner ────────────────────────────────────────────────────────────────────
echo "=================================================="
echo "data_purge.sh"
echo "=================================================="
echo "  Env:       $ENV_NAME (rclone remote: ${RCLONE_REMOTE})"
echo "  Schema:    $SCHEMA"
echo "  Tables:    ${TABLES[*]}"
echo "  Iceberg:   ${PARQUET_BUCKET}/${SCHEMA}/<table>"
echo "  Tracker:   ${TRACKER_BUCKET}/year=*/source_key=<table>/*"
$PURGE_RAW && echo "  Raw cache: ${RAW_CACHE_DIR}/<table>/"
$DRY_RUN && echo "  *** DRY RUN — no changes will be made ***"
echo ""

ERRORS=0

# ── Per-table purge ───────────────────────────────────────────────────────────
for TABLE in "${TABLES[@]}"; do
  TABLE="$(echo -n "$TABLE" | xargs)"  # trim whitespace
  [[ -z "$TABLE" ]] && continue

  echo "── Purging ${SCHEMA}/${TABLE} ──"
  ICEBERG_PATH="${RCLONE_PARQUET}${SCHEMA}/${TABLE}"

  # Step 1: Drop Iceberg table directory
  echo "  [1/2] Iceberg: ${PARQUET_BUCKET}/${SCHEMA}/${TABLE}"
  if $DRY_RUN; then
    echo "        [DRY RUN] Would purge: $ICEBERG_PATH"
  else
    rclone_ls_out=$(rclone ls "$ICEBERG_PATH" 2>/dev/null || true)
    FILE_COUNT=$(echo -n "$rclone_ls_out" | wc -l | tr -d ' ')
    if [[ -z "$rclone_ls_out" || "$FILE_COUNT" -eq 0 ]]; then
      echo "        No files found — skipping"
    else
      if rclone purge "$ICEBERG_PATH" 2>/dev/null; then
        echo "        Purged $FILE_COUNT files"
      else
        echo "        ERROR: failed to purge $ICEBERG_PATH"
        ERRORS=$((ERRORS+1))
      fi
    fi
  fi

  # Step 2: Delete tracker entries (per-year + completion marker)
  echo "  [2/2] Tracker: ${TRACKER_BUCKET}/year=*/source_key=${TABLE}/*"
  if $DRY_RUN; then
    echo "        [DRY RUN] Would delete tracker entries matching source_key=${TABLE}"
  else
    deleted=$(rclone delete "$RCLONE_TRACKER" \
                --include "/year=*/source_key=${TABLE}/*" \
                --stats-one-line --stats 0 2>&1 | tail -1 || true)
    echo "        ${deleted:-done}"
  fi

  # Step 3 (optional): Delete local raw HTTP cache
  if $PURGE_RAW; then
    RAW_PATH="${RAW_CACHE_DIR}/${TABLE}"
    echo "  [3/3] Raw cache: ${RAW_PATH}"
    if $DRY_RUN; then
      echo "        [DRY RUN] Would delete ${RAW_PATH}"
    elif [[ -d "$RAW_PATH" ]]; then
      raw_size=$(du -sh "$RAW_PATH" 2>/dev/null | cut -f1)
      if rm -rf "$RAW_PATH"; then
        echo "        Removed ${raw_size:-?}"
      else
        echo "        ERROR: failed to remove $RAW_PATH"
        ERRORS=$((ERRORS+1))
      fi
    else
      echo "        No cache directory found — skipping"
    fi
  fi
  echo ""
done

# ── Summary ───────────────────────────────────────────────────────────────────
echo "=================================================="
if $DRY_RUN; then
  echo "Dry run complete — no changes made"
elif [[ $ERRORS -gt 0 ]]; then
  echo "Purge completed with $ERRORS error(s)"
  exit 1
else
  echo "Purge complete"
fi
echo "=================================================="
