#!/bin/bash
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
# Reset all 7 SEC Iceberg tables and their tracker entries.
# Deletes Iceberg table directories from S3 and writes "cleared" tracker markers
# so the next ETL run performs a full rematerialization from staging parquet.
#
# Usage:
#   ./scripts/reset-sec-iceberg-tables.sh [--dry-run]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
GOVDATA_HOME="${GOVDATA_HOME:-$(dirname "$SCRIPT_DIR")}"

# Load environment
for env_file in .env.prod .env.test; do
    if [[ -f "$GOVDATA_HOME/$env_file" ]]; then
        echo "Loading $env_file"
        set -a
        # shellcheck source=/dev/null
        source "$GOVDATA_HOME/$env_file"
        set +a
        break
    fi
done

if [[ -z "${AWS_ACCESS_KEY_ID:-}" ]]; then
    echo "Error: AWS credentials not set. Source .env.prod or .env.test first."
    exit 1
fi

DRY_RUN=false
if [[ "${1:-}" == "--dry-run" ]]; then
    DRY_RUN=true
    echo "*** DRY RUN MODE — no changes will be made ***"
fi

S3_BUCKET="${GOVDATA_PARQUET_DIR:-s3://govdata-parquet-v1}"
S3_ENDPOINT="${AWS_ENDPOINT_OVERRIDE:-}"
TRACKER_BUCKET="${CALCITE_TRACKER_S3_BUCKET:-s3://govdata-tracker-v1}"

ICEBERG_BASE="$S3_BUCKET/source=sec/SEC"

SEC_TABLES=(
    "filing_contexts"
    "filing_metadata"
    "financial_line_items"
    "mda_sections"
    "stock_prices"
    "vectorized_chunks"
    "xbrl_relationships"
)

ENDPOINT_ARGS=""
if [[ -n "$S3_ENDPOINT" ]]; then
    ENDPOINT_ARGS="--endpoint-url $S3_ENDPOINT"
fi

echo "=============================================="
echo "Reset All SEC Iceberg Tables"
echo "=============================================="
echo "Iceberg base: $ICEBERG_BASE"
echo "Tracker:      $TRACKER_BUCKET"
echo "Tables:       ${SEC_TABLES[*]}"
echo ""

# -----------------------------------------------
# Step 1: Delete Iceberg table directories from S3
# -----------------------------------------------
echo "Step 1: Deleting Iceberg table directories..."

for table in "${SEC_TABLES[@]}"; do
    iceberg_path="$ICEBERG_BASE/$table"
    if $DRY_RUN; then
        echo "  [DRY RUN] Would delete: $iceberg_path"
    else
        echo "  Deleting $iceberg_path ..."
        # shellcheck disable=SC2086
        DELETED=$(aws s3 rm "$iceberg_path" --recursive $ENDPOINT_ARGS 2>&1 | grep -c "^delete:" || true)
        echo "    Deleted $DELETED files"
    fi
done

echo ""

# -----------------------------------------------
# Step 2: Write table_completion "cleared" markers via DuckDB CLI
# Mirrors S3HivePipelineTracker.invalidateTableCompletion().
# This breaks the watermark fast-path so the next run re-materializes.
#
# Incremental partition markers are NOT cleared here. The materialization
# code treats a missing Iceberg table as "no committed accessions" and
# ignores the tracker entirely, so stale incremental entries are harmless.
# -----------------------------------------------
echo "Step 2: Writing table_completion cleared markers..."

# Strip protocol from endpoint for DuckDB s3_endpoint setting
DUCKDB_ENDPOINT="${S3_ENDPOINT:-}"
DUCKDB_ENDPOINT="${DUCKDB_ENDPOINT#https://}"
DUCKDB_ENDPOINT="${DUCKDB_ENDPOINT#http://}"

# Build DuckDB S3 config block
S3_CONFIG="INSTALL httpfs; LOAD httpfs;"
S3_CONFIG="$S3_CONFIG SET s3_access_key_id='${AWS_ACCESS_KEY_ID}';"
S3_CONFIG="$S3_CONFIG SET s3_secret_access_key='${AWS_SECRET_ACCESS_KEY}';"
if [[ -n "$DUCKDB_ENDPOINT" ]]; then
    S3_CONFIG="$S3_CONFIG SET s3_url_style='path'; SET s3_endpoint='${DUCKDB_ENDPOINT}';"
fi
if [[ -n "${AWS_DEFAULT_REGION:-}" ]]; then
    S3_CONFIG="$S3_CONFIG SET s3_region='${AWS_DEFAULT_REGION}';"
fi

if $DRY_RUN; then
    echo "  [DRY RUN] Would write table_completion cleared markers for: ${SEC_TABLES[*]}"
else
    ERRORS=0
    AS_OF=$(date +%s%3N)   # epoch millis

    for table in "${SEC_TABLES[@]}"; do
        UUID_VAL=$(python3 -c "import uuid; print(uuid.uuid4())" 2>/dev/null \
                   || cat /proc/sys/kernel/random/uuid 2>/dev/null \
                   || uuidgen | tr '[:upper:]' '[:lower:]')

        COMPLETION_PATH="${TRACKER_BUCKET}/year=0/source_key=_table_complete/${UUID_VAL}.parquet"
        duckdb -c "${S3_CONFIG}
            COPY (
                SELECT
                    '_table_complete'::VARCHAR  AS source_key,
                    '${table}'::VARCHAR         AS table_name,
                    'table_completion'::VARCHAR AS phase,
                    'cleared'::VARCHAR          AS state,
                    0::BIGINT                   AS row_count,
                    NULL::VARCHAR               AS config_hash,
                    NULL::VARCHAR               AS signature,
                    NULL::VARCHAR               AS error_message,
                    ${AS_OF}::BIGINT            AS as_of
            ) TO '${COMPLETION_PATH}' (FORMAT PARQUET);" \
            && echo "  ${table}: OK" \
            || { echo "  ERROR: failed for ${table}"; ERRORS=$((ERRORS+1)); }
    done

    if [[ $ERRORS -gt 0 ]]; then
        echo "Completed with $ERRORS error(s). Check output above."
        exit 1
    fi
    echo "All cleared markers written successfully."
fi

echo ""
echo "=============================================="
echo "Reset Complete"
echo "=============================================="
echo ""
echo "Next steps:"
echo "  1. Restart worker processes to pick up code changes"
echo "  2. Run ETL to trigger full rematerialization:"
echo "     ./scripts/etl-full-schedule.sh"
echo ""
echo "  On next run, IcebergMaterializer will:"
echo "    - Find no existing Iceberg tables (Iceberg scan returns 0 accessions)"
echo "    - Deduplicate staging parquet via QUALIFY ROW_NUMBER() in write SQL"
echo "    - Track committed accessions per-run to prevent cross-chunk duplicates"
echo ""
