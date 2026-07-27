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

# Postgres is the canonical ETL state store; its tracker schema is derived from the parquet bucket.
# shellcheck source=/dev/null
source "$SCRIPT_DIR/tracker_pg.sh"
PG_NS="$(pg_ns_from_bucket "$S3_BUCKET")" || exit 2

ICEBERG_BASE="$S3_BUCKET/sec"
# rclone uses r2: prefix instead of s3:// — convert once for all path operations
RCLONE_ICEBERG_BASE="$(echo "$ICEBERG_BASE" | sed 's|^s3://|r2:|')"

SEC_TABLES=(
    "filing_contexts"
    "filing_metadata"
    "financial_line_items"
    "mda_sections"
    "stock_prices"
    "vectorized_chunks"
    "xbrl_relationships"
)

echo "=============================================="
echo "Reset All SEC Iceberg Tables"
echo "=============================================="
echo "Iceberg base: $ICEBERG_BASE"
echo "Tracker:      $PG_NS (postgres)"
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
        rclone_path="${RCLONE_ICEBERG_BASE}/${table}"
        DELETED=$(rclone ls "$rclone_path" 2>/dev/null | wc -l | tr -d ' ' || echo 0)
        rclone purge "$rclone_path" 2>/dev/null || true
        echo "    Deleted $DELETED files"
    fi
done

echo ""

# -----------------------------------------------
# Step 2: Invalidate each table's completion record in the tracker.
# This breaks the watermark fast-path so the next run re-materializes.
#
# Incremental partition markers are NOT cleared here. The materialization
# code treats a missing Iceberg table as "no committed accessions" and
# ignores the tracker entirely, so stale incremental entries are harmless.
# -----------------------------------------------
echo "Step 2: Clearing table_completion records in ${PG_NS}..."

ERRORS=0
for table in "${SEC_TABLES[@]}"; do
    if pg_tracker_clear_completion "$PG_NS" "$table" "$($DRY_RUN && echo true || echo false)"; then
        :
    else
        echo "  ERROR: failed for ${table}"
        ERRORS=$((ERRORS+1))
    fi
done

if [[ $ERRORS -gt 0 ]]; then
    echo "Completed with $ERRORS error(s). Check output above."
    exit 1
fi
$DRY_RUN || echo "All completion records cleared successfully."

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
