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
        source "$GOVDATA_HOME/$env_file"
        set +a
        break
    fi
done

if [[ -z "${AWS_ACCESS_KEY_ID:-}" ]]; then
    echo "Error: AWS credentials not set. Source .env.prod or .env.test first."
    exit 1
fi

S3_BUCKET="${GOVDATA_PARQUET_DIR:-s3://govdata-parquet-4}"
ICEBERG_PATH="$S3_BUCKET/sec/vectorized_chunks"
RCLONE_BUCKET="$(echo "$S3_BUCKET" | sed 's|^s3://|r2:|')"
OPERATING_DIR="${GOVDATA_HOME}/build/.aperio/sec"
PARTITION_DB="$OPERATING_DIR/.partition_status.duckdb"
VSS_DB="${VSS_DB:-$GOVDATA_HOME/build/.aperio/vss/chunks_vss.duckdb}"

echo "=============================================="
echo "Reset vectorized_chunks Pipeline"
echo "=============================================="
echo "S3 bucket:      $S3_BUCKET"
echo "Iceberg path:   $ICEBERG_PATH"
echo "Partition DB:   $PARTITION_DB"
echo "VSS DB:         $VSS_DB"
echo ""

# -----------------------------------------------
# Step 1: Delete staging *_chunks.parquet from S3
# -----------------------------------------------
echo "Step 1: Deleting staging *_chunks.parquet files from S3..."

DELETED=$(rclone ls "$RCLONE_BUCKET/sec/" --include "*_chunks.parquet" 2>/dev/null | wc -l | tr -d ' ' || echo 0)
rclone delete "$RCLONE_BUCKET/sec/" --include "*_chunks.parquet" 2>/dev/null || true

echo "  Deleted $DELETED staging chunk files"
echo ""

# -----------------------------------------------
# Step 2: Drop Iceberg table via PyIceberg
# -----------------------------------------------
echo "Step 2: Dropping vectorized_chunks Iceberg table..."

python3 - << 'PYEOF'
import os, sys

try:
    from pyiceberg.catalog import load_catalog

    s3_endpoint = os.environ.get("AWS_ENDPOINT_OVERRIDE", "")
    # Strip https:// for s3 endpoint config
    s3_endpoint_bare = s3_endpoint.replace("https://", "").replace("http://", "")

    catalog = load_catalog("default", **{
        "type": "rest",
        "uri": "http://localhost:8181",  # If REST catalog is available
    })
    catalog.drop_table("sec.vectorized_chunks")
    print("  Dropped Iceberg table sec.vectorized_chunks via REST catalog")
except Exception as e1:
    # Fallback: try in-memory catalog with S3 warehouse
    try:
        s3_bucket = os.environ.get("GOVDATA_PARQUET_DIR", "s3://govdata-parquet-4")
        warehouse = f"{s3_bucket}/sec".replace("s3://", "s3a://")

        catalog = load_catalog("default", **{
            "type": "in-memory",
            "warehouse": warehouse,
            "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
            "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
            "s3.endpoint": os.environ.get("AWS_ENDPOINT_OVERRIDE", ""),
            "s3.path-style-access": "true",
        })
        catalog.drop_table("default.vectorized_chunks")
        print("  Dropped Iceberg table via in-memory catalog")
    except Exception as e2:
        print(f"  PyIceberg drop failed: {e2}")
        print("  Falling back to deleting Iceberg metadata files from S3...")
        # Manual cleanup: delete the metadata directory
        import subprocess
        bucket = os.environ.get("GOVDATA_PARQUET_DIR", "s3://govdata-parquet-4")
        endpoint = os.environ.get("AWS_ENDPOINT_OVERRIDE", "")
        iceberg_meta = f"{bucket}/sec/vectorized_chunks/metadata/"
        result = subprocess.run(
            ["aws", "s3", "rm", iceberg_meta, "--recursive", "--endpoint-url", endpoint],
            capture_output=True, text=True
        )
        print(f"  Deleted Iceberg metadata: {result.stdout.count('delete:')} files")
PYEOF

echo ""

# -----------------------------------------------
# Step 3: Clear partition_status for vectorized_chunks
# -----------------------------------------------
echo "Step 3: Clearing partition status tracking..."

if [[ -f "$PARTITION_DB" ]]; then
    BEFORE=$(duckdb "$PARTITION_DB" -csv -noheader \
        "SELECT COUNT(*) FROM partition_status WHERE alternate_name LIKE '%vectorized_chunks%'" 2>/dev/null || echo "0")
    duckdb "$PARTITION_DB" \
        "DELETE FROM partition_status WHERE alternate_name LIKE '%vectorized_chunks%'" 2>/dev/null || true
    echo "  Cleared $BEFORE partition_status entries from $PARTITION_DB"
else
    echo "  No partition status DB found at $PARTITION_DB (will be created on next ETL run)"
fi

# Also clear table_completion if it exists
if [[ -f "$PARTITION_DB" ]]; then
    duckdb "$PARTITION_DB" \
        "DELETE FROM table_completion WHERE pipeline_name LIKE '%vectorized_chunks%'" 2>/dev/null || true
    echo "  Cleared table_completion entries"
fi

echo ""

# -----------------------------------------------
# Step 4: Reset VSS cache
# -----------------------------------------------
echo "Step 4: Resetting VSS cache..."

if [[ -f "$VSS_DB" ]]; then
    SIZE=$(ls -lh "$VSS_DB" | awk '{print $5}')
    rm -f "$VSS_DB" "${VSS_DB}.wal"
    echo "  Deleted VSS database ($SIZE): $VSS_DB"
else
    echo "  No local VSS database found at $VSS_DB"
fi

# Also clean S3 VSS cache if it exists
rclone deletefile "$RCLONE_BUCKET/cache/vss/chunks_vss.duckdb" 2>/dev/null && \
    echo "  Deleted S3 VSS cache: $S3_BUCKET/cache/vss/chunks_vss.duckdb" || \
    echo "  No S3 VSS cache found"

rclone deletefile "$RCLONE_BUCKET/cache/vss/metadata.json" 2>/dev/null && \
    echo "  Deleted S3 VSS metadata" || true

echo ""

# -----------------------------------------------
# Summary
# -----------------------------------------------
echo "=============================================="
echo "Reset Complete"
echo "=============================================="
echo ""
echo "Next steps:"
echo "  1. Run ETL to regenerate chunks:"
echo "     ./scripts/etl-full-schedule.sh"
echo ""
echo "  2. GPU pipeline will auto-generate embeddings"
echo "     (triggered by schema postProcess hook)"
echo ""
echo "  3. Rebuild VSS cache after embeddings complete:"
echo "     ./scripts/vss-rebuild-full.sh"
echo ""
