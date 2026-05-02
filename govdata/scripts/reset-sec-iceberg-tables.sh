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
# Step 2: Write "cleared" tracker markers via Python
# Mirrors S3HivePipelineTracker.invalidateAll() and invalidateTableCompletion().
# Writes append-only parquet markers so the tracker treats these tables as unprocessed.
# -----------------------------------------------
echo "Step 2: Writing tracker cleared markers..."

if $DRY_RUN; then
    echo "  [DRY RUN] Would write cleared markers for all 7 tables (partition_status + table_completion)"
    echo "  Tables: ${SEC_TABLES[*]}"
else
python3 - << PYEOF
import os
import sys
import uuid
import time
import io
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from urllib.parse import urlparse

access_key = os.environ["AWS_ACCESS_KEY_ID"]
secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]
endpoint    = os.environ.get("AWS_ENDPOINT_OVERRIDE", "")
region      = os.environ.get("AWS_DEFAULT_REGION", "auto")
tracker_url = os.environ.get("CALCITE_TRACKER_S3_BUCKET", "s3://govdata-tracker-v1")

parsed = urlparse(tracker_url)
tracker_bucket = parsed.netloc
tracker_prefix = parsed.path.lstrip("/")

boto_kwargs = dict(
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name=region,
    config=Config(signature_version="s3v4"),
)
if endpoint:
    boto_kwargs["endpoint_url"] = endpoint

s3 = boto3.client("s3", **boto_kwargs)

tables = [
    "filing_contexts",
    "filing_metadata",
    "financial_line_items",
    "mda_sections",
    "stock_prices",
    "vectorized_chunks",
    "xbrl_relationships",
]

SCHEMA = pa.schema([
    pa.field("source_key",     pa.string()),
    pa.field("table_name",     pa.string()),
    pa.field("phase",          pa.string()),
    pa.field("state",          pa.string()),
    pa.field("row_count",      pa.int64()),
    pa.field("config_hash",    pa.string()),
    pa.field("signature",      pa.string()),
    pa.field("error_message",  pa.string()),
    pa.field("as_of",          pa.int64()),
])

def write_marker(source_key, table_name, phase, state, year):
    as_of = int(time.time() * 1000)
    batch = pa.record_batch({
        "source_key":    [source_key],
        "table_name":    [table_name],
        "phase":         [phase],
        "state":         [state],
        "row_count":     [0],
        "config_hash":   [None],
        "signature":     [None],
        "error_message": [None],
        "as_of":         [as_of],
    }, schema=SCHEMA)

    buf = io.BytesIO()
    pq.write_table(pa.Table.from_batches([batch]), buf)
    buf.seek(0)

    safe_key = source_key.replace("/", "_").replace(" ", "_").replace(":", "_")
    key_parts = [tracker_prefix] if tracker_prefix else []
    key_parts += [f"year={year}", f"source_key={safe_key}", f"{uuid.uuid4()}.parquet"]
    s3_key = "/".join(key_parts)

    s3.put_object(Bucket=tracker_bucket, Key=s3_key, Body=buf.read())
    print(f"  Wrote cleared marker: s3://{tracker_bucket}/{s3_key}")

errors = 0
for table in tables:
    # invalidateTableCompletion: year=0, source_key=_table_complete
    try:
        write_marker("_table_complete", table, "table_completion", "cleared", "0")
    except Exception as e:
        print(f"  ERROR writing table_completion cleared for {table}: {e}", file=sys.stderr)
        errors += 1

    # invalidateAll for incremental phase:
    # Read all completed source_keys for this table and write cleared markers.
    try:
        import duckdb
        con = duckdb.connect()
        if endpoint:
            con.execute("INSTALL httpfs; LOAD httpfs;")
            con.execute(f"SET s3_url_style='path';")
            con.execute(f"SET s3_endpoint='{endpoint.replace('https://','').replace('http://','')}';")
            con.execute(f"SET s3_access_key_id='{access_key}';")
            con.execute(f"SET s3_secret_access_key='{secret_key}';")
            con.execute(f"SET s3_region='{region}';")
        else:
            con.execute("INSTALL httpfs; LOAD httpfs;")
            con.execute(f"SET s3_access_key_id='{access_key}';")
            con.execute(f"SET s3_secret_access_key='{secret_key}';")
            con.execute(f"SET s3_region='{region}';")

        glob = f"{tracker_url}/year=*/source_key=*/*.parquet"
        sql = f"""
            SELECT DISTINCT source_key FROM (
              SELECT source_key, state,
                ROW_NUMBER() OVER (PARTITION BY source_key ORDER BY as_of DESC) AS rn
              FROM read_parquet('{glob}', hive_partitioning=false, union_by_name=true)
              WHERE table_name = '{table}' AND phase = 'incremental'
            ) WHERE rn = 1 AND state = 'complete'
        """
        rows = con.execute(sql).fetchall()
        cleared = 0
        for (source_key,) in rows:
            # Extract year from source_key (e.g. "2024" or "year=2024__accession=...")
            year = "9999"
            if source_key:
                idx = source_key.find("year=")
                if idx >= 0:
                    digits = ""
                    for c in source_key[idx+5:]:
                        if c.isdigit():
                            digits += c
                        else:
                            break
                    if len(digits) == 4:
                        year = digits
                elif len(source_key) == 4 and source_key.isdigit():
                    year = source_key
            write_marker(source_key, table, "incremental", "cleared", year)
            cleared += 1
        con.close()
        print(f"  {table}: cleared {cleared} incremental partition markers")
    except Exception as e:
        print(f"  WARN: DuckDB scan failed for {table}, skipping incremental markers: {e}")
        # table_completion cleared marker is still written — enough to break the watermark fast-path

if errors:
    print(f"\nCompleted with {errors} error(s). Check output above.", file=sys.stderr)
    sys.exit(1)
else:
    print("\nAll cleared markers written successfully.")
PYEOF
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
