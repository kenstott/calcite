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
ENV_FILE="${SCRIPT_DIR}/../.env.prod"

if [[ -f "$ENV_FILE" ]]; then
  set -a; source "$ENV_FILE"; set +a
fi

: "${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID not set}"
: "${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY not set}"
: "${AWS_ENDPOINT_OVERRIDE:?AWS_ENDPOINT_OVERRIDE not set}"

GOVDATA_PARQUET_BASE="${GOVDATA_PARQUET_BASE:-s3://govdata-parquet-v1/sec}"
ENDPOINT="${AWS_ENDPOINT_OVERRIDE#https://}"
ENDPOINT="${ENDPOINT%/}"

NOW=$(date -u +"%Y-%m-%d %H:%M:%S UTC")

echo "================================================================"
echo "  Pipeline Progress Report — $NOW"
echo "  Warehouse: $GOVDATA_PARQUET_BASE"
echo "================================================================"
echo ""

# ── Materialized output counts via iceberg_scan ───────────────────────────────
# Pre-install extensions once; discard noisy "Success" output
duckdb -c "INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;" >/dev/null 2>&1 || true

S3_SETUP="
LOAD iceberg; LOAD httpfs;
CREATE OR REPLACE SECRET r2 (
  TYPE S3,
  KEY_ID '${AWS_ACCESS_KEY_ID}',
  SECRET '${AWS_SECRET_ACCESS_KEY}',
  ENDPOINT '${ENDPOINT}',
  REGION 'auto',
  USE_SSL true
);
SET unsafe_enable_version_guessing = true;
"

_strip_success() {
  python3 -c "
import sys, re
buf = sys.stdin.read()
# Remove any DuckDB result box that contains the word 'Success'
buf = re.sub(r'┌─+┐\n(?:[^\n]+\n)+└─+┘\n?',
             lambda m: '' if 'Success' in m.group(0) else m.group(0), buf)
sys.stdout.write(buf)
"
}
run_duckdb() { duckdb -c "${S3_SETUP}
$1" 2>&1 | _strip_success; }

echo "--- Raw Staging Filings by Year (metadata_batch*.parquet) ---"
run_duckdb "
SELECT year, count(*) AS raw_filings, count(DISTINCT cik) AS distinct_ciks
FROM read_parquet('${GOVDATA_PARQUET_BASE}/year=*/metadata_batch*.parquet',
                  hive_partitioning=true)
WHERE year >= 2019
GROUP BY year ORDER BY year;
" || echo "  (raw staging not found)"

echo ""
echo "--- Materialized Filings by Year (filing_metadata Iceberg) ---"
run_duckdb "
SELECT year, count(*) AS total_filings, count(DISTINCT filing_type) AS form_types
FROM iceberg_scan('${GOVDATA_PARQUET_BASE}/filing_metadata')
GROUP BY year ORDER BY year;
" || echo "  (filing_metadata not yet written)"

echo ""
echo "--- Output Table Row Counts (Iceberg) ---"
printf "%-28s %10s %6s\n" "table" "rows" "years"
printf "%-28s %10s %6s\n" "----------------------------" "----------" "------"

for tbl in filing_metadata financial_line_items filing_contexts mda_sections \
           xbrl_relationships earnings_transcripts insider_transactions; do
  result=$(run_duckdb "
SELECT '${tbl}' AS tbl,
       count(*) AS rows,
       count(DISTINCT year) AS years
FROM iceberg_scan('${GOVDATA_PARQUET_BASE}/${tbl}');
" 2>&1) || true
  if echo "$result" | grep -qE "Error:|error"; then
    printf "  %-26s  (not yet written)\n" "${tbl}"
  else
    counts=$(echo "$result" | grep "^│" | tail -1 | tr -d '│' | awk '{print $2, $3}')
    rows=$(echo "$counts" | awk '{print $1}')
    years=$(echo "$counts" | awk '{print $2}')
    printf "  %-26s %10s %6s\n" "${tbl}" "${rows}" "${years}"
  fi
done

echo ""

# ── In-pipeline tracker counts (metadata-only, no file download) ──────────────
: "${CALCITE_TRACKER_S3_BUCKET:?CALCITE_TRACKER_S3_BUCKET not set}"

ACTIVE_DAYS="${ACTIVE_DAYS:-7}"
export ACTIVE_DAYS
TRACKER_BUCKET="${CALCITE_TRACKER_S3_BUCKET}"
export TRACKER_BUCKET AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_ENDPOINT_OVERRIDE

python3 - << 'PYEOF'
import boto3, os, sys, time

endpoint    = os.environ['AWS_ENDPOINT_OVERRIDE']
ak          = os.environ['AWS_ACCESS_KEY_ID']
sk_cred     = os.environ['AWS_SECRET_ACCESS_KEY']
bucket      = os.environ['TRACKER_BUCKET'].replace('s3://', '')
active_days = int(os.environ.get('ACTIVE_DAYS', '7'))
cutoff_ts   = time.time() - active_days * 86400

s3 = boto3.client('s3', endpoint_url=endpoint,
                  aws_access_key_id=ak,
                  aws_secret_access_key=sk_cred,
                  region_name='auto')
paginator = s3.get_paginator('list_objects_v2')

resp = s3.list_objects_v2(Bucket=bucket, Delimiter='/')
years = sorted([p['Prefix'].rstrip('/').replace('year=', '')
                for p in resp.get('CommonPrefixes', [])])

rows = []
for y in years:
    # Count distinct source_key prefixes active within the cutoff window
    active_sks = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=f'year={y}/source_key='):
        for obj in page.get('Contents', []):
            k = obj['Key']
            if not k.endswith('.parquet'):
                continue
            if obj['LastModified'].timestamp() < cutoff_ts:
                continue
            parts = k.split('/')
            if len(parts) >= 2:
                active_sks.add(parts[1])
    rows.append({'year': y, 'active': len(active_sks)})

print(f"--- In-Flight Tracker (last {active_days}d, state unknown — approx) ---")
print(f"{'year':<8} {'active_source_keys':>20}")
print("-" * 32)
for r in rows:
    print(f"  {r['year']:<6} {r['active']:>20}")
PYEOF
