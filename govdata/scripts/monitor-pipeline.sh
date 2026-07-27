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

# ── In-pipeline tracker counts ────────────────────────────────────────────────
# Postgres is the canonical ETL state store, so this is an indexed query over the tracker rows
# rather than a paginated LIST of marker objects. That also makes the breakdown exact: the old
# object-listing could only count source_key prefixes and had to report "state unknown".
# shellcheck source=/dev/null
source "$SCRIPT_DIR/tracker_pg.sh"
PG_NS="$(pg_ns_from_bucket "${GOVDATA_PARQUET_DIR:-s3://govdata-parquet-v1}")" || exit 2

ACTIVE_DAYS="${ACTIVE_DAYS:-7}"
CUTOFF_MS=$(( ($(date +%s) - ACTIVE_DAYS * 86400) * 1000 ))

echo "--- In-Flight Tracker (last ${ACTIVE_DAYS}d, schema ${PG_NS}) ---"
pg_tracker_exec "
  SELECT substring(source_key from 'year=([0-9]{4})') AS year,
         state,
         count(DISTINCT source_key) AS active_source_keys
    FROM \"${PG_NS}\".pipeline_tracker
   WHERE as_of >= ${CUTOFF_MS}
   GROUP BY 1, state
   ORDER BY 1, state;" || exit 2
