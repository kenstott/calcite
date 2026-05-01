#!/usr/bin/env bash
# clear-batch-sentinel.sh — Delete the legacy "batch" sentinel tracker keys for specified years.
#
# Background: Before the batch filename parser fix, IcebergMaterializer wrote a single accession
# key "batch" to the tracker to represent ALL metadata_batch_*.parquet files for a year. This
# caused only 1 file to be materialized per year instead of all ~226. After the fix, each file
# gets its own unique accession key. But years that already have "batch" in the tracker will be
# skipped entirely on re-run (backward compat guard). This script deletes those sentinels so the
# materializer will rediscover and process all individual batch files for the specified years.
#
# Usage:
#   ./clear-batch-sentinel.sh [year1 year2 ...]      # specific years
#   ./clear-batch-sentinel.sh --all                  # all years found in tracker
#
# After running, re-run the affected workers to materialize the missing filings.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/../.env.prod"

if [[ -f "$ENV_FILE" ]]; then
  set -a; source "$ENV_FILE"; set +a
fi

: "${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID not set}"
: "${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY not set}"
: "${AWS_ENDPOINT_OVERRIDE:?AWS_ENDPOINT_OVERRIDE not set}"
: "${CALCITE_TRACKER_S3_BUCKET:?CALCITE_TRACKER_S3_BUCKET not set}"

BUCKET="${CALCITE_TRACKER_S3_BUCKET#s3://}"
ENDPOINT="${AWS_ENDPOINT_OVERRIDE}"

if [[ $# -eq 0 ]]; then
  echo "Usage: $0 [year1 year2 ...] | --all"
  exit 1
fi

# Resolve years to clear
YEARS=()
if [[ "$1" == "--all" ]]; then
  echo "Discovering all years in tracker bucket s3://${BUCKET}..."
  mapfile -t YEARS < <(python3 - <<PYEOF
import boto3, os
s3 = boto3.client('s3',
  endpoint_url=os.environ['AWS_ENDPOINT_OVERRIDE'],
  aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
  aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
  region_name='auto')
resp = s3.list_objects_v2(Bucket='${BUCKET}', Delimiter='/')
for p in sorted(resp.get('CommonPrefixes', [])):
  prefix = p['Prefix'].rstrip('/')
  if prefix.startswith('year='):
    print(prefix.replace('year=', ''))
PYEOF
)
else
  YEARS=("$@")
fi

echo "Years to clear batch sentinels for: ${YEARS[*]}"
echo ""

TOTAL_DELETED=0

for YEAR in "${YEARS[@]}"; do
  PREFIX="year=${YEAR}/source_key=batch/"
  echo "--- year=${YEAR}: listing s3://${BUCKET}/${PREFIX} ---"

  # List all objects under source_key=batch/ for this year
  KEYS=$(python3 - <<PYEOF
import boto3, os
s3 = boto3.client('s3',
  endpoint_url=os.environ['AWS_ENDPOINT_OVERRIDE'],
  aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
  aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
  region_name='auto')
paginator = s3.get_paginator('list_objects_v2')
for page in paginator.paginate(Bucket='${BUCKET}', Prefix='${PREFIX}'):
  for obj in page.get('Contents', []):
    print(obj['Key'])
PYEOF
)

  if [[ -z "$KEYS" ]]; then
    echo "  No batch sentinel found for year=${YEAR} — skipping"
    continue
  fi

  COUNT=$(echo "$KEYS" | wc -l | tr -d ' ')
  echo "  Found ${COUNT} sentinel object(s) — deleting..."

  python3 - <<PYEOF
import boto3, os
keys = """${KEYS}""".strip().splitlines()
s3 = boto3.client('s3',
  endpoint_url=os.environ['AWS_ENDPOINT_OVERRIDE'],
  aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
  aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
  region_name='auto')
# Delete in batches of 1000 (S3 API limit)
for i in range(0, len(keys), 1000):
  batch = [{'Key': k} for k in keys[i:i+1000]]
  resp = s3.delete_objects(Bucket='${BUCKET}', Delete={'Objects': batch})
  deleted = len(resp.get('Deleted', []))
  errors  = resp.get('Errors', [])
  print(f"  Deleted {deleted} object(s)" + (f", {len(errors)} error(s)" if errors else ""))
  for e in errors:
    print(f"  ERROR: {e}")
PYEOF

  TOTAL_DELETED=$((TOTAL_DELETED + COUNT))
  echo "  Done for year=${YEAR}"
done

echo ""
echo "================================================================"
echo "  Cleared batch sentinels: ${TOTAL_DELETED} object(s) deleted"
echo "  Affected years: ${YEARS[*]}"
echo "  Next step: re-run the workers for these years to materialize"
echo "  the missing filings into Iceberg."
echo "================================================================"
