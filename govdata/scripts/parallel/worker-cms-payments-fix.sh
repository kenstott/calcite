#!/usr/bin/env bash
# Targeted fix for cms_open_payments after switching from DKAN OFFSET to CSV bulk download.
#
# The previous ETL used DKAN with pageSize=5000 which exceeds the 500-row API cap,
# causing all batches to return 0 rows. This script purges the 0-row Iceberg fragments,
# clears the tracker entries for all three payment_type dimensions, and re-runs ETL
# using the new download.cms.gov CSV source.
#
# Usage:
#   ./worker-cms-payments-fix.sh [--dry-run]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

DRY_RUN=false
[ "${1:-}" = "--dry-run" ] && DRY_RUN=true

WORKER_ID="worker-cms-payments-fix"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

parquet_dir="${HEALTH_PARQUET_DIR:-${GOVDATA_PARQUET_DIR}/health}"
cache_dir="${HEALTH_CACHE_DIR:-${GOVDATA_CACHE_DIR}/health}"

run_or_dry() {
  if $DRY_RUN; then
    log_info "  DRY-RUN: $*"
  else
    "$@"
  fi
}

# 1. Purge 0-row Iceberg fragments
log_info "$WORKER_ID: purging Iceberg data for cms_open_payments"
run_or_dry rclone purge "r2:govdata-parquet-v1/health/cms_open_payments"

# 2. Clear tracker entries for all three payment_type dimension values
for sk in general research ownership; do
  log_info "$WORKER_ID: clearing tracker for source_key=$sk"
  if $DRY_RUN; then
    log_info "  DRY-RUN: rclone delete r2:govdata-tracker-v1 --include '/year=*/source_key=$sk/**'"
  else
    rclone delete r2:govdata-tracker-v1 \
      --include "/year=*/source_key=${sk}/**" 2>/dev/null || true
  fi
done

# 3. Generate model and run ETL
model_file="$MODEL_DIR/fix-cms-payments.json"
cat > "$model_file" <<ENDJSON
{
  "version": "1.0",
  "defaultSchema": "health",
  "schemas": [{
    "name": "health",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
    "operand": {
      "dataSource": "health",
      "directory": "${parquet_dir}",
      "cacheDirectory": "${cache_dir}",
      "autoDownload": true,
      "enabledTables": ["cms_open_payments"],
      "s3Config": {
        "accessKeyId": "${AWS_ACCESS_KEY_ID:-}",
        "secretAccessKey": "${AWS_SECRET_ACCESS_KEY:-}",
        "endpoint": "${AWS_ENDPOINT_OVERRIDE:-}",
        "region": "${AWS_REGION:-us-east-1}"
      }
    }
  }]
}
ENDJSON

if $DRY_RUN; then
  log_info "DRY-RUN: would run ETL with model $model_file"
else
  log_info "$WORKER_ID: running ETL for cms_open_payments"
  run_etl "$model_file" "$WORKER_ID"
fi

log_info "$WORKER_ID: done"
