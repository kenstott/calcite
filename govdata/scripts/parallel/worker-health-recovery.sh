#!/usr/bin/env bash
# Targeted recovery for tables that regressed to 0 rows.
#
# Recovers:
#   clinical_trials            — DONE: stale 14-byte raw cache deleted; refetched successfully
#   clinical_trial_conditions  — DONE: same
#   clinical_trial_interventions — DONE: same
#   cms_open_payments          — DONE via worker-cms-payments-fix.sh (CSV source fix)
#
# NOTE: Group 1 (clinical trials) completed successfully. Do NOT re-run this script
# from the top — it will purge the successfully populated Iceberg data.
# For cms_open_payments retry use worker-cms-payments-fix.sh instead.
#
# Usage:
#   ./worker-health-recovery.sh [--dry-run]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

DRY_RUN=false
[ "${1:-}" = "--dry-run" ] && DRY_RUN=true

WORKER_ID="worker-health-recovery"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

parquet_dir="${HEALTH_PARQUET_DIR:-${GOVDATA_PARQUET_DIR}/health}"
cache_dir="${HEALTH_CACHE_DIR:-${GOVDATA_CACHE_DIR}/health}"

purge_iceberg() {
  local table="$1"
  log_info "$WORKER_ID: purging Iceberg data for $table"
  if $DRY_RUN; then
    log_info "  DRY-RUN: rclone purge r2:govdata-parquet-v1/health/$table"
  else
    rclone purge "r2:govdata-parquet-v1/health/$table" 2>/dev/null || true
  fi
}

clear_tracker() {
  local source_key="$1"
  log_info "$WORKER_ID: clearing tracker for source_key=$source_key"
  if $DRY_RUN; then
    log_info "  DRY-RUN: rclone delete r2:govdata-tracker-v1 --include '/year=*/source_key=$source_key/**'"
  else
    rclone delete r2:govdata-tracker-v1 \
      --include "/year=*/source_key=${source_key}/**" 2>/dev/null || true
  fi
}

run_recovery_etl() {
  local model_name="$1"
  local enabled_tables="$2"

  local model_file="$MODEL_DIR/${model_name}.json"
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
      "enabledTables": [${enabled_tables}],
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
    log_info "DRY-RUN: would run ETL for [$enabled_tables]"
    return
  fi

  log_info "$WORKER_ID: running ETL for [$enabled_tables]"
  run_etl "$model_file" "$WORKER_ID"
}

# ── 1. Clinical Trials (0-row regression from second rebuild run) ─────────────
# Iceberg was purged in the second rebuild but API returned 0 rows.
# Purge again to clear any 0-row fragments, then re-fetch.
purge_iceberg "clinical_trials"
purge_iceberg "clinical_trial_conditions"
purge_iceberg "clinical_trial_interventions"
run_recovery_etl "recovery-clinical-trials" \
  '"clinical_trials", "clinical_trial_conditions", "clinical_trial_interventions"'

# ── 2. CMS Open Payments (switched from broken DKAN OFFSET to CSV bulk download) ─────
# Previous ETL used DKAN with pageSize=5000 which exceeds the 500-row API cap → 0 rows.
# Now using direct download.cms.gov CSV files (no pagination needed).
# Purge 0-row Iceberg fragments and clear all three payment_type tracker keys.
purge_iceberg "cms_open_payments"
clear_tracker "general"
clear_tracker "research"
clear_tracker "ownership"
run_recovery_etl "recovery-cms-payments" '"cms_open_payments"'

log_info "$WORKER_ID: recovery complete"
