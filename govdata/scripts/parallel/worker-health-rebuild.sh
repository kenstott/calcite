#!/usr/bin/env bash
# Targeted rebuild for specific health tables that need re-ETL.
#
# Tables rebuilt:
#   cms_open_payments          — migrated from Socrata (404) to DKAN URL
#   clinical_trials            — only had 1 cursor page; full re-fetch needed
#   clinical_trial_conditions  — same
#   clinical_trial_interventions — same
#   medicaid_drug_utilization  — potential partial fetch from prior ETL
#   cms_hospital_quality       — potential partial fetch from prior ETL
#   cdc_brfss                  — break_out/break_out_category fix
#
# Steps per table:
#   1. Purge Iceberg parquet on R2
#   2. Delete tracker entries for that table's source_key(s)
#   3. Run ETL (autoDownload=true will re-fetch)
#
# Usage:
#   ./worker-health-rebuild.sh [--dry-run]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

DRY_RUN=false
[ "${1:-}" = "--dry-run" ] && DRY_RUN=true

WORKER_ID="worker-health-rebuild"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

parquet_dir="${HEALTH_PARQUET_DIR:-${GOVDATA_PARQUET_DIR}/health}"
cache_dir="${HEALTH_CACHE_DIR:-${GOVDATA_CACHE_DIR}/health}"

# ── helper: purge one Iceberg table and its tracker entries ──────────────────
purge_table() {
  local table="$1"
  shift
  # $@ = additional source_keys beyond the table name itself

  log_info "$WORKER_ID: purging Iceberg data for $table"
  if $DRY_RUN; then
    log_info "  DRY-RUN: rclone purge r2:govdata-parquet-v1/health/$table"
  else
    rclone purge "r2:govdata-parquet-v1/health/$table" 2>/dev/null || true
  fi

  # Standard tracker key: source_key = table name (for type=<tableName> dimensions)
  log_info "$WORKER_ID: deleting tracker entries for source_key=$table"
  if $DRY_RUN; then
    log_info "  DRY-RUN: rclone delete r2:govdata-tracker-v1 --include '/year=*/source_key=$table/**'"
  else
    rclone delete r2:govdata-tracker-v1 \
      --include "/year=*/source_key=${table}/**" 2>/dev/null || true
  fi

  # Additional source_keys (e.g. cms_open_payments uses payment_type values, not table name)
  for sk in "$@"; do
    log_info "$WORKER_ID: deleting tracker entries for source_key=$sk"
    if $DRY_RUN; then
      log_info "  DRY-RUN: rclone delete r2:govdata-tracker-v1 --include '/year=*/source_key=$sk/**'"
    else
      rclone delete r2:govdata-tracker-v1 \
        --include "/year=*/source_key=${sk}/**" 2>/dev/null || true
    fi
  done
}

# ── helper: run ETL for a set of tables ──────────────────────────────────────
run_health_rebuild_model() {
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

# ── 1. CMS Open Payments (Socrata→DKAN fix) ───────────────────────────────────
# source_keys are the payment_type dimension values (general/research/ownership)
purge_table "cms_open_payments" "general" "research" "ownership"
run_health_rebuild_model "rebuild-cms-payments" '"cms_open_payments"'

# ── 2. Clinical Trials (cursor pagination — was 1000 rows) ───────────────────
purge_table "clinical_trials"
purge_table "clinical_trial_conditions"
purge_table "clinical_trial_interventions"
run_health_rebuild_model "rebuild-clinical-trials" \
  '"clinical_trials", "clinical_trial_conditions", "clinical_trial_interventions"'

# ── 3. Medicaid + CMS Hospital Quality (potential partial DKAN fetch) ─────────
purge_table "medicaid_drug_utilization"
purge_table "cms_hospital_quality"
run_health_rebuild_model "rebuild-cms-medicaid" \
  '"medicaid_drug_utilization", "cms_hospital_quality"'

# ── 4. CDC BRFSS (break_out / break_out_category fix) ─────────────────────────
purge_table "cdc_brfss"
run_health_rebuild_model "rebuild-cdc-brfss" '"cdc_brfss"'

log_info "$WORKER_ID: all targeted rebuilds complete"
