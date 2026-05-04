#!/usr/bin/env bash
# Health ETL worker — parameterized by MODE.
#
# Usage:
#   worker-health.sh <mode>
#
# Modes:
#   initial   One-time setup: all 15 health tables without incremental filters.
#             Run once before any recurring cadence workers.
#
#   daily     Incremental clinical trials delta.
#             Requires: HEALTH_TRIALS_SINCE_DATE (e.g. "2024-01-01"; leave blank for full fetch)
#             Schedule: every 24 hours.
#
#   weekly    CDC COVID vaccinations delta + CDC mortality full refresh.
#             Optional: HEALTH_CDC_COVID_SINCE_DATE (blank = full fetch)
#             Schedule: weekly (e.g., Monday 03:00 UTC).
#
#   monthly   Stable reference tables: BRFSS, Medicaid drug utilization, CMS, FDA, RxNorm.
#             Optional: HEALTH_BRFSS_SINCE_YEAR, MEDICAID_SINCE_YEAR, MEDICAID_SINCE_QUARTER
#             Schedule: monthly (e.g., 1st of month 02:00 UTC).
#
# Required env vars (set in .env.prod or equivalent):
#   HEALTH_PARQUET_DIR      Local/S3 path for Parquet output (overrides GOVDATA_PARQUET_DIR/source=health)
#   HEALTH_CACHE_DIR        Local/S3 path for raw download cache
#
# Optional env vars:
#   HEALTH_TRIALS_SINCE_DATE      ISO date for clinical trials delta (e.g. "2024-01-01")
#   HEALTH_CDC_COVID_SINCE_DATE   ISO date for CDC COVID vaccinations delta
#   HEALTH_BRFSS_SINCE_YEAR       4-digit year for BRFSS incremental load
#   MEDICAID_SINCE_YEAR           4-digit year for Medicaid drug utilization incremental load
#   MEDICAID_SINCE_QUARTER        Quarter number (1-4) for Medicaid incremental load
#   HEALTH_FDA_API_KEY            openFDA API key (optional; improves rate limits)
#   MEDICAID_DRUG_UTIL_DATASET_ID Medicaid dataset UUID (default: d890d3a9-...; 2023 data)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

MODE="${1:-}"
if [ -z "$MODE" ]; then
  echo "Usage: $0 <initial|daily|weekly|monthly> [--force]" >&2
  exit 1
fi

FORCE=false
for arg in "${@:2}"; do
  [ "$arg" = "--force" ] && FORCE=true
done

WORKER_ID="worker-health-${MODE}"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

HEALTH_SCHEMA_YAML="$GOVDATA_ROOT/src/main/resources/health/health-schema.yaml"

# ── helpers ──────────────────────────────────────────────────────────────────

run_health_model() {
  local model_name=$1 enabled_tables=$2
  shift 2
  local extra_operands="${1:-}"

  local model_file="$MODEL_DIR/${model_name}.json"
  local extra_json=""
  [ -n "$extra_operands" ] && extra_json=",
      ${extra_operands}"

  # Use HEALTH_PARQUET_DIR if set; fall back to GOVDATA_PARQUET_DIR/source=health
  local parquet_dir="${HEALTH_PARQUET_DIR:-${GOVDATA_PARQUET_DIR}/source=health}"
  local cache_dir="${HEALTH_CACHE_DIR:-${GOVDATA_CACHE_DIR}/health}"

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
      "enabledTables": [${enabled_tables}]${extra_json},
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

  log_info "$WORKER_ID: running $model_name"
  run_etl "$model_file" "$WORKER_ID"
}

# ── modes ─────────────────────────────────────────────────────────────────────

INCREMENTAL_YEAR=${GOVDATA_INCREMENTAL_START_YEAR:-2026}
INCREMENTAL_DATE="${INCREMENTAL_YEAR}-01-01"

case "$MODE" in

  initial)
    UNTIL_DATE="$((INCREMENTAL_YEAR - 1))-12-31"
    UNTIL_YEAR=$((INCREMENTAL_YEAR - 1))
    export HEALTH_TRIALS_UNTIL_DATE="${UNTIL_DATE}"
    export HEALTH_CDC_COVID_UNTIL_DATE="${UNTIL_DATE}"
    export HEALTH_BRFSS_UNTIL_YEAR="${UNTIL_YEAR}"
    export MEDICAID_UNTIL_YEAR="${UNTIL_YEAR}"
    # Full fetch of all 15 health tables — capped at GOVDATA_INCREMENTAL_START_YEAR - 1
    run_health_model "health-initial-fda" \
      '"fda_ndc_products", "fda_drug_approvals", "fda_drug_recalls", "fda_adverse_events", "fda_device_recalls"'

    run_health_model "health-initial-trials" \
      '"clinical_trials", "clinical_trial_conditions", "clinical_trial_interventions"'

    run_health_model "health-initial-cdc" \
      '"cdc_covid_vaccinations", "cdc_mortality", "cdc_brfss"'

    run_health_model "health-initial-cms-medicaid" \
      '"cms_hospital_quality", "cms_open_payments", "medicaid_drug_utilization"'

    run_health_model "health-initial-rxnorm" \
      '"rxnorm_drugs"'
    ;;

  daily)
    export HEALTH_TRIALS_SINCE_DATE="${HEALTH_TRIALS_SINCE_DATE:-${INCREMENTAL_DATE}}"
    run_health_model "health-daily-trials" \
      '"clinical_trials", "clinical_trial_conditions", "clinical_trial_interventions"'
    ;;

  weekly)
    export HEALTH_CDC_COVID_SINCE_DATE="${HEALTH_CDC_COVID_SINCE_DATE:-${INCREMENTAL_DATE}}"
    if $FORCE || table_in_window "$HEALTH_SCHEMA_YAML" "cdc_covid_vaccinations"; then
      run_health_model "health-weekly-cdc" \
        '"cdc_covid_vaccinations", "cdc_mortality"'
    fi
    ;;

  monthly)
    export HEALTH_BRFSS_SINCE_YEAR="${HEALTH_BRFSS_SINCE_YEAR:-${INCREMENTAL_YEAR}}"
    export MEDICAID_SINCE_YEAR="${MEDICAID_SINCE_YEAR:-${INCREMENTAL_YEAR}}"
    # Each sub-run is gated to its source's known release window.
    # FDA catalogs and RxNorm update continuously and always run.

    # BRFSS — window read from cdc_brfss.releaseWindow
    if table_in_window "$HEALTH_SCHEMA_YAML" "cdc_brfss"; then
      run_health_model "health-monthly-brfss" '"cdc_brfss"'
    fi

    # Medicaid drug utilization — window read from medicaid_drug_utilization.releaseWindow
    if table_in_window "$HEALTH_SCHEMA_YAML" "medicaid_drug_utilization"; then
      run_health_model "health-monthly-medicaid" '"medicaid_drug_utilization"'
    fi

    # CMS open payments — window read from cms_open_payments.releaseWindow
    if table_in_window "$HEALTH_SCHEMA_YAML" "cms_open_payments"; then
      run_health_model "health-monthly-cms-payments" '"cms_open_payments"'
    fi

    # CMS hospital quality — window read from cms_hospital_quality.releaseWindow
    if table_in_window "$HEALTH_SCHEMA_YAML" "cms_hospital_quality"; then
      run_health_model "health-monthly-cms-quality" '"cms_hospital_quality"'
    fi

    # FDA catalogs + RxNorm — no releaseWindow in schema (continuous/monthly); always runs
    run_health_model "health-monthly-fda-rxnorm" \
      '"fda_ndc_products", "fda_drug_approvals", "fda_drug_recalls", "fda_adverse_events", "fda_device_recalls", "rxnorm_drugs"'
    ;;

  *)
    echo "Unknown mode: $MODE. Valid modes: initial, daily, weekly, monthly" >&2
    exit 1
    ;;
esac

log_info "$WORKER_ID complete"
