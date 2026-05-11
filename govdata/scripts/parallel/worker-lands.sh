#!/usr/bin/env bash
# Lands ETL worker — parameterized by MODE.
#
# Usage:
#   worker-lands.sh <mode> [--force]
#
# Modes:
#   historical  One-time backfill: all 7 lands tables from
#               GOVDATA_START_YEAR (default 2010) through
#               GOVDATA_INCREMENTAL_START_YEAR - 1.
#               Release-window checks are skipped — historical always runs in full.
#
#   daily       Recurring cadence: all lands tables for the current year
#               (GOVDATA_INCREMENTAL_START_YEAR).
#               Each table is gated by table_in_window — static reference tables
#               (national_forests, nps_units, blm_field_offices) refresh annually;
#               nps_visitation and onrr_revenues refresh monthly.
#
# Required env vars (set in .env.prod or equivalent):
#   GOVDATA_PARQUET_DIR              Root Parquet directory (lands data under lands/)
#   GOVDATA_CACHE_DIR                Root cache directory
#   GOVDATA_START_YEAR               Historical start year (default 2010)
#   GOVDATA_INCREMENTAL_START_YEAR   First year of incremental / daily window
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

MODE="${1:-}"
if [ -z "$MODE" ]; then
  echo "Usage: $0 <historical|daily> [--force]" >&2
  exit 1
fi

FORCE=${FORCE:-false}
for arg in "${@:2}"; do
  [ "$arg" = "--force" ] && FORCE=true
done
export FORCE

WORKER_ID="worker-lands-${MODE}"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

LANDS_SCHEMA_YAML="$GOVDATA_ROOT/src/main/resources/lands/lands-schema.yaml"

# ── helpers ───────────────────────────────────────────────────────────────────

run_lands_model() {
  local model_name=$1 enabled_tables=$2 start_year=$3 end_year=${4:-}

  local model_file="$MODEL_DIR/${model_name}.json"
  local parquet_dir="${GOVDATA_PARQUET_DIR}/lands"
  local cache_dir="${GOVDATA_CACHE_DIR}/lands"
  local end_year_json=""
  [ -n "$end_year" ] && end_year_json=",
      \"endYear\": ${end_year}"

  cat > "$model_file" <<ENDJSON
{
  "version": "1.0",
  "defaultSchema": "lands",
  "schemas": [{
    "name": "lands",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
    "operand": {
      "dataSource": "lands",
      "directory": "${parquet_dir}",
      "cacheDirectory": "${cache_dir}",
      "autoDownload": true,
      "startYear": ${start_year}${end_year_json},
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

  log_info "$WORKER_ID: running $model_name"
  run_etl "$model_file" "$WORKER_ID"
}

# ── modes ─────────────────────────────────────────────────────────────────────

INCREMENTAL_YEAR=${GOVDATA_INCREMENTAL_START_YEAR:-2026}

case "$MODE" in

  historical)
    START=${GOVDATA_START_YEAR:-2010}
    END=$((INCREMENTAL_YEAR - 1))
    # Static reference tables — no year dimension, include in historical pass
    run_lands_model "lands-historical-static" \
      '"national_forests", "nps_units", "blm_field_offices"' "$START" "$END"
    # Time-series tables — full historical range
    run_lands_model "lands-historical-timber" \
      '"timber_sales"' "$START" "$END"
    run_lands_model "lands-historical-inventory" \
      '"forest_inventory"' "$START" "$END"
    run_lands_model "lands-historical-visitation" \
      '"nps_visitation"' "$START" "$END"
    run_lands_model "lands-historical-revenues" \
      '"onrr_revenues"' "$START" "$END"
    ;;

  daily)
    START=$INCREMENTAL_YEAR
    # Annual-cadence tables
    if $FORCE || table_in_window "$LANDS_SCHEMA_YAML" "national_forests"; then
      run_lands_model "lands-daily-forests" \
        '"national_forests"' "$START"
    fi
    if $FORCE || table_in_window "$LANDS_SCHEMA_YAML" "nps_units"; then
      run_lands_model "lands-daily-nps-units" \
        '"nps_units"' "$START"
    fi
    if $FORCE || table_in_window "$LANDS_SCHEMA_YAML" "blm_field_offices"; then
      run_lands_model "lands-daily-blm" \
        '"blm_field_offices"' "$START"
    fi
    if $FORCE || table_in_window "$LANDS_SCHEMA_YAML" "timber_sales"; then
      run_lands_model "lands-daily-timber" \
        '"timber_sales"' "$START"
    fi
    if $FORCE || table_in_window "$LANDS_SCHEMA_YAML" "forest_inventory"; then
      run_lands_model "lands-daily-inventory" \
        '"forest_inventory"' "$START"
    fi
    # Monthly-cadence tables (ONRR data arrives ~3 months in arrears)
    if $FORCE || table_in_window "$LANDS_SCHEMA_YAML" "nps_visitation"; then
      run_lands_model "lands-daily-visitation" \
        '"nps_visitation"' "$START"
    fi
    if $FORCE || table_in_window "$LANDS_SCHEMA_YAML" "onrr_revenues"; then
      run_lands_model "lands-daily-revenues" \
        '"onrr_revenues"' "$START"
    fi
    ;;

  *)
    echo "Unknown mode: $MODE. Valid modes: historical, daily" >&2
    exit 1
    ;;
esac

log_info "$WORKER_ID complete"
