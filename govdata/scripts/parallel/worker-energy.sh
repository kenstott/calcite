#!/usr/bin/env bash
# Energy ETL worker — parameterized by MODE.
#
# Usage:
#   worker-energy.sh <mode> [--force]
#
# Modes:
#   initial   One-time backfill: all 11 energy tables from GOVDATA_START_YEAR (default 2010).
#             Run once on first setup before enabling recurring cadence workers.
#             Release-window checks are skipped — initial always runs in full.
#
#   weekly    Weekly cadence: natural gas storage + petroleum stocks.
#             Both series are published weekly by EIA. Window checks are applied;
#             pass --force to bypass.
#             Optional: ENERGY_SINCE_YEAR to limit year range (default 2000)
#
#   monthly   Monthly cadence: electricity generation, electricity prices,
#             capacity changes, fossil fuel production, refinery operations,
#             crude oil imports.
#             Optional: ENERGY_SINCE_YEAR to limit year range.
#
#   annual    Annual cadence: utility survey (EIA-861), power plant inventory (EIA-860),
#             state energy consumption (SEDS), coal mines (MSHA).
#             Optional: ENERGY_SINCE_YEAR to limit year range.
#
# Required env vars (set in .env.prod or equivalent):
#   GOVDATA_PARQUET_DIR     Root Parquet directory (energy data lands in source=energy subdir)
#   GOVDATA_CACHE_DIR       Root cache directory
#
# Optional env vars:
#   ENERGY_SINCE_YEAR       4-digit year — recurring workers start from this year
#                           (default varies per series; see energy-schema.yaml dimension_values)
#   ENERGY_EIA_API_KEY      EIA API key for higher rate limits (recommended; free at eia.gov/opendata)
#   ENERGY_BULK_TESTS       Set to "true" to enable the utility_scorecard view run after annual load
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

MODE="${1:-}"
if [ -z "$MODE" ]; then
  echo "Usage: $0 <initial|weekly|monthly|annual> [--force]" >&2
  exit 1
fi

FORCE=false
for arg in "${@:2}"; do
  [ "$arg" = "--force" ] && FORCE=true
done

WORKER_ID="worker-energy-${MODE}"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

ENERGY_SCHEMA_YAML="$GOVDATA_ROOT/src/main/resources/energy/energy-schema.yaml"

# ── helpers ──────────────────────────────────────────────────────────────────

run_energy_model() {
  local model_name=$1 enabled_tables=$2
  shift 2
  local extra_operands="${1:-}"

  local model_file="$MODEL_DIR/${model_name}.json"
  local extra_json=""
  [ -n "$extra_operands" ] && extra_json=",
      ${extra_operands}"

  local parquet_dir="${GOVDATA_PARQUET_DIR}/source=energy"
  local cache_dir="${GOVDATA_CACHE_DIR}/energy"

  cat > "$model_file" <<ENDJSON
{
  "version": "1.0",
  "defaultSchema": "energy",
  "schemas": [{
    "name": "energy",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
    "operand": {
      "dataSource": "energy",
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

case "$MODE" in

  initial)
    # Full historical backfill — no release-window checks.
    # Uses GOVDATA_START_YEAR (default 2010) via the schema's ENERGY_SINCE_YEAR substitution.
    export ENERGY_SINCE_YEAR="${GOVDATA_START_YEAR:-2010}"

    # EIA API monthly series — electricity generation and retail prices
    run_energy_model "energy-initial-electricity" \
      '"eia_electricity_generation", "eia_electricity_prices"'

    # EIA bulk annual surveys — utility (EIA-861) and power plant inventory (EIA-860)
    run_energy_model "energy-initial-annual-surveys" \
      '"eia_utility_annual", "eia_power_plants"'

    # EIA-860M monthly generator capacity snapshot (available from 2015)
    run_energy_model "energy-initial-capacity" \
      '"eia_capacity_changes"'

    # EIA API: fossil fuel production, SEDS state consumption, refinery operations
    run_energy_model "energy-initial-supply" \
      '"eia_fossil_fuel_production", "eia_state_energy_consumption", "eia_refinery_operations"'

    # EIA API weekly series — natural gas storage and petroleum stocks
    run_energy_model "energy-initial-weekly" \
      '"eia_natural_gas_storage", "eia_petroleum_stocks"'

    # EIA-814 crude oil imports (XLSX archive per month per year)
    run_energy_model "energy-initial-imports" \
      '"eia_crude_oil_imports"'

    # MSHA coal mine production (annual)
    run_energy_model "energy-initial-coal" \
      '"eia_coal_mines"'
    ;;

  weekly)
    export ENERGY_SINCE_YEAR="${GOVDATA_START_YEAR:-2010}"
    # EIA weekly series: gas storage (Thursdays) and petroleum stocks (Wednesdays)
    if $FORCE || table_in_window "$ENERGY_SCHEMA_YAML" "eia_natural_gas_storage"; then
      run_energy_model "energy-weekly-gas-storage" \
        '"eia_natural_gas_storage"'
    fi

    if $FORCE || table_in_window "$ENERGY_SCHEMA_YAML" "eia_petroleum_stocks"; then
      run_energy_model "energy-weekly-petroleum-stocks" \
        '"eia_petroleum_stocks"'
    fi
    ;;

  monthly)
    export ENERGY_SINCE_YEAR="${GOVDATA_START_YEAR:-2010}"
    # EIA API monthly series
    run_energy_model "energy-monthly-electricity" \
      '"eia_electricity_generation", "eia_electricity_prices"'

    run_energy_model "energy-monthly-supply" \
      '"eia_fossil_fuel_production", "eia_refinery_operations"'

    # EIA-860M monthly capacity snapshot
    run_energy_model "energy-monthly-capacity" \
      '"eia_capacity_changes"'

    # EIA-814 crude imports (monthly XLSX archives; ~2-month data lag)
    run_energy_model "energy-monthly-imports" \
      '"eia_crude_oil_imports"'
    ;;

  annual)
    export ENERGY_SINCE_YEAR="${GOVDATA_START_YEAR:-2010}"
    # EIA bulk annual surveys (typically released May-Oct for prior calendar year)
    run_energy_model "energy-annual-surveys" \
      '"eia_utility_annual", "eia_power_plants"'

    # EIA SEDS state energy consumption (1-2 year lag; released annually)
    run_energy_model "energy-annual-consumption" \
      '"eia_state_energy_consumption"'

    # MSHA coal mine production (annual release, ~1 year lag)
    run_energy_model "energy-annual-coal" \
      '"eia_coal_mines"'
    ;;

  *)
    echo "Unknown mode: $MODE. Valid modes: initial, weekly, monthly, annual" >&2
    exit 1
    ;;
esac

log_info "$WORKER_ID complete"
