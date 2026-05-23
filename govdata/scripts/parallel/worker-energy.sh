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
source "$SCRIPT_DIR/common.sh"
load_env

MODE="${1:-}"
if [ -z "$MODE" ]; then
  echo "Usage: $0 <initial|weekly|monthly|annual> [--force]" >&2
  exit 1
fi

FORCE=${FORCE:-false}
for arg in "${@:2}"; do
  [ "$arg" = "--force" ] && FORCE=true
done
export FORCE

WORKER_ID="worker-energy-${MODE}"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

ENERGY_SCHEMA_YAML="$GOVDATA_ROOT/src/main/resources/energy/energy-schema.yaml"

# ── helpers ──────────────────────────────────────────────────────────────────

run_energy_model() {
  local model_name=$1 enabled_tables=$2 start_year=$3 end_year=${4:-}

  local model_file="$MODEL_DIR/${model_name}.json"
  local parquet_dir="${GOVDATA_PARQUET_DIR}/energy"
  local cache_dir="${GOVDATA_CACHE_DIR}/energy"
  local end_year_json=""
  [ -n "$end_year" ] && end_year_json=",
      \"endYear\": ${end_year}"

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

INCREMENTAL_YEAR=${GOVDATA_INCREMENTAL_START_YEAR:-$(date +%Y)}

case "$MODE" in

  initial)
    START=${GOVDATA_START_YEAR:-2010}
    END=$((INCREMENTAL_YEAR - 1))
    # Full historical backfill — no release-window checks.
    run_energy_model "energy-initial-electricity" \
      '"eia_electricity_generation", "eia_electricity_prices"' "$START" "$END"

    run_energy_model "energy-initial-annual-surveys" \
      '"eia_utility_annual", "eia_power_plants"' "$START" "$END"

    run_energy_model "energy-initial-capacity" \
      '"eia_capacity_changes"' "$START" "$END"

    run_energy_model "energy-initial-supply" \
      '"eia_fossil_fuel_production", "eia_state_energy_consumption", "eia_refinery_operations"' "$START" "$END"

    run_energy_model "energy-initial-weekly" \
      '"eia_natural_gas_storage", "eia_petroleum_stocks"' "$START" "$END"

    run_energy_model "energy-initial-imports" \
      '"eia_crude_oil_imports"' "$START" "$END"

    run_energy_model "energy-initial-coal" \
      '"eia_coal_mines"' "$START" "$END"
    ;;

  weekly)
    START=$INCREMENTAL_YEAR
    if $FORCE || table_in_window "$ENERGY_SCHEMA_YAML" "eia_natural_gas_storage"; then
      run_energy_model "energy-weekly-gas-storage" \
        '"eia_natural_gas_storage"' "$START"
    fi

    if $FORCE || table_in_window "$ENERGY_SCHEMA_YAML" "eia_petroleum_stocks"; then
      run_energy_model "energy-weekly-petroleum-stocks" \
        '"eia_petroleum_stocks"' "$START"
    fi
    ;;

  monthly)
    START=$INCREMENTAL_YEAR
    run_energy_model "energy-monthly-electricity" \
      '"eia_electricity_generation", "eia_electricity_prices"' "$START"

    run_energy_model "energy-monthly-supply" \
      '"eia_fossil_fuel_production", "eia_refinery_operations"' "$START"

    run_energy_model "energy-monthly-capacity" \
      '"eia_capacity_changes"' "$START"

    run_energy_model "energy-monthly-imports" \
      '"eia_crude_oil_imports"' "$START"
    ;;

  annual)
    START=$INCREMENTAL_YEAR
    run_energy_model "energy-annual-surveys" \
      '"eia_utility_annual", "eia_power_plants"' "$START"

    run_energy_model "energy-annual-consumption" \
      '"eia_state_energy_consumption"' "$START"

    run_energy_model "energy-annual-coal" \
      '"eia_coal_mines"' "$START"
    ;;

  *)
    echo "Unknown mode: $MODE. Valid modes: initial, weekly, monthly, annual" >&2
    exit 1
    ;;
esac

log_info "$WORKER_ID complete"
