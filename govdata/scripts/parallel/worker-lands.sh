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
  echo "Usage: $0 <historical|daily|once|YEAR|YEAR-YEAR> [--force]" >&2
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

INCREMENTAL_YEAR=${GOVDATA_INCREMENTAL_START_YEAR:-$(date +%Y)}

case "$MODE" in

  once)
    # Non-period tables — NO year dimension, ingested exactly once over the full range regardless
    # of the year-major front. Static reference tables + the USDA FIA per-state tables
    # (forest_inventory, forest_metrics, fia_* — inventory_year is a COLUMN, not a fetch/partition
    # dimension; the download is the full {state}_CSV.zip archive). Slicing these per-year makes
    # every year slot re-download all ~51 state archives (FL alone 261MB), so the pool emits a
    # single 'lands:once' for them instead of one per year. The year-partitioned tables are handled
    # by the bare-year slots below.
    START=${GOVDATA_START_YEAR:-2010}
    END=$((INCREMENTAL_YEAR - 1))
    run_lands_model "lands-once-static" \
      '"national_forests", "nps_units", "blm_field_offices"' "$START" "$END"
    run_lands_model "lands-once-inventory" \
      '"forest_inventory"' "$START" "$END"
    run_lands_model "lands-once-metrics" \
      '"forest_metrics"' "$START" "$END"
    run_lands_model "lands-once-fia" \
      '"fia_plots", "fia_seedlings", "fia_invasives", "fia_down_woody_debris", "fia_pop_evaluations", "fia_tree_grm"' \
      "$START" "$END"
    ;;

  historical|[0-9][0-9][0-9][0-9]|[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9])
    # Year-partitioned tables (timber_sales, nps_visitation, onrr_revenues) — advance with the
    # year-major front. A bare year (2025) or range (2020-2023) narrows to that span; the pool's
    # per-year slots use this path and deliberately skip the non-period tables (see 'once' above).
    # Plain 'historical' ALSO ingests the non-period tables, so a manual `lands:historical` and the
    # `all` alias remain a complete backfill.
    if [ "$MODE" != "historical" ]; then
      export GOVDATA_START_YEAR="${MODE%-*}"
      INCREMENTAL_YEAR=$(( ${MODE#*-} + 1 ))
    fi
    START=${GOVDATA_START_YEAR:-2010}
    END=$((INCREMENTAL_YEAR - 1))
    run_lands_model "lands-historical-timber" \
      '"timber_sales"' "$START" "$END"
    run_lands_model "lands-historical-visitation" \
      '"nps_visitation"' "$START" "$END"
    run_lands_model "lands-historical-revenues" \
      '"onrr_revenues"' "$START" "$END"
    if [ "$MODE" = "historical" ]; then
      # Full manual/`all` backfill: also do the non-period tables (the per-year pool slots do not).
      run_lands_model "lands-historical-static" \
        '"national_forests", "nps_units", "blm_field_offices"' "$START" "$END"
      run_lands_model "lands-historical-inventory" \
        '"forest_inventory"' "$START" "$END"
      run_lands_model "lands-historical-metrics" \
        '"forest_metrics"' "$START" "$END"
      run_lands_model "lands-historical-fia" \
        '"fia_plots", "fia_seedlings", "fia_invasives", "fia_down_woody_debris", "fia_pop_evaluations", "fia_tree_grm"' \
        "$START" "$END"
    fi
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
    if $FORCE || table_in_window "$LANDS_SCHEMA_YAML" "forest_metrics"; then
      run_lands_model "lands-daily-metrics" \
        '"forest_metrics"' "$START"
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
    echo "Unknown mode: $MODE. Valid modes: historical, daily, once (non-period FIA/static tables), a year (2025), or a range (2020-2023)" >&2
    exit 1
    ;;
esac

log_info "$WORKER_ID complete"
