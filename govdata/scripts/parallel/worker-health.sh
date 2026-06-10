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
  echo "Usage: $0 <daily|historical> [--force]" >&2
  exit 1
fi

FORCE=${FORCE:-false}
for arg in "${@:2}"; do
  [ "$arg" = "--force" ] && FORCE=true
done
export FORCE

WORKER_ID="worker-health-${MODE}"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

# ── helpers ──────────────────────────────────────────────────────────────────

run_health_model() {
  local model_name=$1 enabled_tables=$2
  shift 2
  local extra_operands="${1:-}"

  local model_file="$MODEL_DIR/${model_name}.json"
  local extra_json=""
  [ -n "$extra_operands" ] && extra_json=",
      ${extra_operands}"

  # Use HEALTH_PARQUET_DIR if set; fall back to GOVDATA_PARQUET_DIR/health
  local parquet_dir="${HEALTH_PARQUET_DIR:-${GOVDATA_PARQUET_DIR}/health}"
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
#
# Only two modes, matching every other govdata worker: `historical` and `daily`.
# They differ ONLY in the year demarcation they export — `historical` backfills up to
# INCREMENTAL_YEAR-1, `daily` runs the current year onward. Per-table refresh cadence
# (which monthly/weekly tables actually re-fetch on a given run) is NOT the worker's job:
# it is handled entirely by each table's `freshness:` / `releaseWindow:` config in
# health-schema.yaml, enforced by the engine. Both modes therefore run the full table set.

INCREMENTAL_YEAR=${GOVDATA_INCREMENTAL_START_YEAR:-$(date +%Y)}

# Run all 15 health tables, grouped so a single model failure isolates to its group.
run_all_health_tables() {
  run_health_model "health-fda" \
    '"fda_ndc_products", "fda_drug_approvals", "fda_drug_recalls", "fda_adverse_events", "fda_device_recalls"'

  run_health_model "health-trials" \
    '"clinical_trials", "clinical_trial_conditions", "clinical_trial_interventions"'

  run_health_model "health-cdc" \
    '"cdc_covid_vaccinations", "cdc_mortality", "cdc_brfss"'

  run_health_model "health-cms-medicaid" \
    '"cms_hospital_quality", "cms_open_payments", "medicaid_drug_utilization"'

  run_health_model "health-rxnorm" \
    '"rxnorm_drugs"'
}

case "$MODE" in

  historical)
    export GOVDATA_UNTIL_DATE="$((INCREMENTAL_YEAR - 1))-12-31"
    export GOVDATA_END_YEAR=$((INCREMENTAL_YEAR - 1))
    run_all_health_tables
    ;;

  daily)
    export GOVDATA_START_YEAR="${INCREMENTAL_YEAR}"
    run_all_health_tables
    ;;

  *)
    echo "Unknown mode: $MODE. Valid modes: daily, historical" >&2
    exit 1
    ;;
esac

log_info "$WORKER_ID complete"
