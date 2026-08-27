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
  local model_name=$1 table_csv=$2
  shift 2
  local extra_operands="${1:-}"

  local enabled_tables
  enabled_tables="$(filter_enabled_tables "$table_csv")"
  if [ -z "$enabled_tables" ]; then
    log_info "$WORKER_ID: skipping $model_name — none of its tables match GOVDATA_TABLES=${GOVDATA_TABLES}"
    return
  fi

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
      "ignoreReleaseWindow": true,
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
# it was originally gated entirely by each table's `freshness:` / `releaseWindow:` config in
# health-schema.yaml (EtlPipeline's release-window gate) — but every slot type now sets the
# engine's `ignoreReleaseWindow` operand unconditionally (see run_health_model above), so a
# launch always runs regardless of day-of-week; freshness: hash-skip still applies.

INCREMENTAL_YEAR=${GOVDATA_INCREMENTAL_START_YEAR:-$(date +%Y)}

# Run the 18 pre-existing health tables, grouped so a single model failure isolates to its
# group. Called from both historical and daily — same rationale as every other group here
# already: each table self-manages its own cadence via freshness:/releaseWindow:.
run_all_health_tables() {
  run_health_model "health-fda" \
    'fda_ndc_products,fda_drug_approvals,fda_drug_recalls,fda_adverse_events,fda_device_recalls'

  run_health_model "health-who" \
    'who_gho_indicators'

  run_health_model "health-trials" \
    'clinical_trials,clinical_trial_conditions,clinical_trial_interventions'

  run_health_model "health-cdc" \
    'cdc_covid_vaccinations,cdc_mortality,cdc_brfss'

  run_health_model "health-cms-medicaid" \
    'cms_hospital_quality,cms_open_payments,medicaid_drug_utilization,cms_pos_facilities'

  run_health_model "health-rxnorm" \
    'rxnorm_drugs'

  run_health_model "health-hrsa" \
    'ahrf_physician_supply'
}

# Daily-only: the 9 CDC WONDER tables and 4 data.cdc.gov Socrata county/state tables are all
# single raw artifacts (dimensions: type: [<name>] only — no year axis), refreshed in place and
# gated by freshness: type: hash. There is nothing for a historical run to backfill — running
# them there would just re-pull the same current snapshot daily already handles, redundantly
# (and, for the WONDER group, burn through its 15s-per-request budget for no new data). Same
# rationale as cyber_threat being daily-only in worker-cyber.sh.
run_daily_only_health_tables() {
  run_health_model "health-cdc-geo" \
    'cdc_county_overdose_deaths,cdc_county_injury_mortality,cdc_state_vital_provisional,cdc_teen_birth_rates_county'

  # CDC WONDER XML API tables — isolated from health-cdc-geo because WONDER enforces a strict
  # 15-second minimum between requests (HTTP 429 otherwise), unlike data.cdc.gov Socrata.
  run_health_model "health-wonder" \
    'cdc_wonder_cancer_incidence,cdc_wonder_std_morbidity,cdc_wonder_tb,cdc_wonder_natality,cdc_wonder_cause_of_death,cdc_wonder_cause_of_death_2018_2024,cdc_wonder_multiple_cause_of_death,cdc_wonder_multiple_cause_of_death_2018_2024,cdc_wonder_vaers'
}

case "$MODE" in

  historical|[0-9][0-9][0-9][0-9]|[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9])
    # A bare year (2025) or range (2020-2023) narrows the backfill to that span so health
    # advances with the year-major front; plain 'historical' = full 2010..current-1 backfill.
    if [ "$MODE" != "historical" ]; then
      export GOVDATA_START_YEAR="${MODE%-*}"
      INCREMENTAL_YEAR=$(( ${MODE#*-} + 1 ))
    fi
    export GOVDATA_UNTIL_DATE="$((INCREMENTAL_YEAR - 1))-12-31"
    export GOVDATA_END_YEAR=$((INCREMENTAL_YEAR - 1))
    run_all_health_tables
    ;;

  daily)
    export GOVDATA_START_YEAR="${INCREMENTAL_YEAR}"
    run_all_health_tables
    run_daily_only_health_tables
    ;;

  *)
    echo "Unknown mode: $MODE. Valid modes: daily, historical, a year (2025), or a range (2020-2023)" >&2
    exit 1
    ;;
esac

log_info "$WORKER_ID complete"
