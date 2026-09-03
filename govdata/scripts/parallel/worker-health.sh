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

# Groups are declared as data, not as a sequence of calls, so the coverage check below can read
# the exact membership this script assigns without a second list to drift out of step. Grouping
# itself stays hand-curated because it encodes real constraints — failure isolation, and the
# per-source rate limits noted on the daily-only groups — that the schema does not express.
#
# Each entry is "<model name>|<comma-separated tables>".
ALL_MODE_GROUPS=(
  "health-fda|fda_ndc_products,fda_drug_approvals,fda_drug_recalls,fda_adverse_events,fda_device_recalls,fda_drug_shortages"
  "health-who|who_gho_indicators"
  "health-trials|clinical_trials,clinical_trial_conditions,clinical_trial_interventions"
  "health-cdc|cdc_covid_vaccinations,cdc_mortality,cdc_brfss"
  "health-cms-medicaid|cms_hospital_quality,cms_open_payments,medicaid_drug_utilization,cms_pos_facilities,cms_nursing_home"
  "health-rxnorm|rxnorm_drugs"
  "health-hrsa|ahrf_physician_supply"
)

# Run the grouped health tables. Called from both historical and daily — same rationale as every
# other group here already: each table self-manages its own cadence via freshness:/releaseWindow:.
run_all_health_tables() {
  local entry
  for entry in "${ALL_MODE_GROUPS[@]}"; do
    run_health_model "${entry%%|*}" "${entry#*|}"
  done
}

# Daily-only: the 9 CDC WONDER tables, the 4 data.cdc.gov Socrata county/state tables and the
# County Health Rankings annual file are all single raw artifacts (dimensions: type: [<name>]
# only — no year axis), refreshed in place and gated by freshness. There is nothing for a
# historical run to backfill — running them there would just re-pull the same current snapshot
# daily already handles, redundantly (and, for the WONDER group, burn through its
# 15s-per-request budget for no new data). Same rationale as cyber_threat being daily-only in
# worker-cyber.sh.
DAILY_ONLY_GROUPS=(
  "health-cdc-geo|cdc_county_overdose_deaths,cdc_county_injury_mortality,cdc_state_vital_provisional,cdc_teen_birth_rates_county"
  # County Health Rankings' 13MB annual CSV, isolated so its one long download cannot stall the
  # Socrata group above.
  "health-chr|chr_premature_death"
  # CDC WONDER XML API tables — isolated from health-cdc-geo because WONDER enforces a strict
  # 15-second minimum between requests (HTTP 429 otherwise), unlike data.cdc.gov Socrata.
  "health-wonder|cdc_wonder_cancer_incidence,cdc_wonder_std_morbidity,cdc_wonder_tb,cdc_wonder_natality,cdc_wonder_cause_of_death,cdc_wonder_cause_of_death_2018_2024,cdc_wonder_multiple_cause_of_death,cdc_wonder_multiple_cause_of_death_2018_2024,cdc_wonder_vaers"
)

run_daily_only_health_tables() {
  local entry
  for entry in "${DAILY_ONLY_GROUPS[@]}"; do
    run_health_model "${entry%%|*}" "${entry#*|}"
  done
}

# Catch every schema table this script does not explicitly group.
#
# Health names its tables by hand — grouping encodes rate limits and failure isolation the schema
# cannot express — but that hand-maintained membership is exactly how chr_premature_death and
# cms_nursing_home were each added to the schema, given columns and DQ checks, and then never run
# by anything: absent from every group, the enabledTables gate scoped them out and the pool
# reported success. Housing cannot have that bug because it derives its list from the schema.
#
# So membership is now checked against the schema on every launch. An ungrouped table is run
# rather than skipped, routed by the same rule worker.sh uses for the split-aware schemas — a
# `year` dimension means year-addressable (so it belongs to a historical backfill too), no year
# dimension means a snapshot that only daily needs. The warning names it so it can be given a
# proper group; running it in a catch-all is the safe default, not the intended end state.
run_ungrouped_health_tables() {
  local kind="$1" all_tables assigned="" entry t unassigned=""

  all_tables="$(schema_tables health "$kind")" || return 1
  for entry in "${ALL_MODE_GROUPS[@]}" "${DAILY_ONLY_GROUPS[@]}"; do
    assigned="${assigned},${entry#*|}"
  done
  assigned="${assigned},"

  IFS=',' read -ra _all <<< "$all_tables"
  for t in "${_all[@]}"; do
    t="$(echo "$t" | xargs)"
    [ -n "$t" ] || continue
    case "$assigned" in
      *",${t},"*) ;;
      *) unassigned="${unassigned}${t}," ;;
    esac
  done
  unassigned="${unassigned%,}"

  [ -n "$unassigned" ] || return 0
  log_info "$WORKER_ID: WARNING — health-schema.yaml declares tables in no worker group: ${unassigned}. Running them in health-ungrouped; assign them to a group in worker-health.sh."
  run_health_model "health-ungrouped" "$unassigned"
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
    # Only year-addressable strays here: a snapshot table has nothing for a backfill to do, and
    # daily picks it up below on its own pass.
    run_ungrouped_health_tables year
    ;;

  daily)
    export GOVDATA_START_YEAR="${INCREMENTAL_YEAR}"
    run_all_health_tables
    run_daily_only_health_tables
    run_ungrouped_health_tables all
    ;;

  *)
    echo "Unknown mode: $MODE. Valid modes: daily, historical, a year (2025), or a range (2020-2023)" >&2
    exit 1
    ;;
esac

log_info "$WORKER_ID complete"
