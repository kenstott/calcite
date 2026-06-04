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
  echo "Usage: $0 <initial|annual|biennial> [--force]" >&2
  exit 1
fi

FORCE=${FORCE:-false}
for arg in "${@:2}"; do
  [ "$arg" = "--force" ] && FORCE=true
done
export FORCE

WORKER_ID="worker-edu-${MODE}"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

EDU_SCHEMA_YAML="$GOVDATA_ROOT/src/main/resources/edu/edu-schema.yaml"

# ── helpers ──────────────────────────────────────────────────────────────────
# table_in_window reads releaseWindow: from edu-schema.yaml via check-release-window.py

run_edu_model() {
  local model_name=$1 enabled_tables=$2 start_year=$3 end_year=${4:-}

  local model_file="$MODEL_DIR/${model_name}.json"
  local parquet_dir="${EDU_PARQUET_DIR:-${GOVDATA_PARQUET_DIR}}"
  local cache_dir="${EDU_CACHE_DIR:-${GOVDATA_CACHE_DIR}/edu}"
  local end_year_json=""
  [ -n "$end_year" ] && end_year_json=",
      \"endYear\": ${end_year}"

  cat > "$model_file" <<ENDJSON
{
  "version": "1.0",
  "defaultSchema": "edu",
  "schemas": [{
    "name": "edu",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
    "operand": {
      "dataSource": "edu",
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
    # No release-window checks — initial always runs the full historical load.

    run_edu_model "edu-initial-k12" \
      '"ccd_districts", "ccd_schools"' "$START" "$END"

    run_edu_model "edu-initial-assessments" \
      '"naep_scores", "naep_achievement_levels", "crdc_schools"' "$START" "$END"

    run_edu_model "edu-initial-ipeds" \
      '"ipeds_institutions", "ipeds_completions", "ipeds_financials", "ipeds_tuition"' "$START" "$END"

    if [ -n "${API_DATA_GOV:-}" ]; then
      run_edu_model "edu-initial-scorecard" \
        '"college_scorecard", "college_scorecard_programs"' "$START" "$END"
    else
      log_info "$WORKER_ID: API_DATA_GOV not set — skipping college_scorecard tables"
    fi
    ;;

  annual)
    START=$INCREMENTAL_YEAR
    export GOVDATA_START_YEAR="${INCREMENTAL_YEAR}"
    # Each sub-run is gated independently — unrelated sources don't block each other.

    if $FORCE || table_in_window "$EDU_SCHEMA_YAML" "ccd_districts"; then
      run_edu_model "edu-annual-k12" \
        '"ccd_districts", "ccd_schools"' "$START"
    fi

    if $FORCE || table_in_window "$EDU_SCHEMA_YAML" "college_scorecard"; then
      if [ -n "${API_DATA_GOV:-}" ]; then
        run_edu_model "edu-annual-scorecard" \
          '"college_scorecard", "college_scorecard_programs"' "$START"
      else
        log_info "$WORKER_ID: API_DATA_GOV not set — skipping college_scorecard tables"
      fi
    fi

    if $FORCE || table_in_window "$EDU_SCHEMA_YAML" "ipeds_institutions"; then
      run_edu_model "edu-annual-ipeds-core" \
        '"ipeds_institutions", "ipeds_completions", "ipeds_tuition"' "$START"
    fi

    if $FORCE || table_in_window "$EDU_SCHEMA_YAML" "ipeds_financials"; then
      run_edu_model "edu-annual-ipeds-finance" \
        '"ipeds_financials"' "$START"
    fi
    ;;

  biennial)
    # naep_scores and crdc_schools both have dataLag:2 — effectiveEnd = N-2.
    # START must be ≤ effectiveEnd or yearRange is empty and nothing is fetched.
    BIENNIAL_START=$(( INCREMENTAL_YEAR - 2 ))
    export GOVDATA_START_YEAR="${INCREMENTAL_YEAR}"
    if $FORCE || table_in_window "$EDU_SCHEMA_YAML" "naep_scores"; then
      run_edu_model "edu-biennial-naep" \
        '"naep_scores", "naep_achievement_levels"' "$BIENNIAL_START"
    fi

    if $FORCE || table_in_window "$EDU_SCHEMA_YAML" "crdc_schools"; then
      run_edu_model "edu-biennial-crdc" \
        '"crdc_schools"' "$BIENNIAL_START"
    fi
    ;;

  *)
    echo "Unknown mode: $MODE. Valid modes: initial, annual, biennial" >&2
    exit 1
    ;;
esac

log_info "$WORKER_ID complete"
