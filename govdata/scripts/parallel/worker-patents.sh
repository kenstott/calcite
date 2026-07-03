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
  echo "Usage: $0 <historical|daily> [--force]" >&2
  exit 1
fi

FORCE=${FORCE:-false}
for arg in "${@:2}"; do
  [ "$arg" = "--force" ] && FORCE=true
done
export FORCE

WORKER_ID="worker-patents-${MODE}"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

PATENTS_SCHEMA_YAML="$GOVDATA_ROOT/src/main/resources/patents/patents-schema.yaml"

# ── helpers ───────────────────────────────────────────────────────────────────

run_patents_model() {
  local model_name=$1 enabled_tables=$2 start_year=$3 end_year=${4:-}

  local model_file="$MODEL_DIR/${model_name}.json"
  local parquet_dir="${GOVDATA_PARQUET_DIR}"
  local cache_dir="${GOVDATA_CACHE_DIR}/patents"
  local end_year_json=""
  [ -n "$end_year" ] && end_year_json=",
      \"endYear\": ${end_year}"

  cat > "$model_file" <<ENDJSON
{
  "version": "1.0",
  "defaultSchema": "patents",
  "schemas": [{
    "name": "patents",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
    "operand": {
      "dataSource": "patents",
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
      },
      $(tracker_operand_json)
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

  # Two table shapes, both driven by their own dimension — no special handling:
  #   • Dated tables (grants, claims, summaries, trademarks) carry a YEAR dimension.
  #     historical = older years; daily = current year.
  #   • Faithful forever tables (abstracts, applications, figures, inventors, assignees,
  #     locations, cpc) carry a QUARTER dimension (current release token). They have no
  #     year, so they ride the daily pass: when the quarter rolls over, the token re-keys
  #     the cache → the new dump is sourced and a new quarter partition is appended.
  historical|[0-9][0-9][0-9][0-9]|[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9])
    # A bare year (2025) or range (2020-2023) narrows the backfill to that span so patents
    # advances with the year-major front; plain 'historical' = full 2010..current-1 backfill.
    if [ "$MODE" != "historical" ]; then
      export GOVDATA_START_YEAR="${MODE%-*}"
      INCREMENTAL_YEAR=$(( ${MODE#*-} + 1 ))
    fi
    START=${GOVDATA_START_YEAR:-2010}
    END=$((INCREMENTAL_YEAR - 1))
    run_patents_model "patents-historical-grants" \
      '"patent_grants"' "$START" "$END"

    run_patents_model "patents-historical-claims" \
      '"patent_claims"' "$START" "$END"

    run_patents_model "patents-historical-summaries" \
      '"patent_summaries"' "$START" "$END"

    run_patents_model "patents-historical-trademarks" \
      '"trademark_applications"' "$START" "$END"
    ;;

  daily)
    # Quarterly cadence — gate on release window months {3, 6, 9, 12}
    if ! $FORCE; then
      within_release_window "patent" "3,6,9,12" || exit 0
    fi

    START=$INCREMENTAL_YEAR

    # Current-year dated tables.
    run_patents_model "patents-daily-grants" \
      '"patent_grants"' "$START"

    run_patents_model "patents-daily-claims" \
      '"patent_claims"' "$START"

    run_patents_model "patents-daily-summaries" \
      '"patent_summaries"' "$START"

    run_patents_model "patents-daily-trademarks" \
      '"trademark_applications"' "$START"

    # Current-quarter forever tables. startYear is unused by these transformers (no year
    # dimension); the quarter dimension drives sourcing. Inventor file (~8 GB) runs last.
    run_patents_model "patents-daily-locations" \
      '"patent_locations"' "$START"

    run_patents_model "patents-daily-abstracts" \
      '"patent_abstracts"' "$START"

    run_patents_model "patents-daily-applications" \
      '"patent_applications"' "$START"

    run_patents_model "patents-daily-figures" \
      '"patent_figures"' "$START"

    run_patents_model "patents-daily-assignees" \
      '"patent_assignees"' "$START"

    run_patents_model "patents-daily-cpc" \
      '"patent_cpc_classes"' "$START"

    run_patents_model "patents-daily-inventors" \
      '"patent_inventors"' "$START"
    ;;

  *)
    echo "Unknown mode: $MODE. Valid modes: historical, daily, a year (2025), or a range (2020-2023)" >&2
    exit 1
    ;;
esac

log_info "$WORKER_ID complete"
