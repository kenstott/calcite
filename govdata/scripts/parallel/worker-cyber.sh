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
  echo "Usage: $0 <daily|historical> [cyber_threat|cyber_vuln] [--force]" >&2
  exit 1
fi

# Optional second arg restricts which schema runs in this invocation.
SCHEMA_FILTER="${2:-both}"
case "$SCHEMA_FILTER" in
  cyber_threat|cyber_vuln|both) ;;
  --force) SCHEMA_FILTER="both" ;;  # handle caller passing --force as $2
  *) echo "Usage: $0 <mode> [cyber_threat|cyber_vuln] [--force]" >&2; exit 1 ;;
esac

FORCE=${FORCE:-false}
for arg in "$@"; do
  [ "$arg" = "--force" ] && FORCE=true
done
export FORCE

# Returns true if the given schema should run given SCHEMA_FILTER.
should_run() { [ "$SCHEMA_FILTER" = "both" ] || [ "$SCHEMA_FILTER" = "$1" ]; }

WORKER_ID="worker-${SCHEMA_FILTER}-${MODE}"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

CYBER_VULN_SCHEMA_YAML="$GOVDATA_ROOT/src/main/resources/cyber/cyber-vuln-schema.yaml"
CYBER_THREAT_SCHEMA_YAML="$GOVDATA_ROOT/src/main/resources/cyber/cyber-threat-schema.yaml"


# ── helpers ──────────────────────────────────────────────────────────────────

run_cyber_model() {
  local schema=$1 model_name=$2 enabled_tables=$3
  shift 3
  local extra_operands="${1:-}"

  local model_file="$MODEL_DIR/${model_name}.json"
  local extra_json=""
  [ -n "$extra_operands" ] && extra_json=",
      ${extra_operands}"
  local fresh_start_json=""
  [ "${FORCE:-false}" = "true" ] && fresh_start_json=',
      "freshStart": true'

  cat > "$model_file" <<ENDJSON
{
  "version": "1.0",
  "defaultSchema": "${schema}",
  "schemas": [{
    "name": "${schema}",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
    "operand": {
      "dataSource": "${schema}",
      "directory": "${GOVDATA_PARQUET_DIR}",
      "cacheDirectory": "${GOVDATA_CACHE_DIR}",
      "autoDownload": true,
      "trackerBackend": "s3",
      "trackerConfig": {
        "bucket": "${CALCITE_TRACKER_S3_BUCKET}",
        "endpoint": "${AWS_ENDPOINT_OVERRIDE}"
      },
      "s3Config": {
        "accessKeyId": "\${AWS_ACCESS_KEY_ID}",
        "secretAccessKey": "\${AWS_SECRET_ACCESS_KEY}",
        "endpoint": "\${AWS_ENDPOINT_OVERRIDE}"
      },
      "enabledTables": [${enabled_tables}]${extra_json}${fresh_start_json}
    }
  }]
}
ENDJSON

  log_info "$WORKER_ID: running $model_name"
  run_etl "$model_file" "$WORKER_ID"
}

# ── modes ─────────────────────────────────────────────────────────────────────

case "$MODE" in

  historical|daily)
    # Only two modes. The year window (historical = full backfill, daily = current) is set by
    # the caller via GOVDATA_START_YEAR / GOVDATA_RUN_MODE. There is no "initial" load and no
    # per-cadence (weekly/hourly/static) modes: every table self-manages first-load-vs-
    # incremental and its own refresh cadence from its dataset_type / freshness / releaseWindow
    # config, so both modes simply run ALL of the schema's tables (empty enabledTables = all).
    if should_run "cyber_vuln"; then
      run_cyber_model "cyber_vuln" "vuln-$MODE" ''
    fi
    if should_run "cyber_threat"; then
      run_cyber_model "cyber_threat" "threat-$MODE" ''
    fi
    ;;

  *)
    echo "Unknown mode: $MODE. Valid modes: daily, historical" >&2
    exit 1
    ;;
esac

log_info "$WORKER_ID complete"
