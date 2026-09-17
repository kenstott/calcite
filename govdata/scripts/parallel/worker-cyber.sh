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

# Self-contained schema+year conflict guard, same pattern worker.sh uses: this script is
# run directly (not only through run-pool.sh), so it registers its own PID per real schema
# and calls check_schema_year_conflict itself rather than relying on the pool's admission
# check, which never runs for a direct invocation.
PID_DIR="$SCRIPT_DIR/runs/pids"
mkdir -p "$PID_DIR"
read -r _self_start_year _self_end_year <<< "$(_year_range_from_mode "$MODE")"
_CYBER_REGISTERED_IDS=()
_cyber_cleanup_registrations() {
  local _ec=$? _wid
  for _wid in "${_CYBER_REGISTERED_IDS[@]}"; do
    echo "$_ec" > "$PID_DIR/${_wid}.exit" 2>/dev/null
    rm -f "$PID_DIR/${_wid}.foreground" 2>/dev/null
  done
}
trap _cyber_cleanup_registrations EXIT
register_cyber_schema() {
  local _schema=$1
  local _wid="worker-${_schema}-${MODE}"
  local _existing_pid=""
  [ -f "$PID_DIR/${_wid}.pid" ] && _existing_pid=$(head -1 "$PID_DIR/${_wid}.pid" 2>/dev/null | tr -d '[:space:]')
  # A live PID already registered under this exact identity means a pool launch got here
  # first and already passed admission -- trust it, don't re-check or re-register.
  if [ -n "$_existing_pid" ] && kill -0 "$_existing_pid" 2>/dev/null; then
    return 0
  fi
  check_schema_year_conflict "$PID_DIR" "$_schema" "$_self_start_year" "$_self_end_year" || exit 1
  echo $$ > "$PID_DIR/${_wid}.pid"
  touch "$PID_DIR/${_wid}.foreground"
  rm -f "$PID_DIR/${_wid}.exit"
  _CYBER_REGISTERED_IDS+=("$_wid")
}
should_run "cyber_vuln" && register_cyber_schema "cyber_vuln"
should_run "cyber_threat" && register_cyber_schema "cyber_threat"

# ── helpers ──────────────────────────────────────────────────────────────────

run_cyber_model() {
  local schema=$1 model_name=$2 enabled_tables=$3
  shift 3
  local extra_operands="${1:-}"

  local model_file="$MODEL_DIR/${model_name}.json"

  # threat_pulses write/fetch mode: production daily appends only the pulses modified since the
  # prior run (watermark delta), accumulating version history; historical (full snapshot) and DQ
  # sample runs (which need full row-count/variety) full-load + replace-partitions. Read as the
  # cyber_threat.otxWriteMode operand by OtxResponseTransformer and the Iceberg writer.
  local otx_write="replace"
  if [ "$MODE" = "daily" ] && [ "${GOVDATA_DQ:-}" != "true" ]; then
    otx_write="append"
  fi

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
      "otxWriteMode": "${otx_write}",
      "directory": "${GOVDATA_PARQUET_DIR}",
      "cacheDirectory": "${GOVDATA_CACHE_DIR}/${schema}",
      "autoDownload": true,
      $(tracker_operand_json),
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

  historical)
    # Backfill: only the NVD publish-dated tables have a year axis. vulnerabilities /
    # vulnerability_cwes / vulnerability_cpes window over pub year/quarter (GOVDATA_START_YEAR-bounded),
    # so a historical run genuinely backfills them. Everything else in cyber is a current-snapshot
    # or delta feed with no history to backfill — the catalog/IOC/ATT&CK tables would just
    # re-pull the current dump (redundant with daily), and threat_pulses' historical `replace`
    # would clobber the version history daily accumulates via `append`. So historical scopes
    # cyber_vuln to just those three tables and skips cyber_threat entirely (it is daily-only).
    if should_run "cyber_vuln"; then
      run_cyber_model "cyber_vuln" "vuln-$MODE" '"vulnerabilities","vulnerability_cwes","vulnerability_cpes"'
    fi
    ;;

  daily)
    # Refresh: run every table except the three NVD pub-date-partitioned ones
    # (vulnerabilities / vulnerability_cwes / vulnerability_cpes). Those use
    # materialize.iceberg.overwritePartitions with batchPartitionColumns
    # [type, year, quarter] and each self-manages via pubStartDate/pubEndDate's
    # narrow tip window (tipDays: 90) — dynamic-partition-overwrite semantics
    # replace an entire (year, quarter) partition with whatever the tip window's
    # rows contain for it, so a small daily batch touching an old NVD-revised
    # CVE would wipe that partition's full historical row count. historical
    # mode's full-range window is the only mode that may safely touch them; see
    # its case below.
    if should_run "cyber_vuln"; then
      run_cyber_model "cyber_vuln" "vuln-$MODE" \
        '"cwe_catalog","kev_catalog","kev_cwes","osv_vulnerabilities","vuln_cross_refs","advisories"'
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
