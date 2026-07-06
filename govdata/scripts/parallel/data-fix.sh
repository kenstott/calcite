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
# Re-ingest a specific table (or all tables in a schema) by:
#   1. Deleting Iceberg data in R2 for the target path(s)
#   2. Invalidating tracker completions via forceReprocessTables / freshStart
#   3. Running run-pool.sh for the schema so only the cleared table re-downloads
#
# Usage:
#   data-fix.sh <schema> <mode>                  # fix all tables in schema
#   data-fix.sh <schema> <mode> <table>          # fix one table
#   data-fix.sh <schema> <mode> <t1> <t2> ...    # fix multiple tables
#
# Options:
#   --dry-run    Print what would be deleted without deleting anything
#   --force      Pass FORCE=true to run-pool (bypasses release-window checks)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

# ── Argument parsing ──────────────────────────────────────────────────────────

if [ $# -lt 2 ]; then
  echo "Usage: $0 <schema> <mode> [table ...] [--dry-run] [--force]" >&2
  exit 1
fi

SCHEMA="$1"
MODE="$2"
shift 2

TABLES=()
DRY_RUN=false
FORCE_FLAG=false

for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --force)   FORCE_FLAG=true ;;
    *)         TABLES+=("$arg") ;;
  esac
done

ALL_TABLES=false
if [ ${#TABLES[@]} -eq 0 ]; then
  ALL_TABLES=true
fi

# ── Validate env ──────────────────────────────────────────────────────────────

if [ -z "${GOVDATA_PARQUET_DIR:-}" ]; then
  echo "ERROR: GOVDATA_PARQUET_DIR is not set — check .env.prod" >&2
  exit 1
fi

# ── Derive R2 path prefix for this schema ─────────────────────────────────────
# Data lives at  ${GOVDATA_PARQUET_DIR}/${SCHEMA}/  (correct path convention).
# rclone uses    r2:<bucket>/<path>  instead of  s3://<bucket>/<path>.

to_rclone_path() {
  local s3_path="$1"
  echo "$s3_path" | sed 's|^s3://|r2:|'
}

SCHEMA_DATA_PATH="${GOVDATA_PARQUET_DIR}/${SCHEMA}"
SCHEMA_DATA_R2="$(to_rclone_path "$SCHEMA_DATA_PATH")"

# ── Summary ───────────────────────────────────────────────────────────────────

if $ALL_TABLES; then
  log_info "data-fix: schema=$SCHEMA mode=$MODE scope=ALL_TABLES${DRY_RUN:+ DRY_RUN}"
  log_info "  Data root : $SCHEMA_DATA_PATH"
else
  log_info "data-fix: schema=$SCHEMA mode=$MODE tables=${TABLES[*]}${DRY_RUN:+ DRY_RUN}"
  for tbl in "${TABLES[@]}"; do
    log_info "  Data path : ${SCHEMA_DATA_PATH}/${tbl}"
  done
fi

# ── Step 1: Delete Iceberg / parquet data in R2 ───────────────────────────────

delete_path() {
  local r2_path="$1"
  if $DRY_RUN; then
    log_info "  [dry-run] rclone purge $r2_path"
  else
    log_info "  Purging: $r2_path"
    rclone purge "$r2_path" 2>/dev/null || true
  fi
}

if $ALL_TABLES; then
  delete_path "$SCHEMA_DATA_R2"
else
  for tbl in "${TABLES[@]}"; do
    delete_path "$(to_rclone_path "${SCHEMA_DATA_PATH}/${tbl}")"
  done
fi

# ── Step 2: Re-run ETL with tracker bypass ────────────────────────────────────
# For specific tables: GOVDATA_FORCE_REPROCESS_TABLES causes GovDataSchemaFactory
# to call tracker.invalidateTableCompletion() for each listed table before ETL.
# For all tables: export FORCE_FRESH=true which run-pool passes as freshStart=true
# via the model operand, causing tracker.clearAllCompletions() for the schema.

if ! $ALL_TABLES; then
  TABLES_CSV=$(IFS=','; echo "${TABLES[*]}")
  export GOVDATA_FORCE_REPROCESS_TABLES="$TABLES_CSV"
  log_info "data-fix: set GOVDATA_FORCE_REPROCESS_TABLES=$TABLES_CSV"
else
  # freshStart=true causes GovDataSchemaFactory to call tracker.clearAllCompletions(), which writes
  # a schema-scoped "_all/cleared" sentinel. S3HivePipelineTracker.getCachedCompletion() checks
  # clearedTables.contains("_all") so every table lookup returns null (not complete) for this run.
  export FORCE_FRESH=true
  log_info "data-fix: set FORCE_FRESH=true (freshStart — clears all completions for schema)"
fi

if $FORCE_FLAG; then
  export FORCE=true
fi

if $DRY_RUN; then
  log_info "[dry-run] Would run: run-pool.sh --schema $SCHEMA $MODE"
  exit 0
fi

log_info "data-fix: launching run-pool.sh --schema $SCHEMA $MODE"
exec "$SCRIPT_DIR/run-pool.sh" --schema "$SCHEMA" "$MODE"
