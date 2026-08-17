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
#   2. Invalidating tracker completions via tracker_pg.sh (same mechanism data_fix.sh and
#      data_purge.sh use — a real Postgres DELETE/UPSERT, not a shell-to-Java env var)
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
  # Capture the table list BEFORE purging, so Step 2 clears tracker completions for exactly the
  # tables that existed under this schema. table_completion has no schema column — every
  # non-sub-schema worker (all but lands/health) shares one flat govdata_parquet_v1 namespace —
  # so a query for "every completed table" would reach into OTHER schemas' tables too.
  ALL_TABLE_NAMES=()
  while IFS= read -r _d; do
    [ -n "$_d" ] && ALL_TABLE_NAMES+=("${_d%/}")
  done < <(rclone lsf --dirs-only "$SCHEMA_DATA_R2" 2>/dev/null)
  delete_path "$SCHEMA_DATA_R2"
else
  for tbl in "${TABLES[@]}"; do
    delete_path "$(to_rclone_path "${SCHEMA_DATA_PATH}/${tbl}")"
  done
fi

# ── Step 2: Invalidate tracker completions so the freshness gate doesn't skip re-ingest ──
# Purging Iceberg data (Step 1) is enough to force reprocessing for a table with NO freshness:
# gate configured — but a table that DOES declare one (see EtlPipeline's freshness skip-gate)
# would see the now-empty Iceberg table yet still skip the fetch, because its tracker
# completion is untouched by Step 1 and still reads as fresh. Clearing it here is what makes
# this work regardless of whether the table has a freshness gate.
# shellcheck source=/dev/null
source "$(dirname "$SCRIPT_DIR")/tracker_pg.sh"
PG_NS="$(pg_ns_from_bucket "$GOVDATA_PARQUET_DIR")" || exit 2
log_info "data-fix: tracker schema ${PG_NS}"

DRY_RUN_STR=$($DRY_RUN && echo true || echo false)
if $ALL_TABLES; then
  if [ ${#ALL_TABLE_NAMES[@]} -eq 0 ]; then
    log_info "data-fix: no existing tables found under ${SCHEMA_DATA_PATH} — nothing to clear in tracker"
  else
    for tbl in "${ALL_TABLE_NAMES[@]}"; do
      pg_tracker_clear_completion "$PG_NS" "$tbl" "$DRY_RUN_STR" || exit 2
    done
  fi
else
  for tbl in "${TABLES[@]}"; do
    pg_tracker_clear_completion "$PG_NS" "$tbl" "$DRY_RUN_STR" || exit 2
  done
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
