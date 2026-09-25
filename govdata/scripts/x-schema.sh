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
# x-schema.sh — runs the two cross-schema sweeps: the entity-bridge sweep
# (EntityBridgeOrganizer.main), which always runs, and the chunk-parsing sweep
# (ChunkOrganizer.main), which only runs when GOVDATA_XSCHEMA_RUN_CHUNKS=true (see below).
# Together they are the "x-schema" step in the daily -> x-schema -> vss -> historical sequence
# run-pool.sh drives.
#
# The chunk sweep's one pass over every registered source (see ChunkOrganizer.java's
# ROW_CONCAT_SOURCES / DOCUMENT_BLOB_SOURCES) across every schema, including SEC's own
# mda_sections/earnings_transcripts -- SEC is chunked here, in this one centralized sweep, not by
# a per-schema writer during its own ETL run -- when enabled, organizes text into chunk rows in
# Postgres's vc_staging (the sole durable copy -- backed up via the existing nightly pg_dump, not
# a second Iceberg copy; see ChunkOrganizer's class javadoc). When it runs, it runs AFTER daily
# ETL (every source needs to already be materialized) and BEFORE vss-local.sh (which embeds
# vc_staging's un-coded backlog) -- ordering enforced by run-pool.sh's call sequence, not by this
# script.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GOVDATA_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Same env + classpath handling as every other ETL entry point. load_env sources .env.prod but
# PRESERVES a caller's exported overrides -- GOVDATA_PARQUET_DIR above all, so this sweep can be
# pointed at the DQ bucket for a rehearsal instead of always writing prod. A hand-rolled
# `set -a; source .env.prod` (what this script used to do) has .env.prod stomp those overrides,
# which silently sent every run at production regardless of what the caller asked for.
# shellcheck disable=SC1090
source "$GOVDATA_ROOT/scripts/parallel/common.sh"
load_env

# resolve_classpath honours GOVDATA_JAR, so a private build can be exercised without touching the
# shared jar a pool may be running from.
JAR=$(resolve_classpath) || exit 1

# Claim exclusive access to the box for this sweep. EntityBridgeOrganizer/ChunkOrganizer need
# real memory, and the vss-local.sh step that follows this one saturates every CPU core (torch
# pinned to os.cpu_count() — see vss-local.py). run-pool.sh's own $RUN_EMBEDDINGS block already
# sets MAX_WORKERS=0 before calling this script when invoked through the normal scheduled path —
# but that protection lived entirely in the CALLER, not here, so a direct invocation got none of
# it. Confirmed live 2026-09-15: a remediation agent launched this script directly (ops#302,
# entity-bridge re-run) and it ran concurrently with 6 other pool-runner processes, exactly the
# resource contention this reservation exists to prevent. Claiming it here instead makes
# exclusivity a property of this script regardless of how it's invoked — a nested invocation
# (already MAX_WORKERS=0 from the caller) just restores back to that same 0, harmless; the
# caller's own restore-to-prior-value after this script returns still has the final word.
POOL_BUDGET_FILE="$GOVDATA_ROOT/scripts/parallel/runs/pool-budget.conf"
_xschema_prior_budget=""
[ -f "$POOL_BUDGET_FILE" ] && _xschema_prior_budget=$(cat "$POOL_BUDGET_FILE")
_xschema_restore_budget() {
  if [ -n "$_xschema_prior_budget" ]; then
    echo "$_xschema_prior_budget" > "$POOL_BUDGET_FILE"
  else
    rm -f "$POOL_BUDGET_FILE"
  fi
}
trap _xschema_restore_budget EXIT
_xschema_total_mem_mb=$(free -m | awk '/^Mem:/{print $2}')
{ echo "RESERVE_MB=$((_xschema_total_mem_mb - 2000))"; echo "MAX_WORKERS=0"; } > "$POOL_BUDGET_FILE"
echo "[x-schema] claimed exclusive pool budget (MAX_WORKERS=0) — restored on exit"

: "${CALCITE_TRACKER_PG_URL:?CALCITE_TRACKER_PG_URL not set -- required to reach vc_staging}"

# Optional per-step time box. Unset (the default) means no limit, i.e. unchanged behaviour; set
# either to a `timeout`-style duration (e.g. 90m) to stop one step from consuming the whole window.
run_step() {
  local label="$1" limit="$2" main_class="$3"
  echo "[x-schema] $label (jar: $JAR)"
  if [ -n "$limit" ]; then
    timeout "$limit" "$GOVDATA_JAVA_BIN" -cp "$JAR" "$main_class"
  else
    "$GOVDATA_JAVA_BIN" -cp "$JAR" "$main_class"
  fi
}

# Entity bridges run FIRST. These two sweeps are independent — EntityBridgeOrganizer reads no
# vc_staging/chunk output — but they used to run chunks-first in one synchronous script, and the
# chunk sweep routinely consumed every remaining minute of run-scheduled.sh's window. The outer
# timeout then killed the whole process tree before the bridge sweep was ever reached, so
# ref.canonical_org_entity / ref.entity_org_bridge were never rebuilt in ANY scheduled run: the
# bridge start-of-run line above appears in none of the archived scheduled logs. The bridge sweep
# is the far shorter of the two, so ordering it first costs the chunk sweep little and stops it
# being starved indefinitely. Only ChunkOrganizer must precede vss-local.sh, and it still does.
# `if !` rather than a trailing `$?` test: under `set -e` a failing java aborts the script before
# the test is ever reached, so the explicit message was unreachable.
if ! run_step "building entity bridges across all schemas" \
    "${GOVDATA_XSCHEMA_BRIDGE_TIMEOUT:-}" \
    org.apache.calcite.adapter.govdata.ref.EntityBridgeOrganizer; then
  echo "ERROR: EntityBridgeOrganizer failed" >&2
  exit 1
fi

# ChunkOrganizer's chunk/embedding sweep has no resumable checkpoint or row/time cap sized for
# full production scale -- it is validated against the DQ bucket with minimal samples only, by
# defect-register-runner, until that work lands. Opt in explicitly once it's cleared for
# production; the entity-bridge sweep above is unaffected either way, since the two are
# independent.
if [ "${GOVDATA_XSCHEMA_RUN_CHUNKS:-false}" = "true" ]; then
  if ! run_step "sweeping every registered source into vc_staging" \
      "${GOVDATA_XSCHEMA_CHUNK_TIMEOUT:-}" \
      org.apache.calcite.adapter.govdata.ref.ChunkOrganizer; then
    echo "ERROR: ChunkOrganizer failed" >&2
    exit 1
  fi
else
  echo "[x-schema] skipping ChunkOrganizer sweep (set GOVDATA_XSCHEMA_RUN_CHUNKS=true to enable)"
fi
