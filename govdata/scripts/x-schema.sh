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
# (EntityBridgeOrganizer.main) and the chunk-parsing sweep (ChunkOrganizer.main), in that order.
# Together they are the "x-schema" step in the daily -> x-schema -> vss -> historical sequence
# run-pool.sh drives.
#
# One sweep over every registered source (see ChunkOrganizer.java's ROW_CONCAT_SOURCES /
# DOCUMENT_BLOB_SOURCES) across every schema, including SEC's own mda_sections/
# earnings_transcripts -- SEC is chunked here, in this one centralized sweep, not by a
# per-schema writer during its own ETL run. Organizes text into chunk rows in Postgres's
# vc_staging (the sole durable copy -- backed up via the existing nightly pg_dump, not a
# second Iceberg copy; see ChunkOrganizer's class javadoc). Runs AFTER daily ETL (every
# source needs to already be materialized) and BEFORE vss-local.sh (which embeds vc_staging's
# un-coded backlog) -- ordering enforced by run-pool.sh's call sequence, not by this script.
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

if ! run_step "sweeping every registered source into vc_staging" \
    "${GOVDATA_XSCHEMA_CHUNK_TIMEOUT:-}" \
    org.apache.calcite.adapter.govdata.ref.ChunkOrganizer; then
  echo "ERROR: ChunkOrganizer failed" >&2
  exit 1
fi
