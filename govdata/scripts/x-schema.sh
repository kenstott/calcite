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
# x-schema.sh — runs the cross-schema chunk-parsing sweep (ChunkOrganizer.main), the
# "x-schema" step in the daily -> x-schema -> vss -> historical sequence run-pool.sh drives.
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

echo "[x-schema] sweeping every registered source into vc_staging (jar: $JAR)"
# `if !` rather than a trailing `$?` test: under `set -e` a failing java aborts the script before
# the test is ever reached, so the explicit message was unreachable.
if ! "$GOVDATA_JAVA_BIN" -cp "$JAR" org.apache.calcite.adapter.govdata.ref.ChunkOrganizer; then
  echo "ERROR: ChunkOrganizer failed" >&2
  exit 1
fi

echo "[x-schema] building entity bridges across all schemas (jar: $JAR)"
exec "$GOVDATA_JAVA_BIN" -cp "$JAR" org.apache.calcite.adapter.govdata.ref.EntityBridgeOrganizer
