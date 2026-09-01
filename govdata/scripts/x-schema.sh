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

if [ -f "$GOVDATA_ROOT/.env.prod" ]; then
  set -a
  # CRLF-tolerant: see parallel/common.sh's load_env for why (Windows-edited env files).
  # shellcheck disable=SC1090
  source <(tr -d '\r' < "$GOVDATA_ROOT/.env.prod")
  set +a
fi

# Mirrors parallel/common.sh's resolve_classpath (not sourced directly -- this script has no
# other dependency on the worker-pool machinery, so a duplicate few lines beats pulling that
# whole file in). GOVDATA_JAR overrides for testing a private build without touching the shared
# jar a pool might be running concurrently.
if [ -n "${GOVDATA_JAR:-}" ]; then
  JAR=$(echo $GOVDATA_JAR | head -1)
else
  JAR=$(find "$GOVDATA_ROOT/build/libs" -name "sih-govdata.jar" 2>/dev/null | head -1)
  if [ -z "$JAR" ]; then
    JAR=$(find "$GOVDATA_ROOT/build/libs" -name "sih-govdata-*-SNAPSHOT.jar" 2>/dev/null | head -1)
  fi
fi
if [ -z "$JAR" ] || [ ! -f "$JAR" ]; then
  echo "ERROR: no govdata jar found under $GOVDATA_ROOT/build/libs (set GOVDATA_JAR to override)" >&2
  exit 1
fi

: "${CALCITE_TRACKER_PG_URL:?CALCITE_TRACKER_PG_URL not set -- required to reach vc_staging}"

echo "[x-schema] sweeping every registered source into vc_staging (jar: $JAR)"
exec java -cp "$JAR" org.apache.calcite.adapter.govdata.ref.ChunkOrganizer
