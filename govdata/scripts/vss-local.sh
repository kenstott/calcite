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
# vss-local.sh — thin wrapper around vss-local.py: activates the CPU embed venv,
# loads prod env, and dispatches. Replaces vss-gpu-runner.sh (Vultr) + vss.sh in
# the pool. Embeddings are generated LOCALLY on CPU and the DuckDB HNSW cache is
# published to MinIO. Years are walked BACKWARDS (current -> 2010) on backfill.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
GOVDATA_HOME="${GOVDATA_HOME:-$(cd "$SCRIPT_DIR/.." && pwd)}"
VENV="${VSS_EMBED_VENV:-$GOVDATA_HOME/build/.venv-embed}"

# Load prod env (MinIO endpoint, AWS creds, rclone remote) for the Python script.
if [ -f "$GOVDATA_HOME/.env.prod" ]; then
  set -a; source "$GOVDATA_HOME/.env.prod"; set +a
fi
export GOVDATA_HOME

# Ensure the embed venv exists (idempotent).
if [ ! -x "$VENV/bin/python" ]; then
  echo "[vss-local] embed venv missing — provisioning via vss-embed-setup.sh"
  bash "$SCRIPT_DIR/vss-embed-setup.sh"
fi
PY="$VENV/bin/python"
APP="$SCRIPT_DIR/vss-local.py"

BACKFILL_END="${VSS_BACKFILL_END:-2010}"

usage() {
  cat <<EOF
Usage: $0 <command>
  year <N>                 Embed+insert the delta for year N into the cache
  refresh <N>              year <N> then publish  (drop-in for the old vss.sh refresh)
  daily                    Embed the current year delta, then publish
  backfill [startYear]     Walk startYear (default: current) DOWN to ${BACKFILL_END},
                           embed each, then rebuild the index and publish
  rebuild                  Drop+rebuild the HNSW index (hygiene)
  publish                  Atomically upload the cache .duckdb + metadata.json to MinIO
  stats                    Per-year row/accession counts in the cache
EOF
}

cmd="${1:-help}"
case "$cmd" in
  year)     "$PY" "$APP" year --year "${2:?year required}" ;;
  refresh)  "$PY" "$APP" year --year "${2:?year required}"; "$PY" "$APP" publish ;;
  daily)    "$PY" "$APP" year --year "$(date +%Y)"; "$PY" "$APP" publish ;;
  backfill)
    start="${2:-$(date +%Y)}"
    for (( y=start; y>=BACKFILL_END; y-- )); do
      echo "[vss-local] backfill year $y"
      "$PY" "$APP" year --year "$y"
    done
    "$PY" "$APP" rebuild
    "$PY" "$APP" publish
    ;;
  rebuild)  "$PY" "$APP" rebuild ;;
  publish)  "$PY" "$APP" publish ;;
  stats)    "$PY" "$APP" stats ;;
  *)        usage; exit 1 ;;
esac
