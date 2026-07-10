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
# the pool. Embeddings are generated LOCALLY on CPU and quantized codes (binary +
# int8) are appended to the vectorized_chunk_codes dataset in the lake (Path B).
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

usage() {
  cat <<EOF
Usage: $0 <command>
  daily                    PRIMARY: code the un-coded backlog (all years, newest first),
                           time-boxed (~2h). Appends quantized codes to the lake. Daily.
  backlog [maxRows]        Same as daily (with an explicit per-run row cap)
  year <N>                 Code a single year's delta (manual)
  stats                    Per-year counts in the codes dataset
EOF
}

cmd="${1:-help}"
case "$cmd" in
  daily)    "$PY" "$APP" backlog ;;
  backlog)  "$PY" "$APP" backlog ${2:+--max-rows "$2"} ;;
  year)     "$PY" "$APP" year --year "${2:?year required}" ;;
  stats)    "$PY" "$APP" stats ;;
  *)        usage; exit 1 ;;
esac
