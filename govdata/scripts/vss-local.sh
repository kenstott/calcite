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

# Ensure the embed venv exists AND carries its packages (idempotent). Testing for
# bin/python alone is not enough: a venv whose package install was interrupted still
# has the interpreter, so the guard passes forever and every run dies on the first
# import instead of reprovisioning. Probe the same imports vss-embed-setup.sh does.
if ! "$VENV/bin/python" - <<'PY' >/dev/null 2>&1
import torch, sentence_transformers, duckdb, pyarrow  # noqa
PY
then
  echo "[vss-local] embed venv missing or incomplete — provisioning via vss-embed-setup.sh"
  bash "$SCRIPT_DIR/vss-embed-setup.sh"
fi
PY="$VENV/bin/python"
APP="$SCRIPT_DIR/vss-local.py"

usage() {
  cat <<EOF
Usage: $0 <command> [args...]
  daily                    PRIMARY: one queue over vc_staging's whole un-coded delta (every
                           source together), time-boxed (~2h). Appends quantized codes
                           to the lake, resuming from the one chunk_id watermark. Daily.
  backlog [maxRows]        Same as daily (with an explicit per-run row cap)
  stats                    Per-(source_schema, year) counts across every codes dataset
  dedup [--source-schema S]
                           Force-compact + dedup a codes dataset by chunk_id, regardless of
                           file count (manual/one-off; normal backlog runs self-compact but
                           only above the file-count threshold)
EOF
}

cmd="${1:-help}"
case "$cmd" in
  daily)    shift; "$PY" "$APP" backlog "$@" ;;
  backlog)
    shift
    # Back-compat: a bare leading number is the maxRows positional; anything else
    # (a flag, or nothing) passes straight through.
    if [[ "${1:-}" =~ ^[0-9]+$ ]]; then
      maxRows="$1"; shift
      "$PY" "$APP" backlog --max-rows "$maxRows" "$@"
    else
      "$PY" "$APP" backlog "$@"
    fi
    ;;
  stats)    "$PY" "$APP" stats ;;
  dedup)    shift; "$PY" "$APP" dedup "$@" ;;
  *)        usage; exit 1 ;;
esac
