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

# Claim exclusive access to the box for the heavy embedding path (daily/backlog) only — stats
# and dedup don't saturate CPU/memory the way embedding does, no need to reserve for those.
# torch is pinned to every core here (VSS_TORCH_THREADS defaults to os.cpu_count() in
# vss-local.py), so this needs the same exclusivity x-schema.sh claims for itself, for the same
# reason: the reservation used to live only in run-pool.sh's caller-side wrapper, so a directly
# invoked run (bypassing that wrapper) got no protection at all. See x-schema.sh's matching
# comment for the live incident (ops#302, 2026-09-15) that exposed this gap.
POOL_BUDGET_FILE="$GOVDATA_HOME/scripts/parallel/runs/pool-budget.conf"

# Gracefully stop every live top-level run-pool.sh instance before claiming the box.
# MAX_WORKERS=0 alone only blocks NEW admissions — it does nothing about JVMs already
# running, and those directly compete with vss-local.py's all-core torch pin for the
# whole embedding run (confirmed live 2026-09-15: this is exactly what forced a manual
# kill of a live x-schema run earlier the same day). SIGTERM to the top-level PID (not
# SIGKILL) is deliberate: run-pool.sh's own `trap cleanup INT TERM` already kills its
# active workers' sessions cleanly and records a proper .exit marker for each, the same
# graceful path a Ctrl-C gets. For a SCHEDULED instance (a child of run-scheduled.sh's
# run_window()), this exits 130, which run_window()'s crash-handling branch treats as
# one restart (of its 5-attempt budget) after a 30s delay -- but the relaunched instance
# immediately sees MAX_WORKERS=0 (set right after this function returns) and just idles,
# polling harmlessly with no new workers admitted, until the budget is restored on exit --
# so it comes back on its own the moment this script finishes, no separate restart logic
# needed. Ad-hoc/remediation-launched instances (not children of run_window()) won't
# auto-restart the same way; that's a smaller, separate gap.
_vss_gracefully_stop_etl() {
  local pids
  pids=$(pgrep -f 'run-pool.sh' | while read -r p; do
    _cmd=$(ps -o cmd= -p "$p" 2>/dev/null)
    # Skip the /timeout NNN wrapper duplicate (real child is a separate PID) and any
    # harness/monitoring shell whose own text merely mentions run-pool.sh rather than
    # actually being an invocation of it (same false-positive class as elsewhere today).
    echo "$_cmd" | grep -qE '/timeout [0-9]|eval .' && continue
    echo "$p"
  done)
  if [ -z "$pids" ]; then
    echo "[vss-local] no live run-pool.sh instances to stop"
    return 0
  fi
  echo "[vss-local] gracefully stopping live run-pool.sh instance(s): $(echo "$pids" | tr '\n' ' ')"
  echo "$pids" | xargs -r kill -TERM
  # Poll up to 2 minutes for them to actually exit (each one's own cleanup() kills its
  # workers with a 2s TERM->KILL grace period per worker, so this can take a little
  # while with several active workers).
  local waited=0
  while [ "$waited" -lt 120 ]; do
    local still
    still=$(echo "$pids" | xargs -r -I{} sh -c 'kill -0 {} 2>/dev/null && echo {}')
    [ -z "$still" ] && { echo "[vss-local] all stopped after ${waited}s"; return 0; }
    sleep 5
    waited=$((waited + 5))
  done
  echo "[vss-local] WARNING: some run-pool.sh instance(s) still alive after 120s: $still" >&2
}

_vss_claim_exclusive_budget() {
  _vss_gracefully_stop_etl
  _vss_prior_budget=""
  [ -f "$POOL_BUDGET_FILE" ] && _vss_prior_budget=$(cat "$POOL_BUDGET_FILE")
  _vss_restore_budget() {
    if [ -n "$_vss_prior_budget" ]; then
      echo "$_vss_prior_budget" > "$POOL_BUDGET_FILE"
    else
      rm -f "$POOL_BUDGET_FILE"
    fi
  }
  trap _vss_restore_budget EXIT
  _vss_total_mem_mb=$(free -m | awk '/^Mem:/{print $2}')
  { echo "RESERVE_MB=$((_vss_total_mem_mb - 2000))"; echo "MAX_WORKERS=0"; } > "$POOL_BUDGET_FILE"
  echo "[vss-local] claimed exclusive pool budget (MAX_WORKERS=0) — restored on exit"
}

cmd="${1:-help}"
case "$cmd" in
  daily)    shift; _vss_claim_exclusive_budget; "$PY" "$APP" backlog "$@" ;;
  backlog)
    shift
    _vss_claim_exclusive_budget
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
