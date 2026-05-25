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
# Runs run-pool dq-rebuild and restarts it automatically when a new engine
# release is published. Every 5 minutes the release monitor checks for a new
# engine-vX.Y.Z tag; on detection it kills the pool, downloads the new jar,
# and restarts.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

# ── argument parsing ──────────────────────────────────────────────────────────
SCHEMA_FILTER=""
MAX_RESTARTS=5
POLL_INTERVAL=300   # seconds between release polls (default 5 min)
POOL_EXTRA_ARGS=()  # forwarded verbatim to run-pool.sh

while [[ $# -gt 0 ]]; do
  case "$1" in
    --schema)
      shift
      SCHEMA_FILTER="${1:?--schema requires a schema name}"
      ;;
    --max-restarts)
      shift
      MAX_RESTARTS="${1:?--max-restarts requires an integer}"
      ;;
    --poll-interval)
      shift
      POLL_INTERVAL="${1:?--poll-interval requires seconds}"
      ;;
    -j)
      shift
      POOL_EXTRA_ARGS+=(-j "${1:?-j requires an integer}")
      ;;
    --force)
      POOL_EXTRA_ARGS+=(--force)
      ;;
    --help|-h)
      echo "Usage: $(basename "$0") [--schema name] [--max-restarts N] [--poll-interval secs] [-j N] [--force]"
      echo ""
      echo "  Starts run-pool dq-rebuild and monitors for new engine releases."
      echo "  On a new release: kills the pool, downloads the new jar, restarts."
      echo ""
      echo "  --schema name        Run only this schema (passed to run-pool --schema)"
      echo "  --max-restarts N     Max release-triggered restarts before giving up (default 5)"
      echo "  --poll-interval N    Seconds between release polls (default 300)"
      echo "  -j N                 Max concurrent workers (passed to run-pool)"
      echo "  --force              Bypass release-window checks (passed to run-pool)"
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
  shift
done

# ── jar management ────────────────────────────────────────────────────────────
JAR_DIR="$GOVDATA_ROOT/build/libs"
JAR_PATH="$JAR_DIR/askamerica-engine.jar"
export GOVDATA_JAR="$JAR_PATH"

_latest_release() {
  gh release list --repo kenstott/calcite --limit 1 --json tagName \
    --jq '.[0].tagName' 2>/dev/null || echo "unknown"
}

_download_jar() {
  local tag="$1"
  log_info "run-all-dq: downloading jar for release $tag"
  mkdir -p "$JAR_DIR"
  if ! gh release download "$tag" \
      --repo kenstott/calcite \
      --pattern "askamerica-engine.jar" \
      --dir "$JAR_DIR" \
      --clobber; then
    log_info "run-all-dq: ERROR — jar download failed for $tag"
    return 1
  fi
  log_info "run-all-dq: jar ready: $JAR_PATH"
}

# ── pool management ───────────────────────────────────────────────────────────
POOL_PID=""

_start_pool() {
  local cmd=("$SCRIPT_DIR/run-pool.sh")
  [ -n "$SCHEMA_FILTER" ] && cmd+=(--schema "$SCHEMA_FILTER")
  [ "${#POOL_EXTRA_ARGS[@]}" -gt 0 ] && cmd+=("${POOL_EXTRA_ARGS[@]}")
  cmd+=(dq-rebuild)
  log_info "run-all-dq: starting pool: ${cmd[*]}"
  "${cmd[@]}" &
  POOL_PID=$!
  log_info "run-all-dq: pool started (PID $POOL_PID)"
}

_stop_pool() {
  if [ -n "${POOL_PID:-}" ] && kill -0 "$POOL_PID" 2>/dev/null; then
    log_info "run-all-dq: stopping pool (PID $POOL_PID)"
    kill -TERM "$POOL_PID" 2>/dev/null || true
    wait "$POOL_PID" 2>/dev/null || true
    log_info "run-all-dq: pool stopped"
  fi
  POOL_PID=""
}

trap '_stop_pool' INT TERM EXIT

# ── main ──────────────────────────────────────────────────────────────────────
CURRENT_RELEASE=$(_latest_release)
log_info "run-all-dq: starting — release=$CURRENT_RELEASE poll_interval=${POLL_INTERVAL}s max_restarts=$MAX_RESTARTS"

RESTART_COUNT=0

while true; do
  _start_pool
  SAVED_PID=$POOL_PID
  NEW_RELEASE_FOUND=false

  # Poll for a new release while the pool is alive
  while kill -0 "$POOL_PID" 2>/dev/null; do
    sleep "$POLL_INTERVAL"
    kill -0 "$POOL_PID" 2>/dev/null || break   # pool may have finished during sleep

    candidate=$(_latest_release)
    if [ "$candidate" != "unknown" ] && [ "$candidate" != "$CURRENT_RELEASE" ]; then
      log_info "run-all-dq: new release $candidate detected (was $CURRENT_RELEASE)"
      NEW_RELEASE_FOUND=true
      _stop_pool
      if ! _download_jar "$candidate"; then
        log_info "run-all-dq: jar download failed — not restarting"
        exit 1
      fi
      CURRENT_RELEASE="$candidate"
      RESTART_COUNT=$((RESTART_COUNT + 1))
      if [ "$RESTART_COUNT" -gt "$MAX_RESTARTS" ]; then
        log_info "run-all-dq: max restarts ($MAX_RESTARTS) reached — stopping"
        exit 1
      fi
      break
    fi
  done

  if $NEW_RELEASE_FOUND; then
    continue
  fi

  # Pool exited on its own
  pool_exit=0
  wait "$SAVED_PID" 2>/dev/null || pool_exit=$?
  POOL_PID=""
  log_info "run-all-dq: pool finished (exit=$pool_exit)"
  exit $pool_exit
done
