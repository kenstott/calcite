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
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

# ── argument parsing ──────────────────────────────────────────────────────────
MODE="daily"
DRY_RUN=false
REBUILD=false
SCHEMA_FILTER=""
START_YEAR=""
FROM_SCHEMA=""
MAX_RETRIES=5           # max auto-retry passes after a new release is detected
POLL_INTERVAL=120       # seconds between release polls
POLL_MAX=15             # polls before giving up (15 × 120 s = 30 min)

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      shift
      MODE="${1:?--mode requires an argument: daily or historical}"
      ;;
    --dry-run)
      DRY_RUN=true
      ;;
    --rebuild)
      REBUILD=true
      ;;
    --start-year)
      shift
      START_YEAR="${1:?--start-year requires a 4-digit year}"
      ;;
    --schemas)
      shift
      SCHEMA_FILTER="${1:?--schemas requires a comma-separated list}"
      ;;
    --from-schema)
      shift
      FROM_SCHEMA="${1:?--from-schema requires a schema name}"
      ;;
    --max-retries)
      shift
      MAX_RETRIES="${1:?--max-retries requires an integer}"
      ;;
    --help|-h)
      sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
  shift
done

if [[ "$MODE" != "daily" && "$MODE" != "historical" ]]; then
  echo "ERROR: --mode must be 'daily' or 'historical', got: $MODE" >&2
  exit 1
fi

# ── discover schemas ──────────────────────────────────────────────────────────
SCRIPTS_DIR="$GOVDATA_ROOT/scripts"
DQ_WORKER="$SCRIPT_DIR/worker-dq-run.sh"

if [ ! -f "$DQ_WORKER" ]; then
  echo "ERROR: worker-dq-run.sh not found at $DQ_WORKER" >&2
  exit 1
fi

if [ -n "$SCHEMA_FILTER" ]; then
  IFS=',' read -ra ALL_SCHEMAS <<< "$SCHEMA_FILTER"
else
  ALL_SCHEMAS=()
  for f in "$SCRIPTS_DIR"/*_dq.sql; do
    base=$(basename "$f" _dq.sql)
    ALL_SCHEMAS+=("$base")
  done
fi

if [ ${#ALL_SCHEMAS[@]} -eq 0 ]; then
  echo "ERROR: no *_dq.sql scripts found in $SCRIPTS_DIR" >&2
  exit 1
fi

# Apply --from-schema: drop all schemas before the named one
if [ -n "$FROM_SCHEMA" ]; then
  found=false
  filtered=()
  for s in "${ALL_SCHEMAS[@]}"; do
    if [ "$s" = "$FROM_SCHEMA" ]; then found=true; fi
    $found && filtered+=("$s")
  done
  if ! $found; then
    echo "ERROR: --from-schema '$FROM_SCHEMA' not found in schema list: ${ALL_SCHEMAS[*]}" >&2
    exit 1
  fi
  ALL_SCHEMAS=("${filtered[@]}")
  log_info "run-all-dq: --from-schema=$FROM_SCHEMA — running ${#ALL_SCHEMAS[@]} schema(s): ${ALL_SCHEMAS[*]}"
fi

# ── build worker args ─────────────────────────────────────────────────────────
WORKER_EXTRA_ARGS="--mode $MODE"
$DRY_RUN    && WORKER_EXTRA_ARGS="$WORKER_EXTRA_ARGS --dry-run"
$REBUILD    && WORKER_EXTRA_ARGS="$WORKER_EXTRA_ARGS --rebuild"
[ -n "$START_YEAR" ] && WORKER_EXTRA_ARGS="$WORKER_EXTRA_ARGS --start-year $START_YEAR"

# ── release helpers ───────────────────────────────────────────────────────────
CALCITE_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null || echo "")"

_latest_release() {
  gh release list --repo kenstott/calcite --limit 1 --json tagName \
    --jq '.[0].tagName' 2>/dev/null || \
  { [ -n "$CALCITE_ROOT" ] && git -C "$CALCITE_ROOT" describe --tags --abbrev=0 2>/dev/null; } || \
  echo "unknown"
}

_git_pull() {
  [ -z "$CALCITE_ROOT" ] && return
  log_info "run-all-dq: pulling latest code from origin"
  git -C "$CALCITE_ROOT" pull --ff-only 2>/dev/null && \
    log_info "run-all-dq: git pull succeeded" || \
    log_info "run-all-dq: WARNING: git pull failed — proceeding with current checkout"
}

# Record release at start so we can detect a newer one after a failure
CURRENT_RELEASE=$(_latest_release)
log_info "run-all-dq: starting with release $CURRENT_RELEASE"

# ── release poller (background) ───────────────────────────────────────────────
# Writes the new release tag to a temp file when it detects one, then exits.
# The main loop reads this file to know when to restart.
RELEASE_SIGNAL_FILE=""

_start_release_poller() {
  RELEASE_SIGNAL_FILE=$(mktemp)
  local signal_file="$RELEASE_SIGNAL_FILE"
  local known_release="$CURRENT_RELEASE"
  (
    while true; do
      sleep "$POLL_INTERVAL"
      candidate=$(_latest_release)
      if [ "$candidate" != "$known_release" ]; then
        echo "$candidate" > "$signal_file"
        exit 0
      fi
    done
  ) &
  POLLER_PID=$!
}

_stop_release_poller() {
  kill "$POLLER_PID" 2>/dev/null || true
  wait "$POLLER_PID" 2>/dev/null || true
}

# ── main loop ─────────────────────────────────────────────────────────────────
# Workers and the release poller run concurrently. When the poller signals a
# new release, all running workers are killed and the run restarts immediately.
PASS=0

log_info "run-all-dq: mode=$MODE dry_run=$DRY_RUN rebuild=$REBUILD max_retries=$MAX_RETRIES schemas=${ALL_SCHEMAS[*]}"

while true; do
  PASS=$((PASS + 1))
  if [ $PASS -gt $MAX_RETRIES ]; then
    log_info "run-all-dq: max passes ($MAX_RETRIES) reached — stopping"
    exit 1
  fi

  log_info "run-all-dq: starting pass $PASS (release=$CURRENT_RELEASE)"

  LOG_DIR="$SCRIPT_DIR/runs/dq-all-$(date +%Y%m%d_%H%M%S)"
  mkdir -p "$LOG_DIR"

  # Launch all workers
  WORKER_PIDS=()
  SCHEMA_LIST=()
  for schema in "${ALL_SCHEMAS[@]}"; do
    log_file="$LOG_DIR/${schema}.log"
    # shellcheck disable=SC2086
    bash "$DQ_WORKER" "$schema" $WORKER_EXTRA_ARGS > "$log_file" 2>&1 &
    WORKER_PIDS+=($!)
    SCHEMA_LIST+=("$schema")
    log_info "run-all-dq: launched $schema (PID ${WORKER_PIDS[-1]})"
  done

  # Start release poller concurrently
  _start_release_poller

  # Wait for workers — checking the release signal between each
  WORKER_EXIT_CODES=()
  NEW_RELEASE=""
  for i in "${!WORKER_PIDS[@]}"; do
    # If poller already signalled, no need to wait further
    if [ -z "$NEW_RELEASE" ] && [ -s "$RELEASE_SIGNAL_FILE" ]; then
      NEW_RELEASE=$(cat "$RELEASE_SIGNAL_FILE")
    fi
    if [ -n "$NEW_RELEASE" ]; then
      WORKER_EXIT_CODES+=( 255 )   # interrupted
    else
      wait "${WORKER_PIDS[$i]}" && WORKER_EXIT_CODES+=( 0 ) || WORKER_EXIT_CODES+=( $? )
      # Re-check signal after each worker completes
      [ -s "$RELEASE_SIGNAL_FILE" ] && NEW_RELEASE=$(cat "$RELEASE_SIGNAL_FILE") || true
    fi
  done

  _stop_release_poller

  if [ -n "$NEW_RELEASE" ]; then
    # Kill any still-running workers
    for pid in "${WORKER_PIDS[@]}"; do
      kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true

    log_info "run-all-dq: new release $NEW_RELEASE (was $CURRENT_RELEASE) — killing workers, pulling code, restarting"
    CURRENT_RELEASE="$NEW_RELEASE"
    > "$RELEASE_SIGNAL_FILE"
    _git_pull

    # Re-runs always rebuild so data is fresh with the new code
    WORKER_EXTRA_ARGS="--mode $MODE --rebuild"
    $DRY_RUN && WORKER_EXTRA_ARGS="$WORKER_EXTRA_ARGS --dry-run"
    [ -n "$START_YEAR" ] && WORKER_EXTRA_ARGS="$WORKER_EXTRA_ARGS --start-year $START_YEAR"
    continue
  fi

  # No new release — collect results and exit
  rm -f "$RELEASE_SIGNAL_FILE"
  echo ""
  echo "=== DQ Pass $PASS Summary (mode=$MODE release=$CURRENT_RELEASE) ==="
  printf "%-30s  %s\n" "SCHEMA" "RESULT"
  printf "%-30s  %s\n" "------------------------------" "------"
  overall_exit=0
  for i in "${!SCHEMA_LIST[@]}"; do
    if [ "${WORKER_EXIT_CODES[$i]}" -eq 0 ]; then
      printf "%-30s  PASS\n" "${SCHEMA_LIST[$i]}"
    else
      printf "%-30s  FAIL\n" "${SCHEMA_LIST[$i]}"
      overall_exit=1
    fi
  done
  echo ""
  log_info "run-all-dq: logs in $LOG_DIR"
  [ $overall_exit -eq 0 ] && log_info "run-all-dq: ALL SCHEMAS PASSED" || log_info "run-all-dq: ONE OR MORE SCHEMAS FAILED"
  exit $overall_exit
done
