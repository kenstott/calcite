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

# ── worker launch + collect (runs one pass over a schema list) ─────────────────
# Populates FAILED_SCHEMAS (array) and PASS_SCHEMAS (array) in the caller.
_run_pass() {
  local schemas=("$@")
  local log_dir="$SCRIPT_DIR/runs/dq-all-$(date +%Y%m%d_%H%M%S)"
  mkdir -p "$log_dir"

  local pids=()
  local schema_list=()

  for schema in "${schemas[@]}"; do
    local log_file="$log_dir/${schema}.log"
    # shellcheck disable=SC2086
    bash "$DQ_WORKER" "$schema" $WORKER_EXTRA_ARGS > "$log_file" 2>&1 &
    local last_pid=$!
    pids+=($last_pid)
    schema_list+=("$schema")
    log_info "run-all-dq: launched $schema (PID $last_pid)"
  done

  FAILED_SCHEMAS=()
  PASS_SCHEMAS=()

  echo ""
  echo "=== DQ Pass Summary (mode=$MODE release=$CURRENT_RELEASE) ==="
  printf "%-30s  %s\n" "SCHEMA" "RESULT"
  printf "%-30s  %s\n" "------------------------------" "------"

  for i in "${!pids[@]}"; do
    local schema="${schema_list[$i]}"
    if wait "${pids[$i]}"; then
      PASS_SCHEMAS+=("$schema")
      printf "%-30s  PASS\n" "$schema"
    else
      FAILED_SCHEMAS+=("$schema")
      printf "%-30s  FAIL\n" "$schema"
    fi
  done
  echo ""
  log_info "run-all-dq: pass complete — passed=${#PASS_SCHEMAS[@]} failed=${#FAILED_SCHEMAS[@]}"
  log_info "run-all-dq: logs in $log_dir"
}

# ── main loop: run → poll for new release → re-run all schemas ────────────────
# Polling is unconditional — a new release always triggers a full re-run,
# regardless of whether the previous pass passed or failed.
# Exits when the poll window closes with no new release.
SCHEMAS_TO_RUN=("${ALL_SCHEMAS[@]}")
PASS=0

log_info "run-all-dq: mode=$MODE dry_run=$DRY_RUN rebuild=$REBUILD max_retries=$MAX_RETRIES schemas=${SCHEMAS_TO_RUN[*]}"

while true; do
  PASS=$((PASS + 1))
  log_info "run-all-dq: starting pass $PASS (release=$CURRENT_RELEASE)"
  _run_pass "${SCHEMAS_TO_RUN[@]}"

  if [ $PASS -gt $MAX_RETRIES ]; then
    log_info "run-all-dq: max passes ($MAX_RETRIES) reached — stopping"
    [ ${#FAILED_SCHEMAS[@]} -eq 0 ] && exit 0 || exit 1
  fi

  # Always poll for a new release after every pass (pass or fail).
  # A new release means fixes landed — re-run everything with fresh data.
  log_info "run-all-dq: pass $PASS done (passed=${#PASS_SCHEMAS[@]} failed=${#FAILED_SCHEMAS[@]}) — polling for new release (max ${POLL_MAX} × ${POLL_INTERVAL}s = $((POLL_MAX * POLL_INTERVAL / 60)) min)"

  NEW_RELEASE=""
  for poll in $(seq 1 $POLL_MAX); do
    CANDIDATE=$(_latest_release)
    if [ "$CANDIDATE" != "$CURRENT_RELEASE" ]; then
      NEW_RELEASE="$CANDIDATE"
      break
    fi
    log_info "run-all-dq: poll $poll/$POLL_MAX — still at $CURRENT_RELEASE, waiting ${POLL_INTERVAL}s..."
    sleep "$POLL_INTERVAL"
  done

  if [ -z "$NEW_RELEASE" ]; then
    log_info "run-all-dq: no new release after $((POLL_MAX * POLL_INTERVAL / 60)) min — done"
    [ ${#FAILED_SCHEMAS[@]} -eq 0 ] && exit 0 || exit 1
  fi

  log_info "run-all-dq: new release $NEW_RELEASE (was $CURRENT_RELEASE) — pulling code and re-running all schemas"
  CURRENT_RELEASE="$NEW_RELEASE"
  _git_pull

  # Re-runs always use --rebuild so data is fresh with the new code
  WORKER_EXTRA_ARGS="--mode $MODE --rebuild"
  $DRY_RUN  && WORKER_EXTRA_ARGS="$WORKER_EXTRA_ARGS --dry-run"
  [ -n "$START_YEAR" ] && WORKER_EXTRA_ARGS="$WORKER_EXTRA_ARGS --start-year $START_YEAR"

  SCHEMAS_TO_RUN=("${ALL_SCHEMAS[@]}")
done
