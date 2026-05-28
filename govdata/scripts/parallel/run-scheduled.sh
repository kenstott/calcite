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

# On MinIO (pre-prod), redirect all storage to MinIO via .env.preprod.
_env_preprod="$SCRIPT_DIR/../../.env.preprod"
if [ -f "$_env_preprod" ]; then set -a; source "$_env_preprod"; set +a; fi

LOG_DIR="$SCRIPT_DIR/runs"
ERROR_LOG="$LOG_DIR/errors.log"
PID_FILE="$LOG_DIR/pids/scheduled.pid"
GOVDATA_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

WINDOW_SECS=43200   # 12 hours per mode
RESTART_DELAY=30    # seconds to wait after a crash before restarting

# Locate timeout command (Linux: timeout; macOS with coreutils: gtimeout)
TIMEOUT_CMD="$(command -v timeout 2>/dev/null || command -v gtimeout 2>/dev/null || true)"
if [ -z "$TIMEOUT_CMD" ]; then
  echo "ERROR: neither 'timeout' nor 'gtimeout' found in PATH" >&2
  exit 1
fi

# Resolve JAR — prefer GOVDATA_JAR env override, then standard name, then sih-govdata
if [ -z "${GOVDATA_JAR:-}" ]; then
  STANDARD_JAR=$(find "$GOVDATA_ROOT/build/libs" -name "calcite-govdata-*-all.jar" 2>/dev/null | head -1)
  SIH_JAR=$(find "$GOVDATA_ROOT/build/libs" -name "sih-govdata-*-SNAPSHOT.jar" 2>/dev/null | head -1)
  if [ -n "$STANDARD_JAR" ]; then export GOVDATA_JAR="$STANDARD_JAR"
  elif [ -n "$SIH_JAR" ]; then export GOVDATA_JAR="$SIH_JAR"; fi
fi

mkdir -p "$LOG_DIR/pids"
echo $$ > "$PID_FILE"

# ── Helpers ───────────────────────────────────────────────────────────────────

ts() { date '+%Y-%m-%d %H:%M:%S'; }

log_error() {
  echo "[$(ts)] $*" | tee -a "$ERROR_LOG"
}

# Format epoch as HH:MM:SS (portable: Linux -d @, macOS -r)
fmt_epoch() {
  date -d "@$1" '+%H:%M:%S' 2>/dev/null || date -r "$1" '+%H:%M:%S' 2>/dev/null || echo "?"
}

# ── Graceful shutdown ─────────────────────────────────────────────────────────

ACTIVE_POOL_PID=""
_shutdown() {
  log_error "SIGTERM received — stopping perpetual runner (PID $$)"
  if [ -n "$ACTIVE_POOL_PID" ] && kill -0 "$ACTIVE_POOL_PID" 2>/dev/null; then
    kill -TERM "$ACTIVE_POOL_PID" 2>/dev/null || true
  fi
  rm -f "$PID_FILE"
  exit 0
}
trap _shutdown SIGTERM SIGINT

# ── Mode selection ────────────────────────────────────────────────────────────

if [ -n "${1:-}" ]; then
  MODE="$1"
else
  HOUR=$(date +%H)
  if [ "$HOUR" -ge 8 ] && [ "$HOUR" -lt 20 ]; then MODE="daily"; else MODE="historical"; fi
fi

if [ "$MODE" != "daily" ] && [ "$MODE" != "historical" ]; then
  echo "Usage: $0 [daily|historical]" >&2
  exit 1
fi

# ── Window runner ─────────────────────────────────────────────────────────────
#
# Runs the pool in the given mode for up to WINDOW_SECS.
# On non-zero / non-SIGTERM exit: logs to errors.log, waits RESTART_DELAY, restarts.

run_window() {
  local mode="$1"
  local window_log="$LOG_DIR/scheduled-${mode}-$(date '+%Y%m%d-%H%M%S').log"
  local window_end=$(( $(date +%s) + WINDOW_SECS ))
  local attempt=0

  if [ "$mode" = "historical" ]; then
    export GOVDATA_START_YEAR=2010
  else
    unset GOVDATA_START_YEAR 2>/dev/null || true
  fi

  {
    echo "[$(ts)] === Starting $mode window (until $(fmt_epoch "$window_end")) ==="
    [ -n "${GOVDATA_JAR:-}" ] && echo "[$(ts)] JAR: $GOVDATA_JAR"
  } | tee -a "$window_log"

  while true; do
    local now; now=$(date +%s)
    local remaining=$(( window_end - now ))
    [ "$remaining" -le 60 ] && break   # < 1 min left in window — done

    attempt=$(( attempt + 1 ))
    echo "[$(ts)] $mode attempt $attempt (${remaining}s remaining in window)" >> "$window_log"

    set +e
    "$TIMEOUT_CMD" "$remaining" "$SCRIPT_DIR/run-pool.sh" "$mode" >> "$window_log" 2>&1 &
    ACTIVE_POOL_PID=$!
    wait "$ACTIVE_POOL_PID"
    EXIT_CODE=$?
    ACTIVE_POOL_PID=""
    set -e

    if [ "$EXIT_CODE" -eq 0 ] || [ "$EXIT_CODE" -eq 143 ]; then
      # 0 = pool completed all schemas; 143 = SIGTERM from timeout (window elapsed)
      echo "[$(ts)] $mode pool ended normally (exit $EXIT_CODE)" >> "$window_log"
      break
    else
      # Crash / OOM — log to error log and restart within remaining window time
      log_error "ERROR: $mode pool exited with code $EXIT_CODE (attempt $attempt) — restarting in ${RESTART_DELAY}s"
      echo "[$(ts)] ERROR: pool exit $EXIT_CODE — restarting; see $ERROR_LOG" >> "$window_log"
      sleep "$RESTART_DELAY"
    fi
  done

  echo "[$(ts)] === $mode window complete ===" | tee -a "$window_log"
}

# ── Main perpetual loop ───────────────────────────────────────────────────────

log_error "INFO: Perpetual runner started (PID $$), first window: $MODE"

_sync_stamp="${HOME}/.r2-last-sync"

while true; do
  run_window "$MODE"
  if [ "$MODE" = "historical" ]; then MODE="daily"; else MODE="historical"; fi

  # On MinIO: sync new files to R2 once per day after a window completes.
  if [ -f "$_env_preprod" ]; then
    _now=$(date +%s)
    _last=$(cat "$_sync_stamp" 2>/dev/null || echo 0)
    if [ $(( _now - _last )) -ge 86400 ]; then
      log_error "INFO: daily R2 sync starting"
      "$SCRIPT_DIR/sync-to-r2.sh" && echo "$_now" > "$_sync_stamp" \
        || log_error "WARNING: R2 sync failed"
    fi
  fi
done
