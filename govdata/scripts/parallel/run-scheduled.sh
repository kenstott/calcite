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

# Load credentials/config from .env.prod (storage points at MinIO directly there now;
# the old .env.preprod redirect is retired). Needed for the PROD_* publish gate below.
source "$SCRIPT_DIR/common.sh"
load_env

LOG_DIR="$SCRIPT_DIR/runs"
ERROR_LOG="$LOG_DIR/errors.log"
R2_LOG="$LOG_DIR/r2-sync.log"   # detailed R2 sync output — tailed by pool_status.py
PID_FILE="$LOG_DIR/pids/scheduled.pid"
GOVDATA_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

WINDOW_SECS=43200   # 12 hours per mode
RESTART_DELAY=30    # seconds to wait after a crash before restarting

# Concurrency throttle passed to run-pool.sh's -j. run-pool otherwise admits
# workers up to the memory budget alone (MAX_WORKERS=99), which lets ~6 JVMs pound
# a single MinIO node at once and drives the 503 SlowDown / "reduce your request
# rate" / read-timeout storm. Capping concurrent workers caps the aggregate S3
# request rate at the source. Tunable via env; raise once MinIO stops throttling.
POOL_MAX_WORKERS="${POOL_MAX_WORKERS:-4}"

# Locate timeout command (Linux: timeout; macOS with coreutils: gtimeout)
TIMEOUT_CMD="$(command -v timeout 2>/dev/null || command -v gtimeout 2>/dev/null || true)"
if [ -z "$TIMEOUT_CMD" ]; then
  echo "ERROR: neither 'timeout' nor 'gtimeout' found in PATH" >&2
  exit 1
fi

# Resolve JAR — mirror common.sh's find_jar order so this and the workers agree:
# GOVDATA_JAR override, then the staged unversioned sih-govdata.jar (the build-jar
# skill's source of truth), then the standard name, then a versioned SNAPSHOT.
if [ -z "${GOVDATA_JAR:-}" ]; then
  STAGED_JAR=$(find "$GOVDATA_ROOT/build/libs" -name "sih-govdata.jar" 2>/dev/null | head -1)
  STANDARD_JAR=$(find "$GOVDATA_ROOT/build/libs" -name "calcite-govdata-*-all.jar" 2>/dev/null | head -1)
  SIH_JAR=$(find "$GOVDATA_ROOT/build/libs" -name "sih-govdata-*-SNAPSHOT.jar" 2>/dev/null | head -1)
  if [ -n "$STAGED_JAR" ]; then export GOVDATA_JAR="$STAGED_JAR"
  elif [ -n "$STANDARD_JAR" ]; then export GOVDATA_JAR="$STANDARD_JAR"
  elif [ -n "$SIH_JAR" ]; then export GOVDATA_JAR="$SIH_JAR"; fi
fi

mkdir -p "$LOG_DIR/pids"

# ── Helpers ───────────────────────────────────────────────────────────────────

ts() { date '+%Y-%m-%d %H:%M:%S'; }

# Send SIGTERM to a process and its whole descendant tree (children first). Used
# both to supersede a stale runner at startup and to tear our own pool/workers/
# embedder/R2-daemon down on shutdown. The pool runs under a `timeout` wrapper in
# its own process group, so a plain kill of one PID would orphan the rest.
tree_term() {
  local pid="$1" child
  for child in $(pgrep -P "$pid" 2>/dev/null); do tree_term "$child"; done
  kill -TERM "$pid" 2>/dev/null || true
}

log_error() {
  echo "[$(ts)] $*" | tee -a "$ERROR_LOG"
}

# Format epoch as HH:MM:SS (portable: Linux -d @, macOS -r)
fmt_epoch() {
  date -d "@$1" '+%H:%M:%S' 2>/dev/null || date -r "$1" '+%H:%M:%S' 2>/dev/null || echo "?"
}

# ── Graceful shutdown ─────────────────────────────────────────────────────────

ACTIVE_POOL_PID=""
_sync_pid=""
_cleanup() {
  # Tear down the active pool subtree and the R2 sync daemon, then release the
  # pidfile (only if it still points at us — a superseding runner may own it now).
  [ -n "$ACTIVE_POOL_PID" ] && tree_term "$ACTIVE_POOL_PID"
  [ -n "$_sync_pid" ] && tree_term "$_sync_pid"
  [ "$(cat "$PID_FILE" 2>/dev/null || true)" = "$$" ] && rm -f "$PID_FILE"
}
_shutdown() {
  log_error "signal received — stopping perpetual runner (PID $$)"
  exit 0   # triggers the EXIT trap, which runs _cleanup exactly once
}
trap _shutdown SIGTERM SIGINT
trap _cleanup EXIT

# ── Single-instance guard ─────────────────────────────────────────────────────
# Supersede any perpetual runner still alive from a previous launch, so multiple
# invocations don't race to spawn pools and fight over the concurrency cap.
if [ -f "$PID_FILE" ]; then
  _old_pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [ -n "$_old_pid" ] && [ "$_old_pid" != "$$" ] && kill -0 "$_old_pid" 2>/dev/null; then
    log_error "INFO: superseding running perpetual runner (PID $_old_pid) and its subtree"
    tree_term "$_old_pid"
    for _ in $(seq 1 20); do kill -0 "$_old_pid" 2>/dev/null || break; sleep 0.5; done
    if kill -0 "$_old_pid" 2>/dev/null; then
      log_error "WARNING: PID $_old_pid still alive after TERM — sending KILL"
      kill -KILL "$_old_pid" 2>/dev/null || true
    fi
  fi
fi
echo $$ > "$PID_FILE"

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
  local fill_mode="${2:-}"   # when the primary mode finishes early, spend the rest of the window on this mode
  local window_log="$LOG_DIR/scheduled-${mode}-$(date '+%Y%m%d-%H%M%S').log"
  local window_end=$(( $(date +%s) + WINDOW_SECS ))
  local attempt=0
  local current="$mode"      # mode currently running; may switch to fill_mode after an early finish

  {
    echo "[$(ts)] === Starting $mode window (until $(fmt_epoch "$window_end")) ==="
    [ -n "$fill_mode" ] && echo "[$(ts)] early-finish fill mode: $fill_mode"
    echo "[$(ts)] concurrency cap: -j $POOL_MAX_WORKERS (POOL_MAX_WORKERS)"
    [ -n "${GOVDATA_JAR:-}" ] && echo "[$(ts)] JAR: $GOVDATA_JAR"
  } | tee -a "$window_log"

  while true; do
    local now; now=$(date +%s)
    local remaining=$(( window_end - now ))
    [ "$remaining" -le 60 ] && break   # < 1 min left in window — done

    # Historical backfills from 2010; daily uses the default (recent) start year.
    # Re-evaluated each iteration because $current can switch to the fill mode mid-window.
    if [ "$current" = "historical" ]; then
      export GOVDATA_START_YEAR=2010
    else
      unset GOVDATA_START_YEAR 2>/dev/null || true
    fi

    attempt=$(( attempt + 1 ))
    echo "[$(ts)] $current attempt $attempt (${remaining}s remaining in window)" >> "$window_log"

    set +e
    "$TIMEOUT_CMD" "$remaining" "$SCRIPT_DIR/run-pool.sh" -j "$POOL_MAX_WORKERS" "$current" >> "$window_log" 2>&1 &
    ACTIVE_POOL_PID=$!
    wait "$ACTIVE_POOL_PID"
    EXIT_CODE=$?
    ACTIVE_POOL_PID=""
    set -e

    if [ "$EXIT_CODE" -eq 0 ]; then
      # Pool completed all schemas for the current mode before the window elapsed.
      echo "[$(ts)] $current pool completed all schemas (exit 0)" >> "$window_log"
      if [ -n "$fill_mode" ] && [ "$current" != "$fill_mode" ]; then
        echo "[$(ts)] $current finished early — filling remaining window with $fill_mode" | tee -a "$window_log"
        current="$fill_mode"
        continue
      fi
      break
    elif [ "$EXIT_CODE" -eq 143 ]; then
      # SIGTERM from timeout — the window elapsed.
      echo "[$(ts)] $current pool ended on window timeout (exit 143)" >> "$window_log"
      break
    else
      # Crash / OOM — log to error log and restart within remaining window time
      log_error "ERROR: $current pool exited with code $EXIT_CODE (attempt $attempt) — restarting in ${RESTART_DELAY}s"
      echo "[$(ts)] ERROR: pool exit $EXIT_CODE — restarting; see $ERROR_LOG" >> "$window_log"
      sleep "$RESTART_DELAY"
    fi
  done

  echo "[$(ts)] === $mode window complete ===" | tee -a "$window_log"
}

# ── Main perpetual loop ───────────────────────────────────────────────────────

log_error "INFO: Perpetual runner started (PID $$), first window: $MODE"

# When PROD_* publish creds are present, spawn a background sync daemon that copies new
# files from local MinIO to R2 every 24h. Runs out-of-band so ETL is never blocked by it.
if [ -n "${PROD_AWS_ACCESS_KEY_ID:-}" ]; then
  (
    while true; do
      sleep 86400
      # Rotate the R2 log once it grows past ~5MB so tailing it stays cheap.
      if [ -f "$R2_LOG" ] && [ "$(stat -c%s "$R2_LOG" 2>/dev/null || echo 0)" -gt 5242880 ]; then
        mv -f "$R2_LOG" "$R2_LOG.1"
      fi
      log_error "INFO: daily R2 sync starting"
      echo "[$(ts)] R2 sync starting" >> "$R2_LOG"
      # Full sync output goes to R2_LOG (pool_status.py tails it); errors.log keeps
      # only the start/complete markers. The terminal banners match the watcher's
      # "R2 sync complete|FAILED" detection so it can tell active from idle.
      if "$SCRIPT_DIR/sync-to-r2.sh" >> "$R2_LOG" 2>&1; then
        echo "[$(ts)] R2 sync complete" >> "$R2_LOG"
        log_error "INFO: daily R2 sync complete"
      else
        echo "[$(ts)] R2 sync FAILED (will retry next cycle)" >> "$R2_LOG"
        log_error "WARNING: R2 sync failed (will retry next cycle)"
      fi
    done
  ) &
  _sync_pid=$!
  log_error "INFO: R2 sync daemon started (PID $_sync_pid, runs every 24h; log: $R2_LOG)"
fi

while true; do
  if [ "$MODE" = "daily" ]; then
    # Daily runs for at most 12h; if it finishes all schemas early, use the
    # remaining window time for historical backfill rather than ending early.
    run_window "$MODE" historical
  else
    run_window "$MODE"
  fi
  if [ "$MODE" = "historical" ]; then MODE="daily"; else MODE="historical"; fi
done
