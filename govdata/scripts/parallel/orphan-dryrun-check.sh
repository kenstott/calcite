#!/usr/bin/env bash
#
# Daily dry run of the Iceberg metadata/ orphan pass for every maintained table, with an alert when a
# table's would-delete count exceeds the threshold (orphan-dryrun-alert.py). Read-only: never passes
# --delete. Bounded by a timeout, once per day (stamp), and every failure is logged and swallowed so
# the caller's ETL is never failed or delayed by it.
#
# Usage: orphan-dryrun-check.sh [LOG]      GOVDATA_ORPHAN_THRESHOLD overrides the default threshold.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_DIR="${GOVDATA_ORPHAN_DRYRUN_DIR:-$SCRIPT_DIR/runs/orphan-dryrun}"
BOUND_SECS="${GOVDATA_ORPHAN_DRYRUN_TIMEOUT:-3000}"
STAMP="$RUN_DIR/.last-run"
LOG="${1:-$RUN_DIR/last.log}"
ts() { date '+%Y-%m-%d %H:%M:%S'; }

mkdir -p "$RUN_DIR"
today="$(date +%Y-%m-%d)"
if [ -f "$STAMP" ] && [ "$(cat "$STAMP" 2>/dev/null)" = "$today" ]; then
  echo "[$(ts)] orphan dry run: already ran today" >> "$LOG"
  exit 0
fi
echo "$today" > "$STAMP"

out="$RUN_DIR/dryrun-$today.log"
echo "[$(ts)] orphan dry run: starting (bound ${BOUND_SECS}s, log $out)" >> "$LOG"
(
  # shellcheck source=common.sh
  source "$SCRIPT_DIR/common.sh" && load_env
  timeout "$BOUND_SECS" "$SCRIPT_DIR/../orphan-file-cleanup.sh" --metadata-only > "$out" 2>&1
) || echo "[$(ts)] orphan dry run: scan ended non-zero (partial results are still evaluated)" >> "$LOG"

alert_args=(--log "$out" --state-dir "$RUN_DIR")
[ -n "${GOVDATA_ORPHAN_THRESHOLD:-}" ] && alert_args+=(--threshold "$GOVDATA_ORPHAN_THRESHOLD")
if python3 -I "$SCRIPT_DIR/orphan-dryrun-alert.py" "${alert_args[@]}" >> "$LOG" 2>&1; then
  echo "[$(ts)] orphan dry run: evaluated" >> "$LOG"
else
  echo "[$(ts)] orphan dry run: alert step failed (see above); the ETL is unaffected" >> "$LOG"
fi
exit 0
