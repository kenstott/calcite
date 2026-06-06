#!/usr/bin/env bash
#
# Wrapper around run-pool.sh that enables automatic resume after a machine hang.
# Saves the invocation args to ~/.run-pool.state, then deletes the file on any
# clean exit (normal completion, script error, Ctrl+C, SIGTERM).  A machine hang
# kills processes before cleanup can run, leaving the file intact.
# run-pool-resume.sh reads the file at boot and re-invokes this script with the
# same args — but only if the file is present.
#
# --reset   Clear the completed-slots checkpoint before running (re-run everything).
#           Stripped from args before saving to state file so a resume after hang
#           does not also reset.
#
# Usage: run-pool-persist.sh [--reset] [same args as run-pool.sh]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STATE_FILE="${HOME}/.run-pool.state"
COMPLETED_FILE="${HOME}/.run-pool-completed.state"

# ── parse --reset out of args before saving state ────────────────────────────
RESET=false
POOL_ARGS=()
for arg in "$@"; do
  if [ "$arg" = "--reset" ]; then
    RESET=true
  else
    POOL_ARGS+=("$arg")
  fi
done

# --reset clears the completed checkpoint so this run re-runs everything;
# otherwise it persists across restarts so already-finished schemas are never
# re-run. The checkpoint file is SHARED across concurrent pools, so:
#   - when a --schema is given, clear only that schema's slots (leave other
#     concurrent pools' checkpoints intact);
#   - otherwise (no --schema) clear the whole file.
# flock serializes against the appender in run-pool.sh, and the read-filter-write
# uses an atomic temp+rename so a concurrent append can't be lost mid-rewrite.
if $RESET; then
  _reset_schema=""
  for ((_i=0; _i<${#POOL_ARGS[@]}; _i++)); do
    if [ "${POOL_ARGS[$_i]}" = "--schema" ]; then
      _reset_schema="${POOL_ARGS[$((_i+1))]:-}"
      break
    fi
  done
  (
    flock 9
    if [ -n "$_reset_schema" ] && [ -f "$COMPLETED_FILE" ]; then
      grep -v "^${_reset_schema}:" "$COMPLETED_FILE" > "${COMPLETED_FILE}.tmp" || true
      mv -f "${COMPLETED_FILE}.tmp" "$COMPLETED_FILE"
    else
      rm -f "$COMPLETED_FILE"
    fi
  ) 9>>"${COMPLETED_FILE}.lock"
fi

# Save args (without --reset) one-per-line so args containing spaces survive.
printf '%s\n' "${POOL_ARGS[@]+"${POOL_ARGS[@]}"}" > "$STATE_FILE"

_cleanup() {
    rm -f "$STATE_FILE"
}
trap _cleanup EXIT

"$SCRIPT_DIR/run-pool.sh" "${POOL_ARGS[@]+"${POOL_ARGS[@]}"}"
