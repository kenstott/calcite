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

# --reset explicitly clears the completed checkpoint; otherwise it persists
# across restarts so already-finished schemas are never re-run.
if $RESET; then
  rm -f "$COMPLETED_FILE"
fi

# Save args (without --reset) one-per-line so args containing spaces survive.
printf '%s\n' "${POOL_ARGS[@]+"${POOL_ARGS[@]}"}" > "$STATE_FILE"

_cleanup() {
    rm -f "$STATE_FILE"
}
trap _cleanup EXIT

"$SCRIPT_DIR/run-pool.sh" "${POOL_ARGS[@]+"${POOL_ARGS[@]}"}"
