#!/usr/bin/env bash
#
# Wrapper around run-pool.sh that enables automatic resume after a machine hang.
# Saves the invocation args to ~/.run-pool.state, then deletes the file on any
# clean exit (normal completion, script error, Ctrl+C, SIGTERM).  A machine hang
# kills processes before cleanup can run, leaving the file intact.
# run-pool-resume.sh reads the file at boot and re-invokes this script with the
# same args — but only if the file is present.
#
# Usage: run-pool-persist.sh [same args as run-pool.sh]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STATE_FILE="${HOME}/.run-pool.state"
COMPLETED_FILE="${HOME}/.run-pool-completed.state"

# The completed-slots checkpoint (${COMPLETED_FILE}) is SHARED across concurrent
# pools and is never wiped here — completions are append-only (run-pool.sh) and the
# checkpoint persists across restarts so already-finished schemas are never re-run.
# To force a single slot to re-run, purge that one table's data+tracker and use
# --etl-resume; do not delete the shared checkpoint.
POOL_ARGS=("$@")

# Save args one-per-line so args containing spaces survive.
printf '%s\n' "${POOL_ARGS[@]+"${POOL_ARGS[@]}"}" > "$STATE_FILE"

_cleanup() {
    rm -f "$STATE_FILE"
}
trap _cleanup EXIT

"$SCRIPT_DIR/run-pool.sh" "${POOL_ARGS[@]+"${POOL_ARGS[@]}"}"
