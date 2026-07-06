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
# pools — completions are append-only (run-pool.sh) and persist across restarts, so an
# unforced pool (e.g. the nightly multi-schema run) skips already-finished schemas on
# boot-resume. A forced run (`run-pool.sh --force`, which run-all-dq always passes)
# clears just its own queued slots' entries up front so the re-run isn't silently
# skipped; it never wipes the whole shared file, so other schemas' entries survive.
POOL_ARGS=("$@")

# Save args one-per-line so args containing spaces survive.
printf '%s\n' "${POOL_ARGS[@]+"${POOL_ARGS[@]}"}" > "$STATE_FILE"

_cleanup() {
    rm -f "$STATE_FILE"
}
trap _cleanup EXIT

"$SCRIPT_DIR/run-pool.sh" "${POOL_ARGS[@]+"${POOL_ARGS[@]}"}"
