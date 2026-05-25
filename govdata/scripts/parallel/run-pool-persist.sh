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

# Save args one-per-line so args containing spaces survive the round-trip.
printf '%s\n' "$@" > "$STATE_FILE"

_cleanup() {
    rm -f "$STATE_FILE"
}
trap _cleanup EXIT

"$SCRIPT_DIR/run-pool.sh" "$@"
