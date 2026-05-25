#!/usr/bin/env bash
#
# Boot-time resume script.  Run via systemd @reboot service.
# If ~/.run-pool.state exists, run-pool was interrupted by a machine hang —
# re-invoke run-pool-persist.sh with the saved args.
# If the file is absent, run-pool exited cleanly and nothing happens.
#
set -euo pipefail

STATE_FILE="${HOME}/.run-pool.state"

if [[ ! -f "$STATE_FILE" ]]; then
    exit 0
fi

mapfile -t args < "$STATE_FILE"

if [[ "${#args[@]}" -eq 0 ]]; then
    rm -f "$STATE_FILE"
    exit 0
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "run-pool-resume: restarting with args: ${args[*]}"
exec "$SCRIPT_DIR/run-pool-persist.sh" "${args[@]}"
