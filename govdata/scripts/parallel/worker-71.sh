#!/usr/bin/env bash
# Worker 71: Education initial load — all 10 edu tables, full historical year range.
# Run once on first-time setup before enabling recurring cadence workers (72-73).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

"$SCRIPT_DIR/worker-edu.sh" initial
