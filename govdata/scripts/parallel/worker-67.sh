#!/usr/bin/env bash
# Worker 67: Health initial load — all 15 health tables, no incremental filters.
# Run once on first-time setup before enabling recurring cadence workers (68-70).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

"$SCRIPT_DIR/worker-health.sh" initial
