#!/usr/bin/env bash
# Worker 74: Energy initial load — full historical backfill from GOVDATA_START_YEAR (default 2010).
# Run once on first-time setup before enabling recurring cadence workers (75-77).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

"$SCRIPT_DIR/worker-energy.sh" initial
