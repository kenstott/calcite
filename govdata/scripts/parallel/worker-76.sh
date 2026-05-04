#!/usr/bin/env bash
# Worker 76: Energy monthly — electricity generation/prices, capacity changes,
#            fossil fuel production, refinery operations, crude oil imports.
# Schedule: monthly (e.g., 15th of each month 03:00 UTC; ~2-month EIA data lag)
# Optional: set ENERGY_SINCE_YEAR in .env.prod to limit year range
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

"$SCRIPT_DIR/worker-energy.sh" monthly
