#!/usr/bin/env bash
# Worker 75: Energy weekly — EIA natural gas storage + petroleum stocks.
# Schedule: weekly (e.g., Friday 06:00 UTC after EIA Thursday release)
# Optional: set ENERGY_SINCE_YEAR in .env.prod to limit year range (default 2000)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

"$SCRIPT_DIR/worker-energy.sh" weekly
