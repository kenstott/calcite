#!/usr/bin/env bash
# Worker 77: Energy annual — utility survey (EIA-861), power plant inventory (EIA-860),
#            state energy consumption (SEDS), coal mine production (MSHA).
# Schedule: annually (e.g., October 01:00 UTC after EIA-861/EIA-860 annual release)
# Optional: set ENERGY_SINCE_YEAR in .env.prod to limit year range
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

"$SCRIPT_DIR/worker-energy.sh" annual
