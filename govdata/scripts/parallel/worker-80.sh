#!/usr/bin/env bash
# Worker 80: Patents historical load — full backfill from GOVDATA_START_YEAR (default 2010)
# through GOVDATA_INCREMENTAL_START_YEAR - 1.
# Run once on first-time setup before enabling the recurring daily worker (81).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

"$SCRIPT_DIR/worker-patents.sh" historical
