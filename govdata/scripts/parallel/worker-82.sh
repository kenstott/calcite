#!/usr/bin/env bash
# Worker 82: Lands historical — all 7 tables, full history from GOVDATA_START_YEAR.
# Schedule: once (first setup)
# Heap: 4 GB / 6 GB max (ONRR bulk CSV is large)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

"$SCRIPT_DIR/worker-lands.sh" historical
