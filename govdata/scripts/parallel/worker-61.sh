#!/usr/bin/env bash
# Worker 61: Federal Register — rules, proposed rules, notices, presidential docs + agency registry.
# Schedule: historical (once) and daily (recurring); GOVDATA_RUN_MODE governs which.
# Heap: 2 GB / 3 GB max
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

"$SCRIPT_DIR/worker-fedregister.sh" "${GOVDATA_RUN_MODE:-daily}"
