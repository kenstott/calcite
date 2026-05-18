#!/usr/bin/env bash
# Worker 84: Economic Reference Data — all 7 reference tables, TTL-gated refresh.
# Schedule: daily (each table gates itself via incrementalTtlDays + releaseWindow)
# Heap: 2 GB / 3 GB max
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

"$SCRIPT_DIR/worker-econ-reference.sh"
