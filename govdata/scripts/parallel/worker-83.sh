#!/usr/bin/env bash
# Worker 83: Lands daily — all recurring lands tables, gated by release window.
# Schedule: daily (each table gates itself; annual vs. monthly cadence varies by table)
# Heap: 2 GB / 3 GB max
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

"$SCRIPT_DIR/worker-lands.sh" daily
