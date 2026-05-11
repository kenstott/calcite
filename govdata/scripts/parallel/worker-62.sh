#!/usr/bin/env bash
# Worker 62: Cyber initial load — full NVD catalog, CWE, KEV, static standards, OTX backfill
# Run once on first-time setup before enabling recurring cadence workers (63-65).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

"$SCRIPT_DIR/worker-cyber.sh" initial
