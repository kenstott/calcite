#!/usr/bin/env bash
# Worker 63: Cyber daily — delta NVD CVEs (last 1 day) + KEV refresh
# Schedule: every 24 hours (e.g., 06:00 UTC via cron or run-pool.sh 63)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

"$SCRIPT_DIR/worker-cyber.sh" daily
