#!/usr/bin/env bash
# Worker 68: Health daily — incremental clinical trials delta.
# Schedule: every 24 hours (e.g., 06:30 UTC via cron or run-pool.sh 68)
# Optional: set HEALTH_TRIALS_SINCE_DATE in .env.prod (ISO date; blank = full fetch)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

"$SCRIPT_DIR/worker-health.sh" daily
