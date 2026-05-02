#!/usr/bin/env bash
# Worker 69: Health weekly — CDC COVID vaccinations delta + CDC mortality refresh.
# Schedule: weekly (e.g., Monday 03:00 UTC via cron or run-pool.sh 69)
# Optional: set HEALTH_CDC_COVID_SINCE_DATE in .env.prod (ISO date; blank = full fetch)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

"$SCRIPT_DIR/worker-health.sh" weekly
