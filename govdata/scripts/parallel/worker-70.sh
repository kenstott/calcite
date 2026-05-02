#!/usr/bin/env bash
# Worker 70: Health monthly — BRFSS, Medicaid drug utilization, CMS, FDA catalogs, RxNorm.
# Schedule: monthly (e.g., 1st of month 02:00 UTC via cron or run-pool.sh 70)
# Optional: set HEALTH_BRFSS_SINCE_YEAR, MEDICAID_SINCE_YEAR, MEDICAID_SINCE_QUARTER in .env.prod
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

"$SCRIPT_DIR/worker-health.sh" monthly
