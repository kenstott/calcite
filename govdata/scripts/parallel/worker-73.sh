#!/usr/bin/env bash
# Worker 73: Education biennial — NAEP assessments + CRDC civil rights data.
# Schedule: annually (e.g., October 02:00 UTC; sources return unchanged data between release years)
# Optional: set EDU_NAEP_SINCE_YEAR, EDU_CRDC_SINCE_YEAR in .env.prod (4-digit year; blank = full fetch)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

"$SCRIPT_DIR/worker-edu.sh" biennial
