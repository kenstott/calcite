#!/usr/bin/env bash
# Worker 72: Education annual — CCD, IPEDS, and College Scorecard annual refreshes.
# Schedule: annually (e.g., November 01:00 UTC after IPEDS release, or January after financials)
# Optional: set EDU_CCD_SINCE_YEAR, EDU_IPEDS_SINCE_YEAR, EDU_IPEDS_FINANCE_SINCE_YEAR,
#           EDU_SCORECARD_SINCE_YEAR in .env.prod (4-digit year; blank = full fetch)
# Optional: set COLLEGE_SCORECARD_API_KEY in .env.prod to include College Scorecard tables
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

"$SCRIPT_DIR/worker-edu.sh" annual
