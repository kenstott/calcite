#!/usr/bin/env bash
# Worker 65: Cyber hourly — live IOC feeds (URLhaus, MalwareBazaar, Feodo, ThreatFox) + OTX delta
# Schedule: every 1-4 hours via cron or run-pool.sh 65
# Set CYBER_OTX_DELTA_DAYS=1 in environment for delta OTX fetch (recommended for recurring runs).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

export CYBER_OTX_DELTA_DAYS="${CYBER_OTX_DELTA_DAYS:-1}"
"$SCRIPT_DIR/worker-cyber.sh" hourly
