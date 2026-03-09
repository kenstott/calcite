#!/usr/bin/env bash
# Worker 23: SEC Secondary current year back to 2026 (8-K, Proxy, Insider, 13F, 13D/G)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

WORKER_ID="worker-23"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

CURRENT_YEAR=$(date +%Y)
generate_sec_secondary_model 2026 "$CURRENT_YEAR" "$MODEL_DIR/sec-secondary-current.json"
run_etl "$MODEL_DIR/sec-secondary-current.json" "$WORKER_ID"

log_info "$WORKER_ID complete"
