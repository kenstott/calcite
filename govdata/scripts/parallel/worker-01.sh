#!/usr/bin/env bash
# Worker 01: SEC Primary current year back to 2026 (10-K/10-Q + Stock Prices)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

WORKER_ID="worker-01"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

CURRENT_YEAR=$(date +%Y)
generate_sec_primary_model 2026 "$CURRENT_YEAR" "$MODEL_DIR/sec-primary-current.json"
ETL_NO_COMPACT=true run_etl "$MODEL_DIR/sec-primary-current.json" "$WORKER_ID"

log_info "$WORKER_ID complete"
