#!/usr/bin/env bash
# Worker 15: SEC Primary 2012 (10-K/10-Q + Stock Prices)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

WORKER_ID="worker-15"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

generate_sec_primary_model 2012 2012 "$MODEL_DIR/sec-primary-2012.json"
ETL_NO_COMPACT=true run_etl "$MODEL_DIR/sec-primary-2012.json" "$WORKER_ID"

log_info "$WORKER_ID complete"
