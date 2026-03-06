#!/usr/bin/env bash
# Worker 11: SEC Primary 2016 (10-K/10-Q + Stock Prices)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

WORKER_ID="worker-11"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

generate_sec_primary_model 2016 2016 "$MODEL_DIR/sec-primary-2016.json"
ETL_NO_COMPACT=true run_etl "$MODEL_DIR/sec-primary-2016.json" "$WORKER_ID"

log_info "$WORKER_ID complete"
