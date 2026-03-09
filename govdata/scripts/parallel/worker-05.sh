#!/usr/bin/env bash
# Worker 05: SEC Primary 2022 (10-K/10-Q + Stock Prices)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

WORKER_ID="worker-05"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

generate_sec_primary_model 2022 2022 "$MODEL_DIR/sec-primary-2022.json"
run_etl "$MODEL_DIR/sec-primary-2022.json" "$WORKER_ID"

log_info "$WORKER_ID complete"
