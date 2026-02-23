#!/usr/bin/env bash
# Worker 04: SEC Primary 2023 (10-K/10-Q + Stock Prices)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

WORKER_ID="worker-04"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

generate_sec_primary_model 2023 2023 "$MODEL_DIR/sec-primary-2023.json"
run_etl "$MODEL_DIR/sec-primary-2023.json" "$WORKER_ID"

log_info "$WORKER_ID complete"
