#!/usr/bin/env bash
# Worker 40: Stock Prices (Stooq) 2010-2026
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

WORKER_ID="worker-40"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

generate_prices_model 2010 2026 "$MODEL_DIR/prices-2010-2026.json"
run_etl "$MODEL_DIR/prices-2010-2026.json" "$WORKER_ID"

log_info "$WORKER_ID complete"
