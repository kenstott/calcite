#!/usr/bin/env bash
# Worker 19: Pre-XBRL stock prices 2004-2006
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

WORKER_ID="worker-19"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

generate_prices_model 2004 2006 "$MODEL_DIR/prices-2004-2006.json"
run_etl "$MODEL_DIR/prices-2004-2006.json" "$WORKER_ID"

log_info "$WORKER_ID complete"
