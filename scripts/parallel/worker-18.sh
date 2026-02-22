#!/usr/bin/env bash
# Worker 18: Pre-XBRL stock prices 2007-2009
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

WORKER_ID="worker-18"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

generate_prices_model 2007 2009 "$MODEL_DIR/prices-2007-2009.json"
run_etl "$MODEL_DIR/prices-2007-2009.json" "$WORKER_ID"

log_info "$WORKER_ID complete"
