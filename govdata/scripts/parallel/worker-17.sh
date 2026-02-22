#!/usr/bin/env bash
# Worker 17: SEC 2010 (first XBRL year)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

WORKER_ID="worker-17"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

generate_sec_model 2010 2010 "$MODEL_DIR/sec-2010.json"
run_etl "$MODEL_DIR/sec-2010.json" "$WORKER_ID"

log_info "$WORKER_ID complete"
