#!/usr/bin/env bash
# Worker 36: SEC Secondary 2013 (8-K, Proxy, Insider)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

WORKER_ID="worker-36"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

generate_sec_secondary_model 2013 2013 "$MODEL_DIR/sec-secondary-2013.json"
run_etl "$MODEL_DIR/sec-secondary-2013.json" "$WORKER_ID"

log_info "$WORKER_ID complete"
