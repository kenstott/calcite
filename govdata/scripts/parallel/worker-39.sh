#!/usr/bin/env bash
# Worker 39: SEC Secondary (8-K, Proxy, Insider, 13F, 13D/G)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

WORKER_ID="worker-39"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

generate_sec_secondary_model 2010 2010 "$MODEL_DIR/sec-secondary-2010.json"
ETL_NO_COMPACT=true run_etl "$MODEL_DIR/sec-secondary-2010.json" "$WORKER_ID"

log_info "$WORKER_ID complete"
