#!/usr/bin/env bash
# Worker 60: FEC schema (campaign finance bulk downloads)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

WORKER_ID="worker-60"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

generate_fec_model "$MODEL_DIR/fec.json"
run_etl "$MODEL_DIR/fec.json" "$WORKER_ID"

log_info "$WORKER_ID complete"
