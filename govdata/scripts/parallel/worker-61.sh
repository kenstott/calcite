#!/usr/bin/env bash
# Worker 61: Federal Register (rules, proposed rules, notices, presidential docs)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

WORKER_ID="worker-61"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

generate_single_schema_model "fedregister" "$MODEL_DIR/fedregister.json"
run_etl "$MODEL_DIR/fedregister.json" "$WORKER_ID"

log_info "$WORKER_ID complete"
