#!/usr/bin/env bash
# Worker 41: REF schema (GLEIF entities, CIK mapping, OpenFIGI)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

WORKER_ID="worker-41"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

# Resolve the daily GLEIF golden copy CSV URL
discover_gleif_url

generate_ref_model "$MODEL_DIR/ref.json"
run_etl "$MODEL_DIR/ref.json" "$WORKER_ID"

log_info "$WORKER_ID complete"
