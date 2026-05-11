#!/usr/bin/env bash
# Chunks backfill worker: writes vectorized_chunks parquet for accessions that have all
# core staging tables (metadata, facts, contexts, mda, relationships) but no chunks entry.
#
# Usage:
#   worker-vss-backfill.sh [start_year] [end_year]
#
# Defaults to the full primary range (2010 to current year) when no args given.
# Run one year at a time to limit JVM memory pressure, e.g.:
#   worker-vss-backfill.sh 2025 2025
#
# Prerequisites:
#   - ENABLE_VECTORIZATION must resolve to true in sec-schema.yaml (it defaults to true)
#   - Core staging parquet already exists on S3 for the target year range
#   - After this worker completes, run vss-gpu-runner.sh to generate embeddings for
#     the new chunks rows (embedding column is NULL until the GPU step runs)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

WORKER_ID="worker-vss-backfill"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

START_YEAR="${1:-2010}"
END_YEAR="${2:-$(date +%Y)}"

log_info "$WORKER_ID starting chunks backfill for years $START_YEAR-$END_YEAR"

for YEAR in $(seq "$START_YEAR" "$END_YEAR"); do
  MODEL_FILE="$MODEL_DIR/sec-chunks-backfill-${YEAR}.json"
  generate_sec_chunks_backfill_model "$YEAR" "$YEAR" "$MODEL_FILE"
  log_info "$WORKER_ID processing year $YEAR"
  run_etl "$MODEL_FILE" "$WORKER_ID"
done

log_info "$WORKER_ID complete"
