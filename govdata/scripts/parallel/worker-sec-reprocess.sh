#!/usr/bin/env bash
# Reprocess specific SEC accessions regardless of existing tracker state.
#
# Usage:
#   worker-sec-reprocess.sh [--force-download] <accession1> [accession2 ...]
#
# --force-download
#   Also delete the raw EDGAR cache for each accession before reprocessing,
#   so EDGAR documents are re-fetched from the SEC website.
#
# Accession format: XXXXXXXXXX-YY-ZZZZZZ
#   The first 10 characters are the filing agent's CIK (NOT the company CIK).
#   YY is the 2-digit year (e.g. 24 → 2024).
#   The model uses _ALL_EDGAR_FILERS so the full index is searched regardless of filer.
#
# Examples:
#   worker-sec-reprocess.sh 0000320193-24-000123
#   worker-sec-reprocess.sh --force-download 0000320193-24-000123 0000789019-23-000456
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

WORKER_ID="worker-sec-reprocess"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

FORCE_DOWNLOAD=false
ACCESSIONS=()

for arg in "$@"; do
  if [ "$arg" = "--force-download" ]; then
    FORCE_DOWNLOAD=true
  else
    ACCESSIONS+=("$arg")
  fi
done

if [ ${#ACCESSIONS[@]} -eq 0 ]; then
  echo "Usage: $(basename "$0") [--force-download] <accession1> [accession2 ...]" >&2
  exit 1
fi

# Determine year range from accession numbers.
# Accession format: XXXXXXXXXX-YY-ZZZZZZ  (first 10 chars are the filer/agent CIK, NOT company CIK)
MIN_YEAR=9999
MAX_YEAR=0

for acc in "${ACCESSIONS[@]}"; do
  yy="${acc:11:2}"
  year="20${yy}"
  [ "$year" -lt "$MIN_YEAR" ] && MIN_YEAR="$year"
  [ "$year" -gt "$MAX_YEAR" ] && MAX_YEAR="$year"
done

if $FORCE_DOWNLOAD; then
  log_info "$WORKER_ID --force-download: deleting raw EDGAR cache for ${#ACCESSIONS[@]} accession(s)"
  for acc in "${ACCESSIONS[@]}"; do
    filer_cik="${acc:0:10}"
    cache_path="$(echo "${GOVDATA_CACHE_DIR}/sec/${filer_cik}/${acc}/" | sed 's|^s3://|r2:|')"
    log_info "  Deleting: $cache_path"
    rclone purge "$cache_path" 2>/dev/null || true
  done
fi

ACCESSIONS_STR="${ACCESSIONS[*]}"

MODEL_FILE="$MODEL_DIR/sec-reprocess-$(date +%Y%m%d_%H%M%S).json"
generate_sec_reprocess_model "$ACCESSIONS_STR" "$MIN_YEAR" "$MAX_YEAR" "$MODEL_FILE"

log_info "$WORKER_ID reprocessing ${#ACCESSIONS[@]} accession(s) (years ${MIN_YEAR}-${MAX_YEAR}): ${ACCESSIONS[*]}"
run_etl "$MODEL_FILE" "$WORKER_ID"
log_info "$WORKER_ID complete"
