#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
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
