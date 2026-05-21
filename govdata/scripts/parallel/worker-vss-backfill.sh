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
