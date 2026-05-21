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

MODE="${1:-}"
if [ -z "$MODE" ]; then
  echo "Usage: $0 <historical|daily> [--force]" >&2
  exit 1
fi

FORCE=${FORCE:-false}
for arg in "${@:2}"; do
  [ "$arg" = "--force" ] && FORCE=true
done
export FORCE

case "$MODE" in
  historical|daily) ;;
  *)
    echo "Unknown mode: $MODE. Valid modes: historical, daily" >&2
    exit 1 ;;
esac

export GOVDATA_RUN_MODE="$MODE"

WORKER_ID="worker-econ-${MODE}"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

# generate_single_schema_model reads GOVDATA_RUN_MODE to set year bounds:
#   historical: startYear=GOVDATA_START_YEAR, endYear=INCREMENTAL_YEAR-1
#   daily:      startYear=GOVDATA_INCREMENTAL_START_YEAR (current year only)
# Per-table TTL gates in econ-schema.yaml handle refresh cadence within that range.
generate_single_schema_model "econ" "$MODEL_DIR/econ-${MODE}.json"

log_info "$WORKER_ID: running econ tables (mode=$MODE)"
run_etl "$MODEL_DIR/econ-${MODE}.json" "$WORKER_ID"

log_info "$WORKER_ID complete"
