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

# Accept positional mode arg, or fall back to GOVDATA_RUN_MODE env var
MODE="${1:-${GOVDATA_RUN_MODE:-}}"
if [ -z "$MODE" ]; then
  echo "Usage: $0 <historical|daily> [--force]  (or set GOVDATA_RUN_MODE)" >&2
  exit 1
fi

FORCE=${FORCE:-false}
for arg in "${@:2}"; do
  [ "$arg" = "--force" ] && FORCE=true
done
export FORCE

WORKER_ID="worker-60-${MODE}"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

INCREMENTAL_YEAR=${GOVDATA_INCREMENTAL_START_YEAR:-2026}

case "$MODE" in

  historical)
    generate_fec_model "$MODEL_DIR/fec-historical.json" 2010
    # Unset env var so GovDataSchemaFactory's system property (startYear=2010) governs
    # minYear resolution — VariableResolver checks env before system property.
    unset GOVDATA_START_YEAR
    run_etl "$MODEL_DIR/fec-historical.json" "$WORKER_ID"
    ;;

  daily)
    generate_fec_model "$MODEL_DIR/fec-daily.json" "$INCREMENTAL_YEAR"
    # Unset env var so GovDataSchemaFactory's system property (startYear=INCREMENTAL_YEAR)
    # governs minYear resolution — filters fec_election_cycles to current cycle only.
    unset GOVDATA_START_YEAR
    run_etl "$MODEL_DIR/fec-daily.json" "$WORKER_ID"
    ;;

  *)
    echo "Unknown mode: $MODE. Valid modes: historical, daily" >&2
    exit 1
    ;;
esac

log_info "$WORKER_ID complete"
