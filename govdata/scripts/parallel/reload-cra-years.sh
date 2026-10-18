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
# reload-cra-years.sh [year ...] — load the banking.cra_small_business_lending years that are
# missing, one at a time and slowly.
#
# FFIEC's flat-files host sits behind a Cloudflare challenge that trips on request VOLUME
# (kenstott/govdata-ops#241). Production's 2009-2020 years went in during one sequential overnight
# pass on 2026-09-21/22 at about one zip request per 45 minutes and the gate never fired; from
# 2026-10-04 the daily plus 16 per-year workers asked for the same host repeatedly (138 attempts
# at 25exp_aggr.zip alone) and it has been shut since. This script repeats what worked: one year
# per run, GAP_MIN minutes between years, nothing else touching ffiec.gov, and it STOPS at the
# first failure instead of trying the next year (a failure means the gate is shut; more requests
# only keep it shut).
#
# Why the marker is cleared first: every banking year is already in historical-complete/banking.done
# (set when a failed table was still reported as success, before #982), so run-pool.sh would skip
# banking:<year>. A year is re-marked by the pool only when its run succeeds.
#
# Default years are the ones the table is missing per its observedCoverage, newest first. 2025 is
# left to the daily (its file may not be published).
#
# Usage: reload-cra-years.sh [--dry-run] [year ...]       GAP_MIN=45 by default
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DONE_FILE="$SCRIPT_DIR/runs/historical-complete/banking.done"
GAP_MIN="${GAP_MIN:-45}"
DRY_RUN=false
YEARS=()
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    [0-9][0-9][0-9][0-9]) YEARS+=("$arg") ;;
    *) echo "Unknown argument: $arg" >&2; exit 1 ;;
  esac
done
[ "${#YEARS[@]}" -gt 0 ] || YEARS=(2024 2022 2021 2011 2010)

if pgrep -f 'worker-banking' >/dev/null; then
  echo "A banking worker is running; it would hit ffiec.gov at the same time. Try again when it ends." >&2
  exit 1
fi

for i in "${!YEARS[@]}"; do
  y="${YEARS[$i]}"
  echo "[reload-cra] $(date '+%F %T') banking:$y"
  if $DRY_RUN; then
    echo "[reload-cra] dry run: would remove $y from $DONE_FILE and run: run-pool.sh -j 1 banking:$y"
  else
    sed -i "/^${y}\$/d" "$DONE_FILE"
    if ! "$SCRIPT_DIR/run-pool.sh" -j 1 "banking:$y"; then
      echo "[reload-cra] banking:$y FAILED; stopping so the host is not hit again. Years after $y not attempted." >&2
      exit 1
    fi
  fi
  if [ "$i" -lt $((${#YEARS[@]} - 1)) ]; then
    echo "[reload-cra] waiting ${GAP_MIN} min before the next year"
    $DRY_RUN || sleep $((GAP_MIN * 60))
  fi
done
echo "[reload-cra] done"
