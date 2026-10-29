#!/usr/bin/env bash
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
# Run a data-applying command in the single heavy slot, at low CPU and IO priority.
#   heavy.sh [--wait SECONDS] -- command args...
# Takes the one flock all heavy jobs share (GOVDATA_HEAVY_LOCK); fails fast (exit 75) when the slot
# is taken and --wait is 0 (default), or after waiting SECONDS. Memory is checked by admit.py, not here.
set -euo pipefail
LOCK="${GOVDATA_HEAVY_LOCK:-/tmp/govdata-heavy.lock}"
WAIT=0
while [ $# -gt 0 ]; do
  case "$1" in
    --wait) WAIT="$2"; shift 2 ;;
    --) shift; break ;;
    *) echo "Usage: $0 [--wait SECONDS] -- command args..." >&2; exit 2 ;;
  esac
done
[ $# -gt 0 ] || { echo "no command given" >&2; exit 2; }
exec 9>"$LOCK"
if [ "$WAIT" -gt 0 ]; then
  flock -w "$WAIT" 9 || { echo "heavy slot still taken after ${WAIT}s" >&2; exit 75; }
else
  flock -n 9 || { echo "heavy slot taken" >&2; exit 75; }
fi
exec nice -n 10 ionice -c2 -n7 "$@"
