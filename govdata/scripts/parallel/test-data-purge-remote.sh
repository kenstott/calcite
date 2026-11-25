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
# data_purge.sh --env prod deletes on the remote the environment names, never on a built-in one,
# and stops when the environment names none. Every case is a --dry-run against a temporary
# GOVDATA_HOME with stub rclone and psql, so no object store or tracker is reached.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fail=0
ok() { echo "PASS $1"; }
bad() { echo "FAIL $1"; fail=1; }

T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
mkdir -p "$T/home" "$T/bin"
printf '#!/bin/sh\necho "$*" >> "%s/rclone-calls"\n[ "$1" = listremotes ] && printf "minio:\\nr2:\\nother:\\n"\nexit 0\n' "$T" > "$T/bin/rclone"
printf '#!/bin/sh\ncase "$*" in *to_regclass*) echo t ;; *) echo "0|0" ;; esac\n' > "$T/bin/psql"
chmod +x "$T/bin/rclone" "$T/bin/psql"

run() { # env-file-lines [purge args...] -> output in $out, rc in $rc
  local envfile="$1"; shift
  printf '%s\n' "AWS_ACCESS_KEY_ID=stub" "GOVDATA_TRACKER_PG_URL=jdbc:postgresql://stub:1/stub" \
    "GOVDATA_PARQUET_DIR=s3://govdata-parquet-v1" "$envfile" > "$T/home/.env.prod"
  rm -f "$T/rclone-calls"
  out="$(env -i HOME="$T" PATH="$T/bin:/usr/bin:/bin" GOVDATA_HOME="$T/home" \
    bash "$HERE/../data_purge.sh" --schema fec --tables t1 --dry-run "$@" 2>&1)"; rc=$?
}

run "GOVDATA_RCLONE_REMOTE=minio"
[ $rc -eq 0 ] && [[ "$out" == *"Would purge: minio:govdata-parquet-v1/fec/t1"* && "$out" != *r2:* ]] \
  && ok "prod purge targets GOVDATA_RCLONE_REMOTE" || bad "configured remote: rc=$rc $out"

run ""
[ $rc -ne 0 ] && [[ "$out" == *"GOVDATA_RCLONE_REMOTE is not set"* && "$out" != *"Would purge"* ]] \
  && ok "prod purge stops when no remote is configured" || bad "unset remote: rc=$rc $out"

run "" --remote other
[ $rc -eq 0 ] && [[ "$out" == *"Would purge: other:govdata-parquet-v1/fec/t1"* ]] \
  && ok "--remote selects the remote" || bad "--remote: rc=$rc $out"

run "GOVDATA_RCLONE_REMOTE=absent"
[ $rc -ne 0 ] && [[ "$out" == *"'absent:' is not configured"* ]] \
  && ok "an unknown remote stops the run" || bad "unknown remote: rc=$rc $out"

grep -qvx listremotes "$T/rclone-calls" 2>/dev/null && bad "dry run called rclone beyond listremotes" \
  || ok "dry run reads the remote list only"

exit $fail
