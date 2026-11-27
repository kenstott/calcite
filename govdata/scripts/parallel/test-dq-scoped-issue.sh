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
# A PASS retires the schema's DQ issue only when the run checked the whole schema: a --tables run
# comments on it, leaves it open and says what it checked. Runs a copy of worker-dq-run.sh in a temp
# tree with the real duckdb and every outward-facing command stubbed; the gh stub records its calls.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fail=0
ok() { echo "PASS $1"; }
bad() { echo "FAIL $1"; fail=1; }

DUCKDB="$(command -v duckdb || echo "$HOME/.local/bin/duckdb")"
[ -x "$DUCKDB" ] || { echo "FAIL duckdb CLI not found"; exit 1; }

T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
S="$T/govdata/scripts"; P="$S/parallel"; CALLS="$T/calls"
mkdir -p "$P" "$T/bin" "$T/claims"
cp "$HERE"/../*.sh "$S/"
cp "$HERE"/*.sh "$HERE"/*.py "$P/"
# the runner's S3 secret needs the DuckDB extensions installed under the real home
printf '#!/bin/sh\nHOME="%s" exec "%s" "$@"\n' "$HOME" "$DUCKDB" > "$T/bin/duckdb"
for c in aws rclone psql curl; do printf '#!/bin/sh\nexit 0\n' > "$T/bin/$c"; done
# gh: answers the open-issue lookup from $T/open and records every call
cat > "$T/bin/gh" <<GH
#!/bin/sh
echo "gh \$*" >> "$CALLS"
[ "\$1 \$2" = "issue list" ] && cat "$T/open"
exit 0
GH
chmod +x "$T/bin/"*

write_sql() { # $1 = status of the one check
  cat > "$S/zziss_dq.sql" <<SQL
CREATE TEMP TABLE dq_results (schema VARCHAR, tbl VARCHAR, test VARCHAR, status VARCHAR, value VARCHAR, threshold VARCHAR, detail VARCHAR);
INSERT INTO dq_results SELECT 'zziss', 'good_one', 'row_count', '$1', '1', '1', 's3://\${GOVDATA_DQ_BUCKET}/zziss/good_one';
SQL
}
run() { # open-issue-number args... -> output in $out, rc in $rc, gh calls in $CALLS
  echo "$1" > "$T/open"; shift
  : > "$CALLS"
  out=$(env -i HOME="$T" PATH="$T/bin:/usr/bin:/bin" GOVDATA_TMPDIR="$T/tmp" GOVDATA_CLAIM_DIR="$T/claims" \
    GH_TOKEN=x GOVDATA_DQ_BUCKET=govdata-parquet-v1-dq GOVDATA_DQ_RCLONE_REMOTE=zzremote \
    AWS_ENDPOINT_OVERRIDE="http://127.0.0.1:1" AWS_ACCESS_KEY_ID=x AWS_SECRET_ACCESS_KEY=y \
    GOVDATA_CACHE_DIR="s3://zz-raw" timeout 60 bash "$P/worker-dq-run.sh" zziss "$@" 2>&1); rc=$?
}
retired() { grep -qE '^gh issue close( |$)' "$CALLS"; }
commented() { grep -q '^gh issue comment 42 ' "$CALLS"; }
scope_named() { grep -qF "Scoped run: only \`$1\` checked" "$CALLS"; }

write_sql pass
run 42
[ $rc -eq 0 ] && retired \
  && ok "a whole-schema PASS retires the issue" || bad "full pass: rc=$rc $(cat "$CALLS") $out"

run 42 --tables good_one
[ $rc -eq 0 ] && ! retired \
  && ok "a --tables PASS leaves the issue open" || bad "scoped pass: rc=$rc $(cat "$CALLS") $out"
commented && scope_named good_one \
  && ok "...and comments what it checked" || bad "scoped pass comment: $(cat "$CALLS")"

run "" --tables good_one
[ $rc -eq 0 ] && ! grep -qE '^gh issue (close|create|comment)' "$CALLS" \
  && ok "a --tables PASS with no open issue writes nothing" || bad "no issue: $(cat "$CALLS") $out"

write_sql warn
run 42 --tables good_one
! retired && commented && scope_named good_one \
  && ok "a --tables WARN comment names its scope" || bad "scoped warn: $(cat "$CALLS") $out"

run "" --tables good_one
grep -q '^gh issue create ' "$CALLS" && scope_named good_one \
  && ok "an issue filed by a --tables run names its scope" || bad "scoped create: $(cat "$CALLS") $out"

run 42
commented && ! grep -q 'Scoped run' "$CALLS" \
  && ok "a whole-schema comment carries no scope note" || bad "full warn: $(cat "$CALLS") $out"

write_sql pass
run 42 --tables nothing_checked
[ $rc -eq 1 ] && ! retired && commented && scope_named nothing_checked \
  && ok "a scoped script error names its scope" || bad "scoped error: rc=$rc $(cat "$CALLS") $out"

run "" --tables nothing_checked
[ $rc -eq 1 ] && grep -q '^gh issue create ' "$CALLS" && scope_named nothing_checked \
  && ok "a script-error issue filed by a --tables run names its scope" || bad "scoped error create: rc=$rc $(cat "$CALLS") $out"

exit $fail
