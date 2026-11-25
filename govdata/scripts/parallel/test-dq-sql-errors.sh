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
# A DQ statement that errors must fail the schema verdict and name its table, while every other
# table keeps its results. Runs worker-dq-run.sh --dry-run in a temp tree with the real duckdb,
# a missing local table standing in for a missing Iceberg table, and outward-facing commands stubbed.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fail=0
ok() { echo "PASS $1"; }
bad() { echo "FAIL $1"; fail=1; }

DUCKDB="$(command -v duckdb || echo "$HOME/.local/bin/duckdb")"
[ -x "$DUCKDB" ] || { echo "FAIL duckdb CLI not found"; exit 1; }

T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
S="$T/govdata/scripts"; P="$S/parallel"
mkdir -p "$P" "$T/bin"
cp "$HERE"/../*.sh "$S/"
cp "$HERE"/*.sh "$HERE"/*.py "$P/"
# the runner's S3 secret needs the DuckDB extensions installed under the real home
printf '#!/bin/sh\nHOME="%s" exec "%s" "$@"\n' "$HOME" "$DUCKDB" > "$T/bin/duckdb"; chmod +x "$T/bin/duckdb"
for c in aws rclone psql gh curl; do
  printf '#!/bin/sh\nexit 0\n' > "$T/bin/$c"; chmod +x "$T/bin/$c"
done

# One statement per table; 'absent_table' errors the way a table with no Iceberg metadata does.
write_sql() { # $1 = file, remaining = table names
  local f="$1"; shift
  {
    echo "CREATE TEMP TABLE dq_results (schema VARCHAR, tbl VARCHAR, test VARCHAR, status VARCHAR, value VARCHAR, threshold VARCHAR, detail VARCHAR);"
    for t in "$@"; do
      if [ "$t" = "absent_table" ]; then
        echo "-- iceberg_scan('s3://\${GOVDATA_DQ_BUCKET}/zzerr/$t', allow_moved_paths=true)"
        echo "INSERT INTO dq_results SELECT 'zzerr', '$t', 'row_count', 'pass', '1', '1', NULL FROM \"zzerr/$t\";"
      else
        echo "INSERT INTO dq_results SELECT 'zzerr', '$t', 'row_count', 'pass', '1', '1', NULL;"
      fi
    done
  } > "$f"
}
run() { # args passed to the runner; output in $out, rc in $rc
  out=$(env -i HOME="$T" PATH="$T/bin:/usr/bin:/bin" GOVDATA_TMPDIR="$T/tmp" \
    GOVDATA_DQ_BUCKET=govdata-parquet-v1-dq GOVDATA_DQ_RCLONE_REMOTE=zzremote AWS_ENDPOINT_OVERRIDE="http://127.0.0.1:1" \
    AWS_ACCESS_KEY_ID=testkeyid AWS_SECRET_ACCESS_KEY=testsecretvalue GOVDATA_CACHE_DIR="s3://zz-raw" \
    timeout 60 bash "$P/worker-dq-run.sh" zzerr --dry-run "$@" 2>&1); rc=$?
}

write_sql "$S/zzerr_dq.sql" good_one absent_table good_two
run
[ $rc -eq 1 ] && [[ "$out" == *"SCHEMA RESULT: FAIL (fails=1 warns=0)"* ]] \
  && ok "an errored statement fails the verdict" || bad "verdict: rc=$rc $out"
echo "$out" | grep -E 'absent_table.*sql_error' >/dev/null \
  && ok "the failing result names the table" || bad "no sql_error row for absent_table: $out"
echo "$out" | grep -q 'testsecretvalue' && bad "a credential reached the output" || ok "no credential in the output"

write_sql "$S/zzerr_dq.sql" good_one good_two
run
[ $rc -eq 0 ] && [[ "$out" == *"SCHEMA RESULT: PASS"* ]] \
  && ok "a script with no errors still passes" || bad "clean run: rc=$rc $out"

# --tables: statements for unbuilt tables, directly or through a view, are left out
B='s3://${GOVDATA_DQ_BUCKET}/zzerr'
scan() { echo "iceberg_scan('$B/$1', allow_moved_paths=true)"; }
write_scoped_sql() { # $1 = file
  local ins="INSERT INTO dq_results SELECT 'zzerr'"
  cat > "$1" <<SQL
CREATE TEMP TABLE dq_results (schema VARCHAR, tbl VARCHAR, test VARCHAR, status VARCHAR, value VARCHAR, threshold VARCHAR, detail VARCHAR);
-- a comment; with a semicolon and an apostrophe's quote
$ins, 'good_one', 'row_count', 'pass', '1', '1', '$B/good_one';
$ins, 'out_one', 'row_count', 'pass', '1', '1', NULL FROM $(scan out_one);
CREATE TEMP VIEW out_view AS SELECT * FROM $(scan out_two);
$ins, 'derived', 'row_count', 'pass', '1', '1', NULL FROM out_view;
$ins, 'good_one', 'orphans', 'pass', '1', '1', '$B/good_one' FROM out_view;
$ins, t, 'existence', 'fail', '0', '1', NULL FROM (VALUES ('out_one')) v(t);
SQL
}
write_scoped_sql "$S/zzerr_dq.sql"
run --tables good_one
[ $rc -eq 0 ] && [[ "$out" == *"SCHEMA RESULT: PASS"* ]] \
  && ok "a scoped run ignores tables outside --tables" || bad "scoped: rc=$rc $out"
[[ "$out" == *"a check reading good_one needs out_two, outside --tables"* ]] \
  && ok "a check left out for an in-scope table is listed" || bad "not-run list: $out"

# a cross-table statement that errors on the out-of-scope side is listed, not counted
write_scoped_sql "$S/zzerr_dq.sql"
# (a missing local table stands in for the unbuilt one, so nothing goes to the network)
echo "INSERT INTO dq_results SELECT 'zzerr', 'good_one', 'join', 'pass', '1', '1', '$B/good_one $B/out_one' FROM \"zzerr/out_one\";" >> "$S/zzerr_dq.sql"
run --tables good_one
[ $rc -eq 0 ] && [[ "$out" == *"need out_one, outside --tables"* ]] \
  && ok "an out-of-scope statement error is not counted" || bad "mixed: rc=$rc $out"

write_scoped_sql "$S/zzerr_dq.sql"
echo 'INSERT INTO dq_results SELECT * FROM no_such_table;' >> "$S/zzerr_dq.sql"
run --tables good_one
[ $rc -eq 1 ] && [[ "$out" == *"SCHEMA RESULT: FAIL (fails=1 warns=0)"* ]] \
  && ok "an unattributed error still fails a scoped run" || bad "unresolved: rc=$rc $out"

write_scoped_sql "$S/zzerr_dq.sql"
run --tables nothing_checked
[ $rc -eq 1 ] && [[ "$out" == *"no check reading any of the --tables"* ]] \
  && ok "a scope with no checks fails" || bad "empty scope: rc=$rc $out"

# a long path is cut short in DuckDB's snippet; the prefix still identifies the table
printf "INSERT INTO t SELECT 1 FROM iceberg_scan('s3://b/zzerr/district_assessments', allow_moved_paths=true);\n" > "$T/known.sql"
printf "IO Error: no metadata\n\nLINE 3: ... iceberg_scan('s3://b/zzerr/district_asses...\n" > "$T/cut.log"
csv=$(python3 "$HERE/dq_sql_errors.py" "$T/cut.log" "$T/known.sql" 1)
[[ "$csv" == *"district_assessments,1,IO Error: no metadata"* ]] \
  && ok "a truncated path resolves to its table" || bad "truncated path: $csv"

printf "no error text here\n" > "$T/none.log"
csv=$(python3 "$HERE/dq_sql_errors.py" "$T/none.log" "$T/known.sql" 137)
[[ "$csv" == *"(unresolved),1,"*"code 137"* ]] \
  && ok "a non-zero exit with no error line is still a failure" || bad "silent exit: $csv"

exit $fail
