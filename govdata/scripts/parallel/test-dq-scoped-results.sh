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
# A --tables run leaves the schema's whole-run DQ results intact: it uploads and persists under its
# own scope, and its --rebuild does not purge dq-results. Runs a copy of the scripts in a temp tree
# with the real duckdb; stubbed aws and rclone act on a directory standing in for the object store.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fail=0
ok() { echo "PASS $1"; }
bad() { echo "FAIL $1"; fail=1; }

DUCKDB="$(command -v duckdb || echo "$HOME/.local/bin/duckdb")"
[ -x "$DUCKDB" ] || { echo "FAIL duckdb CLI not found"; exit 1; }

T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
S="$T/govdata/scripts"; P="$S/parallel"; STORE="$T/store"
mkdir -p "$P" "$T/bin" "$T/claims" "$STORE"
cp "$HERE"/../*.sh "$S/"
cp "$HERE"/*.sh "$HERE"/*.py "$P/"
printf '#!/bin/sh\nexit 0\n' > "$P/worker.sh"
# the Postgres tracker purge is not under test
printf '\nschema_tables() { :; }\n' >> "$P/common.sh"
printf '\npg_tracker_purge_schema() { :; }\n' >> "$S/tracker_pg.sh"
# the MinIO data purge is not under test
printf 'import sys\n' > "$P/s3_purge_prefix.py"
# the runner's S3 secret needs the DuckDB extensions installed under the real home
printf '#!/bin/sh\nHOME="%s" exec "%s" "$@"\n' "$HOME" "$DUCKDB" > "$T/bin/duckdb"
for c in psql curl gh; do printf '#!/bin/sh\nexit 0\n' > "$T/bin/$c"; done
# aws [--endpoint-url u] s3 cp <local> s3://<bucket>/<key>
cat > "$T/bin/aws" <<AWS
#!/bin/sh
[ "\$1" = "--endpoint-url" ] && shift 2
[ "\$1 \$2" = "s3 cp" ] || exit 0
dest="$STORE/\${4#s3://}"
mkdir -p "\$(dirname "\$dest")" && cp "\$3" "\$dest"
AWS
# rclone purge <remote>:<path> removes that directory of the stand-in store; the rest succeed
cat > "$T/bin/rclone" <<RC
#!/bin/sh
if [ "\$1" = "purge" ]; then rm -rf "$STORE/\${2#*:}"; fi
exit 0
RC
chmod +x "$T/bin/"*

cat > "$S/zzscope_dq.sql" <<'SQL'
CREATE TEMP TABLE dq_results (schema VARCHAR, tbl VARCHAR, test VARCHAR, status VARCHAR, value VARCHAR, threshold VARCHAR, detail VARCHAR);
INSERT INTO dq_results SELECT 'zzscope', 'one', 'row_count', 'pass', '1', '1', 's3://${GOVDATA_DQ_BUCKET}/zzscope/one';
INSERT INTO dq_results SELECT 'zzscope', 'two', 'row_count', 'pass', '1', '1', 's3://${GOVDATA_DQ_BUCKET}/zzscope/two';
SQL
run() { # args... -> output in $out, rc in $rc
  out=$(env -i HOME="$T" PATH="$T/bin:/usr/bin:/bin" GOVDATA_TMPDIR="$T/tmp" GOVDATA_CLAIM_DIR="$T/claims" \
    GOVDATA_DQ_BUCKET=govdata-parquet-v1-dq GOVDATA_DQ_RCLONE_REMOTE=zzremote \
    AWS_ENDPOINT_OVERRIDE="http://127.0.0.1:1" AWS_ACCESS_KEY_ID=x AWS_SECRET_ACCESS_KEY=y \
    GOVDATA_CACHE_DIR="s3://zz-raw" timeout 60 bash "$P/worker-dq-run.sh" zzscope "$@" 2>&1); rc=$?
}
today="$(date +%Y-%m-%d)"
base="$STORE/govdata-tracker-v1-dq/dq-results/schema=zzscope/run_date=$today/type=daily"
local_dir="$P/runs/worker-dq-zzscope-daily"
tables_in() { "$T/bin/duckdb" -csv -noheader -c "SELECT string_agg(table_name, '+' ORDER BY table_name) FROM read_parquet('$1')"; }
scoped_files() { find "$base" -path '*/scope=*/results.parquet' | wc -l; }

run
[ $rc -eq 0 ] && [ "$(tables_in "$base/results.parquet")" = "one+two" ] \
  && ok "a whole-schema run uploads to type=<mode>/results.parquet" || bad "full: rc=$rc $out"

run --tables one
[ $rc -eq 0 ] && [ "$(tables_in "$base/results.parquet")" = "one+two" ] \
  && ok "a --tables run leaves the whole-schema results intact" || bad "scoped: rc=$rc $out"
[ "$(scoped_files)" -eq 1 ] && [ "$(tables_in "$(find "$base" -path '*/scope=*/results.parquet')")" = "one" ] \
  && ok "...and uploads its own results under scope=<id>" || bad "scoped upload: $(find "$STORE" -type f)"
[ "$(tables_in "$local_dir/results_$today.parquet")" = "one+two" ] \
  && [ "$(find "$local_dir" -name 'results_*_scope-*.parquet' | wc -l)" -eq 1 ] \
  && ok "the local copies are kept apart too" || bad "local: $(ls "$local_dir")"

run --tables two
[ "$(scoped_files)" -eq 2 ] && ok "another table set gets its own scope" || bad "two: $(find "$base" -type f)"
run --tables one,two
run --tables two,one
[ "$(scoped_files)" -eq 3 ] && ok "table order does not change the scope" || bad "order: $(find "$base" -type f)"

run --rebuild --tables one
[ $rc -eq 0 ] && [ "$(tables_in "$base/results.parquet")" = "one+two" ] && [ "$(scoped_files)" -eq 3 ] \
  && ok "a --rebuild --tables run keeps the earlier results" || bad "scoped rebuild: rc=$rc $(find "$STORE" -type f) $out"

run --rebuild
[ $rc -eq 0 ] && [ "$(tables_in "$base/results.parquet")" = "one+two" ] && [ "$(scoped_files)" -eq 0 ] \
  && ok "a whole-schema --rebuild still clears the schema's results" || bad "full rebuild: rc=$rc $(find "$STORE" -type f) $out"

exit $fail
