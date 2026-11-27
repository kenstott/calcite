#!/usr/bin/env bash
# worker-dq-run.sh removes its writer guard and claim on every exit, and stops when
# AWS_ENDPOINT_OVERRIDE is unset. dq-report.sh needs the endpoint and GOVDATA_DQ_RCLONE_REMOTE and
# joins its filters with AND. Runs a copy of the scripts in a temp tree (no credentials files) with
# every external command stubbed to record its call.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fail=0
ok() { echo "PASS $1"; }
bad() { echo "FAIL $1"; fail=1; }

T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
S="$T/govdata/scripts"; P="$S/parallel"; CALLS="$T/calls"; PIDS="$P/runs/pids"; SQL="$T/sql"
mkdir -p "$P" "$T/bin" "$T/claims"
cp "$HERE"/../*.sh "$S/"
cp "$HERE"/*.sh "$HERE"/*.py "$P/"
echo "SELECT 1;" > "$S/zzgrd_dq.sql"
for c in aws psql gh curl python3; do
  printf '#!/bin/sh\necho "%s $*" >> "%s"\n' "$c" "$CALLS" > "$T/bin/$c"
done
# duckdb records its SQL and the writer-guard files present while it runs; it writes no results
printf '#!/bin/sh\necho "duckdb $*" >> "%s"\nls "%s" >> "%s" 2>/dev/null\ncat >> "%s"\n' \
  "$CALLS" "$PIDS" "$T/during" "$SQL" > "$T/bin/duckdb"
# rclone copy leaves one result file, so the report reaches its queries
cat > "$T/bin/rclone" <<STUB
#!/bin/sh
echo "rclone \$*" >> "$CALLS"
if [ "\$1" = copy ]; then mkdir -p "\$3/d" && : > "\$3/d/results.parquet"; fi
STUB
chmod +x "$T/bin/"*

run() { # script extra-env... -- args... -> output in $out, rc in $rc, external calls in $CALLS
  local script="$1" envs=(); shift
  while [ "$1" != "--" ]; do envs+=("$1"); shift; done
  shift
  : > "$CALLS"; : > "$SQL"; : > "$T/during"
  out=$(env -i HOME="$T" PATH="$T/bin:/usr/bin:/bin" GOVDATA_TMPDIR="$T/tmp" GOVDATA_CLAIM_DIR="$T/claims" \
    AWS_ACCESS_KEY_ID=x AWS_SECRET_ACCESS_KEY=y GOVDATA_CACHE_DIR="s3://zz-raw" "${envs[@]}" \
    timeout 60 bash "$P/$script" "$@" 2>&1 </dev/null); rc=$?
}
EP="AWS_ENDPOINT_OVERRIDE=http://127.0.0.1:1"
RM="GOVDATA_DQ_RCLONE_REMOTE=zzremote"
PROD="GOVDATA_DQ_BUCKET=govdata-parquet-v1"
GUARD=worker-zzgrd-historical
left() { ls "$PIDS" "$T/claims" 2>/dev/null | grep -E "^${GUARD}\.|\.claim|^[0-9]+" ; }

# a production-bucket run that fails at the DQ step
mkdir -p "$PIDS"; echo 0 > "$PIDS/$GUARD.exit"
run worker-dq-run.sh "$PROD" "$EP" "$RM" -- zzgrd
grep -qx "$GUARD.pid" "$T/during" && grep -qx "$GUARD.foreground" "$T/during" \
  && ok "a production-bucket run holds its writer guard while it works" || bad "during: $(cat "$T/during") $out"
grep -qx "$GUARD.exit" "$T/during" \
  && bad "an earlier .exit marker hides the guard" || ok "an earlier .exit marker is cleared"
[ $rc -ne 0 ] && [ -z "$(ls "$PIDS")" ] && [ -z "$(ls "$T/claims")" ] \
  && ok "a failed run leaves no guard file and no claim" || bad "rc=$rc left: $(ls "$PIDS" "$T/claims")"

# an exit before the error handler is installed
run worker-dq-run.sh "$PROD" "$EP" "$RM" -- zznosql
[ $rc -eq 1 ] && [[ "$out" == *"DQ script not found"* ]] && [ -z "$(ls "$PIDS")" ] && [ -z "$(ls "$T/claims")" ] \
  && ok "an early exit leaves no guard file and no claim" || bad "rc=$rc left: $(ls "$PIDS" "$T/claims") $out"

for flags in "" "--rebuild" "--etl-resume" "--rebuild --dry-run"; do
  # shellcheck disable=SC2086
  run worker-dq-run.sh GOVDATA_DQ_BUCKET=govdata-parquet-v1-dq "$RM" -- zzgrd $flags
  [ $rc -eq 1 ] && [[ "$out" == *"AWS_ENDPOINT_OVERRIDE is not set"* ]] && [ ! -s "$CALLS" ] \
    && [ -z "$(ls "$T/claims")" ] \
    && ok "unset endpoint (flags='$flags') stops the run before any external command" || bad "rc=$rc $(cat "$CALLS") $out"
done

run dq-report.sh "$EP" GOVDATA_RCLONE_REMOTE=zzprod --
[ $rc -eq 1 ] && [[ "$out" == *"GOVDATA_DQ_RCLONE_REMOTE is not set"* ]] && [ ! -s "$CALLS" ] \
  && ok "the report does not use another remote" || bad "report: rc=$rc $(cat "$CALLS") $out"

run dq-report.sh "$RM" --
[ $rc -eq 1 ] && [[ "$out" == *"AWS_ENDPOINT_OVERRIDE is not set"* ]] && [ ! -s "$CALLS" ] \
  && ok "the report needs the endpoint" || bad "report: rc=$rc $out"

printf '%s\n%s\n' "$RM" "$EP" > "$T/govdata/.env.dq"
run dq-report.sh GOVDATA_RCLONE_REMOTE=zzprod -- --mode daily --schemas a,b
[ $rc -eq 0 ] && grep -q '^rclone copy zzremote:govdata-tracker-v1-dq/dq-results/' "$CALLS" \
  && ok "the report reads the remote named in .env.dq" || bad "report: rc=$rc $(cat "$CALLS") $out"
grep -qF "WHERE run_type = 'daily' AND schema_name IN ('a','b')" "$SQL" \
  && ok "--mode and --schemas are joined with AND" || bad "sql: $(grep WHERE "$SQL")"

exit $fail
