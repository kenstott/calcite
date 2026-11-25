#!/usr/bin/env bash
# worker-dq-run.sh --dry-run changes nothing: no S3, rclone, tracker, ETL or GitHub call under
# --rebuild or --etl-resume, and only the read-only DQ queries otherwise. Runs a copy of the scripts
# in a temp tree (no credentials files) with every external command stubbed to record its call.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fail=0
ok() { echo "PASS $1"; }
bad() { echo "FAIL $1"; fail=1; }

T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
S="$T/govdata/scripts"; P="$S/parallel"; CALLS="$T/calls"
mkdir -p "$P" "$T/bin" "$T/claims"
cp "$HERE"/../*.sh "$S/"
cp "$HERE"/*.sh "$HERE"/*.py "$P/"
echo "SELECT 1;" > "$S/zzdry_dq.sql"
printf '#!/bin/sh\necho "worker.sh $*" >> "%s"\n' "$CALLS" > "$P/worker.sh"
for c in aws rclone psql gh curl python3 duckdb; do
  printf '#!/bin/sh\necho "%s $*" >> "%s"\n' "$c" "$CALLS" > "$T/bin/$c"
done
chmod +x "$T/bin/"*

TRACKER=govdata-tracker-v1-dq
run() { # bucket args... -> output in $out, rc in $rc, external calls in $CALLS; $TRACKER names the results bucket
  local bucket="$1"; shift
  : > "$CALLS"
  out=$(env -i HOME="$T" PATH="$T/bin:/usr/bin:/bin" GOVDATA_TMPDIR="$T/tmp" GOVDATA_CLAIM_DIR="$T/claims" \
    ${bucket:+GOVDATA_DQ_BUCKET="$bucket"} ${TRACKER:+GOVDATA_DQ_TRACKER_BUCKET="$TRACKER"} \
    GOVDATA_DQ_RCLONE_REMOTE=zzremote AWS_ENDPOINT_OVERRIDE="http://127.0.0.1:1" \
    AWS_ACCESS_KEY_ID=x AWS_SECRET_ACCESS_KEY=y GOVDATA_CACHE_DIR="s3://zz-raw" \
    timeout 60 bash "$P/worker-dq-run.sh" zzdry "$@" 2>&1); rc=$?
}

untouched() { [ ! -s "$CALLS" ] && [ ! -e "$P/runs" ] && [ -z "$(ls "$T/claims")" ]; }

# an unnamed bucket stops every kind of run before anything happens
for flags in "" "--rebuild" "--etl-resume" "--dry-run" "--rebuild --dry-run"; do
  # shellcheck disable=SC2086
  run "" $flags
  [ $rc -eq 1 ] && [[ "$out" == *"GOVDATA_DQ_BUCKET is not set"* ]] && untouched \
    && ok "no data bucket, '$flags': refused" || bad "no data bucket '$flags': rc=$rc $(cat "$CALLS") $out"
  # shellcheck disable=SC2086
  TRACKER="" run govdata-parquet-v1-dq $flags
  [ $rc -eq 1 ] && [[ "$out" == *"GOVDATA_DQ_TRACKER_BUCKET is not set"* ]] && untouched \
    && ok "no tracker bucket, '$flags': refused" || bad "no tracker bucket '$flags': rc=$rc $(cat "$CALLS") $out"
done

# a run that writes refuses any bucket that is not a -dq bucket
for flags in "--rebuild" "--etl-resume" "--rebuild --dry-run"; do
  # shellcheck disable=SC2086
  run govdata-parquet-v1 $flags
  [ $rc -eq 1 ] && [[ "$out" == *"must end in -dq"* ]] && untouched \
    && ok "production data bucket, '$flags': refused" || bad "prod data '$flags': rc=$rc $(cat "$CALLS") $out"
  # shellcheck disable=SC2086
  TRACKER=govdata-tracker-v1 run govdata-parquet-v1-dq $flags
  [ $rc -eq 1 ] && [[ "$out" == *"must end in -dq"* ]] && untouched \
    && ok "non-dq tracker bucket, '$flags': refused" || bad "prod tracker '$flags': rc=$rc $(cat "$CALLS") $out"
done

for bucket in govdata-parquet-v1-dq; do
  for flags in "--rebuild" "--etl-resume" "--rebuild --etl-resume"; do
    # shellcheck disable=SC2086
    run "$bucket" $flags --start-year 2020 --dry-run
    [ $rc -eq 0 ] && [[ "$out" == *"nothing changed"* ]] \
      && ok "$bucket $flags: dry run exits 0" || bad "$bucket $flags: rc=$rc $out"
    [ ! -s "$CALLS" ] \
      && ok "$bucket $flags: no external command runs" || bad "$bucket $flags ran: $(cat "$CALLS")"
    [ ! -e "$P/runs/pids" ] && [ -z "$(ls "$T/claims")" ] \
      && ok "$bucket $flags: no claim or writer guard" || bad "$bucket $flags left a claim or guard"
  done
done

run govdata-parquet-v1-dq --rebuild --dry-run
[[ "$out" == *"would delete every object under s3://govdata-parquet-v1-dq/zzdry/"* ]] \
  && [[ "$out" == *"would delete the Postgres tracker rows of schema=zzdry"* ]] \
  && ok "the teardown plan names what would be deleted" || bad "plan: $out"

# plain --dry-run: DQ queries only; the stub writes no results file, which is the script-error path
run govdata-parquet-v1-dq --dry-run
grep -q '^duckdb' "$CALLS" && ok "plain dry run runs the DQ queries" || bad "no DQ run: $out"
if grep -qE '^(aws|gh|psql|worker\.sh|python3|curl) |^rclone (purge|delete|copy|mkdir)' "$CALLS"; then
  bad "plain dry run ran: $(cat "$CALLS")"
else
  ok "a failing plain dry run uploads nothing and files no issue"
fi
[ $rc -eq 1 ] && [[ "$out" == *"not filing an issue"* ]] \
  && ok "a failing dry run still exits 1" || bad "failing dry run: rc=$rc $out"

# control: the stubs do observe a real rebuild's teardown
run govdata-parquet-v1-dq --rebuild
grep -q '^python3 .*s3_purge_prefix.py' "$CALLS" || grep -q '^aws .*s3 mb' "$CALLS" \
  && ok "control: a real --rebuild reaches the teardown" || bad "control saw no teardown: $(cat "$CALLS") $out"

exit $fail
