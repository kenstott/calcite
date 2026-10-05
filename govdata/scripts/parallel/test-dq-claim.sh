#!/bin/bash
# Tests for the DQ-run claim: one worker-dq-run.sh per (DQ bucket, schema). Part 1 exercises the library with
# worker-dq-run.sh owners; part 2 runs the REAL worker-dq-run.sh against a schema that has no DQ script, so even
# a missing refusal would stop at "DQ script not found" before any rebuild or purge could start.
set -u
D="$(cd "$(dirname "$0")" && pwd)"; T=$(mktemp -d)
trap 'kill $(jobs -p) 2>/dev/null; rm -rf "$T" "$D/runs/worker-dq-zzclaimtest-daily"' EXIT
export GOVDATA_TMPDIR="$T/tmp"
source "$D/common.sh"; source "$D/schema-claim.sh"
set +e +u +o pipefail
CL="$T/claims"; PROD="s3://govdata-parquet-v1"; DQ="s3://govdata-parquet-v1-dq"
fail=0; ck() { if eval "$2"; then echo "ok   $1"; else echo "FAIL $1"; fail=1; fi; }
own() { bash -c "exec -a $1 sleep 100" >/dev/null 2>&1 & echo $!; }
DQ1=$(own worker-dq-run.sh); DQ2=$(own worker-dq-run.sh); FR=$(own force-reprocess.sh); sleep 0.3

echo "-- library"
claim_schema_years "$CL" $DQ1 "$DQ" ag 0 9999 nass_crop_production worker-dq-run.sh 2>/dev/null
claim_schema_years "$CL" $DQ2 "$DQ" ag 0 9999 nass_crop_production worker-dq-run.sh 2>"$T/err"; rc=$?
ck "a second DQ run of the same schema in the same DQ bucket is refused"       '[ $rc -ne 0 ]'
ck "...and the message names worker-dq-run.sh and the holder"                    'grep -q "worker-dq-run.sh pid $DQ1" "$T/err"'
claim_schema_years "$CL" $DQ2 "$DQ" ag 0 9999 other worker-dq-run.sh 2>/dev/null; rc=$?
ck "--start-year/--tables do not narrow it: the rebuild purges the whole schema" '[ $rc -ne 0 ]'
claim_schema_years "$CL" $FR "$PROD" ag 2010 2026 nass_land_values 2>/dev/null; rc=$?
ck "a PRODUCTION remediation of the same schema may overlap the DQ run"         '[ $rc -eq 0 ]'
release_schema_years_claim "$CL" $FR
claim_schema_years "$CL" $DQ2 "$DQ" housing 0 9999 hmda worker-dq-run.sh 2>/dev/null; rc=$?
ck "a DQ run of a different schema is independent"                              '[ $rc -eq 0 ]'
release_schema_years_claim "$CL" $DQ2
kill $DQ1 2>/dev/null; wait $DQ1 2>/dev/null; sleep 0.3
claim_schema_years "$CL" $DQ2 "$DQ" ag 0 9999 x worker-dq-run.sh 2>/dev/null; rc=$?
ck "a DQ claim whose owner died is ignored"                                     '[ $rc -eq 0 ]'
release_schema_years_claim "$CL" $DQ2
# a claim whose owner is alive but is not the command it was recorded for holds nothing
printf "c_pid=$FR\nc_bucket='$DQ'\nc_schema='ag'\nc_start=0\nc_end=9999\nc_tables='x'\nc_since=1\nc_pattern='worker-dq-run.sh'\n" > "$CL/$FR.claim"
claim_schema_years "$CL" $DQ2 "$DQ" ag 0 9999 y worker-dq-run.sh 2>/dev/null; rc=$?
ck "a live pid that is not worker-dq-run.sh (recycled) holds nothing"           '[ $rc -eq 0 ]'
release_schema_years_claim "$CL" $DQ2; rm -f "$CL/$FR.claim"

echo "-- the real worker-dq-run.sh (schema zzclaimtest has no DQ script)"
export GOVDATA_CLAIM_DIR="$T/rc"; mkdir -p "$GOVDATA_CLAIM_DIR"
HOLD=$(own worker-dq-run.sh); sleep 0.3
printf "c_pid=$HOLD\nc_bucket='$DQ'\nc_schema='zzclaimtest'\nc_start=0\nc_end=9999\nc_tables='x'\nc_since=$(date +%%s)\nc_pattern='worker-dq-run.sh'\n" > "$GOVDATA_CLAIM_DIR/$HOLD.claim"
out=$(timeout 120 bash "$D/worker-dq-run.sh" zzclaimtest --rebuild --start-year 2020 --tables t 2>&1); rc=$?
ck "a second DQ run of that schema exits 3"                                     '[ $rc -eq 3 ]'
ck "...naming the DQ run that holds it"                                         'echo "$out" | grep -q "worker-dq-run.sh pid $HOLD"'
ck "...before creating its run directory"                                       '[ ! -d "$D/runs/worker-dq-zzclaimtest-daily" ]'
ck "...and holding no claim of its own"                                         '[ "$(ls "$GOVDATA_CLAIM_DIR"/*.claim | wc -l)" -eq 1 ]'
kill $HOLD 2>/dev/null; wait $HOLD 2>/dev/null; rm -f "$GOVDATA_CLAIM_DIR"/*.claim

# The script creates runs/<worker> right after the claim. runs/ is a symlink into the temp disk, so from a shell
# started before that disk was mounted (a stale mount namespace) the mkdir fails for reasons unrelated to the
# claim; skip the "proceeds" half there rather than report a false failure.
if mkdir -p "$D/runs/.claim-test-probe" 2>/dev/null; then
  rmdir "$D/runs/.claim-test-probe"
  out=$(timeout 120 bash "$D/worker-dq-run.sh" zzclaimtest 2>&1); rc=$?
  ck "with no holder the claim is granted and it proceeds (stops safely: no DQ script)"  '[ $rc -eq 1 ] && echo "$out" | grep -q "DQ script not found"'
  ck "...and releases its claim when it exits"                                    '[ "$(ls "$GOVDATA_CLAIM_DIR"/*.claim 2>/dev/null | wc -l)" -eq 0 ]'
else
  echo "skip with no holder the claim is granted and released (runs/ is not writable from this shell's mount view)"
fi
HOLD=$(own worker-dq-run.sh); sleep 0.3
printf "c_pid=$HOLD\nc_bucket='$DQ'\nc_schema='zzclaimtest'\nc_start=0\nc_end=9999\nc_tables='x'\nc_since=$(date +%%s)\nc_pattern='worker-dq-run.sh'\n" > "$GOVDATA_CLAIM_DIR/$HOLD.claim"
out=$(timeout 120 bash "$D/worker-dq-run.sh" zzclaimtest --dry-run 2>&1); rc=$?
ck "--dry-run is not refused by a holder (it starts nothing)"                   '[ $rc -ne 3 ]'

echo "-- release wiring (the real _on_exit, extracted)"
awk '/^_on_exit\(\) \{/{p=1} p{print} p && /^}/{exit}' "$D/worker-dq-run.sh" > "$T/on_exit.sh"
ck "_on_exit was found in worker-dq-run.sh"                                      'grep -q "^_on_exit()" "$T/on_exit.sh"'
run_exit() { # held(true|false) -> prints whether the claim file survived
  ( source "$D/schema-claim.sh"; _file_script_error_issue() { :; }; _SCRIPT_COMPLETE=true; TMP_DIR=""
    source "$T/on_exit.sh"; CLAIM_DIR="$T/oe"; mkdir -p "$CLAIM_DIR"; : > "$CLAIM_DIR/$$.claim"; export _DQ_CLAIM_HELD=$1
    ( _on_exit ) ; [ -e "$CLAIM_DIR/$$.claim" ] && echo kept || echo released )
}
ck "_on_exit releases the claim when the run holds one"                           '[ "$(run_exit true)" = released ]'
ck "_on_exit leaves it alone when the run took none (dry run / refused)"          '[ "$(run_exit false)" = kept ]'
ck "the claim flag is set only after the claim is granted"                        '[ "$(grep -n "_DQ_CLAIM_HELD=true" "$D/worker-dq-run.sh" | head -1 | cut -d: -f1)" -gt "$(grep -n "claim_schema_years" "$D/worker-dq-run.sh" | head -1 | cut -d: -f1)" ]'
exit $fail
