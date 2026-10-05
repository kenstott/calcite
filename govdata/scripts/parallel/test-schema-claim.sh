#!/bin/bash
# Tests for schema-claim.sh: one remediation run per (bucket, schema, years).
set -u
D="$(cd "$(dirname "$0")" && pwd)"
T=$(mktemp -d); trap 'kill $(jobs -p) 2>/dev/null; rm -rf "$T"' EXIT
export GOVDATA_TMPDIR="$T/tmp"          # common.sh creates its scratch dir at source time
source "$D/common.sh"; source "$D/schema-claim.sh"
set +e +u +o pipefail   # common.sh enables strict mode; the checks below inspect failures on purpose
CL="$T/claims"; PROD="s3://govdata-parquet-v1"; DQ="s3://govdata-parquet-v1-dq"
fail=0
ck() { if eval "$2"; then echo "ok   $1"; else echo "FAIL $1"; fail=1; fi; }
# a live "owner" that looks like force-reprocess.sh to the identity check
own() { bash -c 'exec -a force-reprocess.sh sleep 120' >/dev/null 2>&1 & echo $!; }   # detached, or $(own) would wait on its pipe
A=$(own); B=$(own); C=$(own); sleep 0.3

claim_schema_years "$CL" $A "$PROD" ag 2010 2026 nass_land_values 2>/dev/null
ck "first claim is granted"                                               '[ -e "$CL/$A.claim" ]'
claim_schema_years "$CL" $B "$PROD" ag 2010 2026 nass_land_values 2>"$T/err"; rc=$?
ck "same bucket+schema+years is refused"                                  '[ $rc -ne 0 ] && [ ! -e "$CL/$B.claim" ]'
ck "...and the message names the owner and its tables"                    'grep -q "pid $A" "$T/err" && grep -q "nass_land_values" "$T/err"'
claim_schema_years "$CL" $B "$PROD" ag 2015 2016 other_table 2>/dev/null; rc=$?
ck "an overlapping sub-range of the same schema is refused (other table too)"  '[ $rc -ne 0 ]'
claim_schema_years "$CL" $B "$DQ" ag 2010 2026 nass_land_values 2>/dev/null; rc=$?
ck "a DQ-bucket run of the same schema+years does NOT conflict with prod"      '[ $rc -eq 0 ]'
release_schema_years_claim "$CL" $B
claim_schema_years "$CL" $B "$PROD" housing 2019 2025 hmda_lar 2>/dev/null; rc=$?
ck "a different schema is independent"                                    '[ $rc -eq 0 ]'
release_schema_years_claim "$CL" $B

claim_schema_years "$CL" $B "$PROD" sec_primary 2015 2015 financial_line_items 2>/dev/null
claim_schema_years "$CL" $C "$PROD" sec_primary 2018 2022 financial_line_items 2>/dev/null; rc=$?
ck "same schema, disjoint years, is allowed"                              '[ $rc -eq 0 ]'
release_schema_years_claim "$CL" $C
claim_schema_years "$CL" $C "$PROD" sec_secondary 2015 2015 x 2>/dev/null; rc=$?
ck "sec_primary and sec_secondary may run together (as the worker guard allows)"  '[ $rc -eq 0 ]'
release_schema_years_claim "$CL" $C
claim_schema_years "$CL" $C "$PROD" sec 2015 2015 x 2>/dev/null; rc=$?
ck "a bare sec run conflicts with a sec_primary run of the same years"    '[ $rc -ne 0 ]'
release_schema_years_claim "$CL" $B
claim_schema_years "$CL" $C "$PROD" ag 0 9999 daily_all 2>/dev/null; rc=$?
ck "a daily run (all years) conflicts with any claim in the schema"        '[ $rc -ne 0 ]'

# stale claims never hold a schema
kill $A 2>/dev/null; wait $A 2>/dev/null; sleep 0.2
claim_schema_years "$CL" $C "$PROD" ag 2010 2026 nass_land_values 2>/dev/null; rc=$?
ck "a claim whose owner died is ignored and cleaned up"                    '[ $rc -eq 0 ] && [ ! -e "$CL/$A.claim" ]'
release_schema_years_claim "$CL" $C
sleep 120 & R=$!; printf "c_pid=$R\nc_bucket='$PROD'\nc_schema='ag'\nc_start=2010\nc_end=2026\nc_tables='x'\nc_since=1\n" > "$CL/$R.claim"
claim_schema_years "$CL" $C "$PROD" ag 2010 2026 y 2>/dev/null; rc=$?
ck "a live pid that is not a force-reprocess.sh (recycled pid) holds nothing"  '[ $rc -eq 0 ]'
release_schema_years_claim "$CL" $C; kill $R 2>/dev/null

# the check and the write are atomic: of N simultaneous launches exactly one wins
rm -rf "$CL"; owners=(); for i in 1 2 3 4 5 6 7 8; do owners+=("$(own)"); done; sleep 0.3
for o in "${owners[@]}"; do ( claim_schema_years "$CL" "$o" "$PROD" econ 2020 2024 t 2>/dev/null && echo won >> "$T/winners" ) & done; sleep 3
ck "8 simultaneous launches of the same schema+years: exactly one is granted"  '[ "$(wc -l < "$T/winners" 2>/dev/null || echo 0)" -eq 1 ]'
exit $fail
