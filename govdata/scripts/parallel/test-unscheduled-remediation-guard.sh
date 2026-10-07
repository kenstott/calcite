#!/usr/bin/env bash
# A worker outside a scheduled production slot (UNSCHEDULED_SLOTS: today sec_secondary) is never
# remediated against production: the helper refuses a production target for it, allows DQ targets, other
# schemas and the explicit override, and both entry points (run-pool.sh, force-reprocess.sh) use it.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fail=0
ok() { echo "PASS $1"; }
bad() { echo "FAIL $1"; fail=1; }

guard() { # schema parquet-dir override [unscheduled-list] -> exit code of the helper
  ( source "$HERE/common.sh" >/dev/null 2>&1
    [ -n "${4:-}" ] && UNSCHEDULED_SLOTS="$4"
    [ -n "$2" ] && export GOVDATA_PARQUET_DIR="$2" || unset GOVDATA_PARQUET_DIR
    [ -n "$3" ] && export GOVDATA_ALLOW_UNSCHEDULED_REMEDIATION="$3" || unset GOVDATA_ALLOW_UNSCHEDULED_REMEDIATION
    refuse_unscheduled_remediation "$1" "test" 2>/dev/null )
}
( source "$HERE/common.sh" >/dev/null 2>&1; [ "$UNSCHEDULED_SLOTS" = "sec_secondary" ] ) \
  && ok "today the only unscheduled slot is sec_secondary" || bad "unscheduled list is not exactly sec_secondary"

guard sec_secondary "" "";                              [ $? -eq 1 ] && ok "sec_secondary on the default (production) target is refused" || bad "default target not refused"
guard sec_secondary "s3://govdata-parquet-v1" "";       [ $? -eq 1 ] && ok "sec_secondary on the production bucket is refused" || bad "production bucket not refused"
guard sec_secondary "s3://govdata-parquet-v1-dq" "";    [ $? -eq 0 ] && ok "sec_secondary on DQ is allowed" || bad "DQ refused"
guard sec_secondary "s3://govdata-parquet-v1-dq/" "";   [ $? -eq 0 ] && ok "DQ with a trailing slash is allowed" || bad "DQ (slash) refused"
guard sec_secondary "s3://govdata-parquet-v1" "true";   [ $? -eq 0 ] && ok "the override lifts the rule" || bad "override ignored"
guard sec_secondary "s3://govdata-parquet-v1" "false";  [ $? -eq 1 ] && ok "override=false still refuses" || bad "override=false not refused"
for s in sec sec_primary sec_13f sec_prices econ fec; do
  guard "$s" "s3://govdata-parquet-v1" ""; [ $? -eq 0 ] && ok "scheduled schema $s is not affected" || bad "$s wrongly refused"
done
guard econ "s3://govdata-parquet-v1" "" "econ"; [ $? -eq 1 ] && ok "any schema added to the list is refused" || bad "list is not general"
guard sec_secondary "s3://govdata-parquet-v1" "" "";  # empty override keeps the default list
[ $? -eq 1 ] && ok "an emptied override keeps the default list" || bad "default list lost"

msg="$( ( source "$HERE/common.sh" >/dev/null 2>&1; unset GOVDATA_PARQUET_DIR; refuse_unscheduled_remediation sec_secondary "force-reprocess.sh x" ) 2>&1 )"
[[ "$msg" == *REFUSED* && "$msg" == *"reduced accession set in DQ"* && "$msg" == *"not in a scheduled production slot"* ]] \
  && ok "the refusal says why" || bad "refusal text: $msg"

out="$(unset GOVDATA_PARQUET_DIR; timeout 60 bash "$HERE/force-reprocess.sh" --schema sec_secondary --tables insider_transactions --start 2024 --end 2024 --dry-run 2>&1)"; rc=$?
[ $rc -eq 2 ] && [[ "$out" == *REFUSED* ]] && ok "force-reprocess.sh refuses sec_secondary on production" || bad "force-reprocess.sh rc=$rc: $(echo "$out" | tail -2)"

grep -q 'refuse_unscheduled_remediation' "$HERE/force-reprocess.sh" && ok "force-reprocess.sh uses the guard" || bad "force-reprocess.sh has no guard"
grep -q 'refuse_unscheduled_remediation' "$HERE/run-pool.sh" && ok "run-pool.sh uses the guard" || bad "run-pool.sh has no guard"
exit $fail
