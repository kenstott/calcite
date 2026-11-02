#!/usr/bin/env bash
# GOVDATA_DQ=true must never resolve to the production bucket: load_env stops when .env.dq is
# missing or leaves GOVDATA_DQ_BUCKET empty, and the ETL launchers stop when GOVDATA_PARQUET_DIR is
# not a -dq bucket. Every case runs with a stub java, so a launch that gets past the guard is
# visible and no S3 call is made.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fail=0
ok() { echo "PASS $1"; }
bad() { echo "FAIL $1"; fail=1; }

T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
mkdir -p "$T/root"; : > "$T/root/.env.prod"
printf '#!/bin/sh\ntouch "%s/java-called"\n' "$T" > "$T/java"; chmod +x "$T/java"
: > "$T/stub.jar"; export GOVDATA_JAR="$T/stub.jar"

run() { # dq-env-content|NONE parquet-dir -> prints output, rc in $rc
  local envdq="$1" pdir="$2"
  rm -f "$T/root/.env.dq" "$T/java-called"
  [ "$envdq" = NONE ] || printf '%s\n' "$envdq" > "$T/root/.env.dq"
  out="$( (
    source "$HERE/common.sh" >/dev/null 2>&1
    GOVDATA_ROOT="$T/root"; GOVDATA_JAVA_BIN="$T/java"; export GOVDATA_DQ=true
    [ -n "$pdir" ] && export GOVDATA_PARQUET_DIR="$pdir" || unset GOVDATA_PARQUET_DIR
    load_env || exit 3
    assert_dq_target || exit 4
  ) 2>&1 )"; rc=$?
}

run NONE "s3://govdata-parquet-v1-dq"
[ $rc -ne 0 ] && [[ "$out" == *GOVDATA_DQ_BUCKET* && "$out" == *"does not exist"* ]] && [ ! -e "$T/java-called" ] \
  && ok "missing .env.dq stops the run and names the bucket" || bad "missing .env.dq: rc=$rc $out"

run "GOVDATA_DQ_BUCKET=" "s3://govdata-parquet-v1-dq"
[ $rc -ne 0 ] && [[ "$out" == *"GOVDATA_DQ_BUCKET is empty"* ]] && [ ! -e "$T/java-called" ] \
  && ok "empty GOVDATA_DQ_BUCKET stops the run" || bad "empty bucket: rc=$rc $out"

run "GOVDATA_DQ_BUCKET=govdata-parquet-v1-dq" "s3://govdata-parquet-v1"
[ $rc -ne 0 ] && [[ "$out" == *"not a -dq bucket"* ]] && [ ! -e "$T/java-called" ] \
  && ok "production parquet dir under GOVDATA_DQ=true stops the run" || bad "prod dir: rc=$rc $out"

run "GOVDATA_DQ_BUCKET=govdata-parquet-v1-dq" ""
[ $rc -ne 0 ] && [[ "$out" == *"not a -dq bucket"* ]] && ok "unset parquet dir stops the run" || bad "unset dir: rc=$rc $out"

run "GOVDATA_DQ_BUCKET=govdata-parquet-v1-dq" "s3://govdata-parquet-v1-dq"
[ $rc -eq 0 ] && ok "a -dq target passes the guard" || bad "dq target refused: rc=$rc $out"

( source "$HERE/common.sh" >/dev/null 2>&1; unset GOVDATA_DQ; GOVDATA_PARQUET_DIR=s3://govdata-parquet-v1; assert_dq_target ) \
  && ok "non-DQ runs are unaffected" || bad "non-DQ run refused"

for fn in run_etl run_etl_inline; do
  awk -v f="$fn" '$0 ~ "^"f"\\(\\)" {on=1} on && /assert_dq_target/ {found=1} on && /GOVDATA_JAVA_BIN" \\/ {exit} END {exit !found}' "$HERE/common.sh" \
    && ok "$fn checks the DQ target before the JVM starts" || bad "$fn does not check the DQ target before the JVM"
done

exit $fail
