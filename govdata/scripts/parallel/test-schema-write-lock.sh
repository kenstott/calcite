#!/usr/bin/env bash
# Schema write lock (#1064): a live remediation holder blocks worker starts for its schema (exit 0, named in
# the message); a stale holder does not; the holder's own workers pass; a DQ target is exempt.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fail=0
ok() { echo "PASS $1"; }
bad() { echo "FAIL $1"; fail=1; }

tmp=$(mktemp -d); mkdir -p "$tmp/pids"
bash -c 'exec -a fix-sec.sh sleep 300' & holder=$!
trap 'kill "$holder" 2>/dev/null; rm -rf "$tmp"' EXIT
sleep 0.3
src() { ( source "$HERE/common.sh" >/dev/null 2>&1; "$@" ); }
lockdir="$tmp/locks"

src acquire_schema_lock "$lockdir" "$holder" fix-sec.sh sec_secondary sec_primary && ok "acquire" || bad "acquire"
src acquire_schema_lock "$lockdir" "$$" force-reprocess.sh sec_secondary 2>/dev/null && bad "second acquire allowed" || ok "second acquire refused"

out=$(src check_schema_year_conflict "$tmp/pids" sec_secondary 2018 2025 2>&1); rc=$?
[ $rc -ne 0 ] && [[ "$out" == *write-locked* ]] && ok "pool admission refused, names holder" || bad "pool admission not refused: $out"
src check_schema_year_conflict "$tmp/pids" sec_13f 2018 2025 >/dev/null 2>&1 && ok "unlocked schema admitted" || bad "unlocked schema refused"
( export GOVDATA_SCHEMA_LOCK_OWNER=$holder; source "$HERE/common.sh" >/dev/null 2>&1; check_schema_year_conflict "$tmp/pids" sec_secondary 2018 2025 ) \
  && ok "holder's own workers pass" || bad "holder's workers refused"
( export GOVDATA_PARQUET_DIR=s3://govdata-parquet-v1-dq; source "$HERE/common.sh" >/dev/null 2>&1; check_schema_year_conflict "$tmp/pids" sec_secondary 2018 2025 ) \
  && ok "DQ target exempt" || bad "DQ target refused"

# worker.sh start: refused with exit 0 and a message
out=$(GOVDATA_LOCK_DIR="$lockdir" bash "$HERE/worker.sh" sec_secondary 2018 2>&1); rc=$?
[ $rc -eq 0 ] && [[ "$out" == *"NOT STARTING"*"write-locked"* ]] && ok "worker.sh refuses, exit 0" || bad "worker.sh rc=$rc: $out"

src release_schema_lock "$lockdir" "$holder" sec_secondary sec_primary
[ ! -e "$lockdir/sec_secondary.lock" ] && ok "release" || bad "release"

# stale: dead pid
src acquire_schema_lock "$lockdir" "$holder" fix-sec.sh sec_secondary
kill "$holder"; wait "$holder" 2>/dev/null; sleep 0.2
src check_schema_year_conflict "$tmp/pids" sec_secondary 2018 2025 >/dev/null 2>&1 && ok "stale lock ignored" || bad "stale lock blocks"
[ ! -e "$lockdir/sec_secondary.lock" ] && ok "stale lock removed" || bad "stale lock kept"
exit $fail
