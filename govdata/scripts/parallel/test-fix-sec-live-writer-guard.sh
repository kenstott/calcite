#!/usr/bin/env bash
# fix-sec.sh's live-writer guard applies to the production bucket only: a live worker-sec* pid
# blocks a production target and does not block a DQ target.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fail=0
ok() { echo "PASS $1"; }
bad() { echo "FAIL $1"; fail=1; }

tmp=$(mktemp -d); trap 'kill "$sleeper" 2>/dev/null; rm -rf "$tmp"' EXIT
sleep 300 & sleeper=$!
echo "$sleeper" > "$tmp/worker-sec_secondary.pid"

live() { ( source "$HERE/common.sh" >/dev/null 2>&1; live_sec_workers "$1" ); }
tgt()  { ( source "$HERE/common.sh" >/dev/null 2>&1
           [ -n "$1" ] && export GOVDATA_PARQUET_DIR="$1" || unset GOVDATA_PARQUET_DIR
           target_is_dq_bucket ); }

[[ "$(live "$tmp")" == *worker-sec_secondary* ]] && ok "a live worker is detected" || bad "live worker not detected"
touch "$tmp/worker-sec_secondary.exit"
[ -z "$(live "$tmp")" ] && ok "a worker with an exit record is not live" || bad "exited worker counted live"
rm "$tmp/worker-sec_secondary.exit"

tgt "";                                  [ $? -ne 0 ] && ok "default target is production" || bad "default treated as DQ"
tgt "s3://govdata-parquet-v1";           [ $? -ne 0 ] && ok "production bucket is not DQ" || bad "prod treated as DQ"
tgt "s3://govdata-parquet-v1-dq";        [ $? -eq 0 ] && ok "DQ bucket is DQ" || bad "DQ not recognised"
tgt "s3://govdata-parquet-v1-dq/";       [ $? -eq 0 ] && ok "DQ bucket with trailing slash is DQ" || bad "DQ slash not recognised"

# The guard in fix-sec.sh must branch on the target and keep the production refusal.
grep -q 'if target_is_dq_bucket' "$HERE/fix-sec.sh" && ok "fix-sec.sh branches on the target bucket" || bad "fix-sec.sh does not use target_is_dq_bucket"
grep -q 'REFUSING to reprocess — live SEC worker' "$HERE/fix-sec.sh" && ok "production refusal kept" || bad "production refusal removed"
grep -q 'live_sec_workers' "$HERE/fix-sec.sh" && ok "fix-sec.sh uses live_sec_workers" || bad "fix-sec.sh does not use live_sec_workers"

exit $fail
