#!/usr/bin/env bash
# wait_for_sec_workers (#1073): returns once the live worker exits, times out naming a worker that never
# exits, and never signals a worker. fix-sec.sh takes the lock before it waits.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fail=0
ok() { echo "PASS $1"; }
bad() { echo "FAIL $1"; fail=1; }

tmp=$(mktemp -d); trap 'kill "$a" "$b" 2>/dev/null; rm -rf "$tmp"' EXIT
wait_fn() { ( source "$HERE/common.sh" >/dev/null 2>&1; wait_for_sec_workers "$@" ); }

sleep 3 & a=$!
echo "$a" > "$tmp/worker-sec_secondary-2020.pid"
out=$(wait_fn "$tmp" 30 1 2>&1); rc=$?
[ $rc -eq 0 ] && [[ "$out" == *"worker-sec_secondary-2020"* ]] && ok "waits for a worker that exits, names it" || bad "exit case rc=$rc: $out"

sleep 300 & b=$!
echo "$b" > "$tmp/worker-sec_primary-2019.pid"
out=$(wait_fn "$tmp" 2 1 2>&1); rc=$?
[ $rc -eq 1 ] && [[ "$out" == *"still alive"*"worker-sec_primary-2019(pid $b)"* ]] && ok "timeout names the live worker, exits 1" || bad "timeout case rc=$rc: $out"
kill -0 "$b" 2>/dev/null && ok "worker was not signalled" || bad "worker was killed"

lk=$(grep -n 'acquire_schema_lock' "$HERE/fix-sec.sh" | head -1 | cut -d: -f1)
wt=$(grep -n 'wait_for_sec_workers' "$HERE/fix-sec.sh" | head -1 | cut -d: -f1)
[ -n "$lk" ] && [ -n "$wt" ] && [ "$lk" -lt "$wt" ] && ok "fix-sec.sh locks before it waits" || bad "order lock=$lk wait=$wt"

exit $fail
