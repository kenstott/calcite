#!/bin/bash
# Regression test: a pool terminated while it holds the embeddings-block budget claim must give the
# claim back. Uses the REAL cleanup() and write_budget_file() from run-pool.sh (extracted, run with
# stubs), so it fails if either regresses.   Usage: test-pool-budget-restore.sh [run-pool.sh path]
set -u
SRC="${1:-$(cd "$(dirname "$0")" && pwd)/run-pool.sh}"
fail=0; T=$(mktemp -d); trap 'rm -rf "$T"' EXIT

extract() { awk -v fn="$1" '$0 ~ "^"fn"\\(\\) \\{" {p=1} p {print} p && /^}/ {exit}' "$SRC"; }
{ echo 'set -euo pipefail'
  echo '_cleanup_log() { :; }; _kill_worker_session() { :; }'
  echo 'active_pids=(); active_labels=(); active_exit_files=()'
  extract write_budget_file; extract cleanup; } > "$T/fns.sh"
grep -q "^cleanup()" "$T/fns.sh" && grep -q "^write_budget_file()" "$T/fns.sh" || { echo "FAIL: could not extract the functions from $SRC"; exit 1; }

# run cleanup() with the given claim state; echoes the budget file afterwards
run_cleanup() { # claim_flag_value (or "unset")
  printf 'RESERVE_MB=54237\nMAX_WORKERS=0\n' > "$T/pool-budget.conf"
  ( source "$T/fns.sh"
    BUDGET_FILE="$T/pool-budget.conf"; OS_RESERVE_MB=54237; MAX_WORKERS=0
    _prior_reserve=1500; _prior_workers=9
    if [ "$1" != unset ]; then _budget_claim_active=$1; else _budget_claim_active=false; fi
    cleanup ) >/dev/null 2>&1
  echo $?
}
check() { # name expected_exit expected_file_contents actual_exit
  local got; got=$(tr '\n' ' ' < "$T/pool-budget.conf")
  if [ "$4" = "$2" ] && [ "$got" = "$3" ]; then echo "ok   $1"; else echo "FAIL $1: exit=$4 file='$got' (wanted exit=$2 file='$3')"; fail=1; fi
}

rc=$(run_cleanup true);  check "terminated while holding the claim restores the prior budget" 130 "RESERVE_MB=1500 MAX_WORKERS=9 " "$rc"
rc=$(run_cleanup false); check "terminated without a claim leaves the budget file alone"      130 "RESERVE_MB=54237 MAX_WORKERS=0 " "$rc"

# the claim flag must be set where the claim is written and cleared where it is given back
claim=$(grep -n '^  _budget_claim_active=true' "$SRC" | head -1 | cut -d: -f1)
drop=$(grep -n '^  _budget_claim_active=false' "$SRC" | head -1 | cut -d: -f1)
write=$(grep -n 'x-schema/vss: reserving' "$SRC" | head -1 | cut -d: -f1)
if [ -n "$claim" ] && [ -n "$drop" ] && [ "$claim" -lt "$write" ] && [ "$drop" -gt "$write" ]; then echo "ok   claim flag is set with the claim and cleared when it is restored"
else echo "FAIL claim flag placement (true@${claim:-none} reserving@${write:-none} false@${drop:-none})"; fail=1; fi
exit $fail
