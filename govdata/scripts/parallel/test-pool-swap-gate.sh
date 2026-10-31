#!/bin/bash
# Unit test for the run-pool.sh swap gate. Uses the REAL swap_gate_reason() and
# get_swap_rate_pages_sec() from run-pool.sh (extracted) with fake /proc/vmstat values.
# Usage: test-pool-swap-gate.sh [run-pool.sh path]
set -u
SRC="${1:-$(cd "$(dirname "$0")" && pwd)/run-pool.sh}"
fail=0; T=$(mktemp -d); trap 'rm -rf "$T"' EXIT

extract() { awk -v fn="$1" '$0 ~ "^"fn"\\(\\) \\{" {p=1} p {print} p && /^}/ {exit}' "$SRC"; }
{ echo 'OS_RESERVE_MB=1500; SWAP_PRESSURE_PCT=70; SWAP_HARD_PCT=90; SWAP_ACTIVE_PAGES_SEC=200'
  echo '_swap_prev_pages=""; _swap_prev_ts=0; _swap_rate=0'
  extract swap_gate_reason; extract get_swap_rate_pages_sec; } > "$T/fns.sh"
grep -q "^swap_gate_reason()" "$T/fns.sh" && grep -q "^get_swap_rate_pages_sec()" "$T/fns.sh" \
  || { echo "FAIL: could not extract the functions from $SRC"; exit 1; }
source "$T/fns.sh"

check() { # name expect(hold|admit) pct rate avail need
  local out; out=$(swap_gate_reason "$3" "$4" "$5" "$6")
  local got=admit; [ -n "$out" ] && got=hold
  if [ "$got" = "$2" ]; then echo "ok   $1"; else echo "FAIL $1: got $got ('$out'), wanted $2"; fail=1; fi
}

# the 2026-10-31 incident: 72% swap used, no swapping, ~40GB available, census needs 4096MB
check "stale swap, idle, free RAM admits"          admit 72 0     40000 4096
check "stale swap at 95%, idle, free RAM admits"   admit 95 0     40000 4096
check "heavy swap in/out holds"                    hold  30 5000  40000 4096
check "heavy swap holds at the threshold rate"     hold  10 200   40000 4096
check "light swap activity below threshold admits" admit 40 199   40000 4096
check "swap nearly full and swapping holds"        hold  92 5     40000 4096
check "swap nearly full, idle, free RAM admits"    admit 92 0     40000 4096
check "stale swap but low MemAvailable holds"      hold  72 0     4500  4096
check "low swap, low MemAvailable is left to the memory check" admit 10 0 4500 4096

# rate from /proc/vmstat deltas, called exactly as fill_pool does (no command substitution)
export VMSTAT_FILE="$T/vmstat"
rate_check() { # name expect now
  get_swap_rate_pages_sec "$3"
  if [ "$_swap_rate" = "$2" ]; then echo "ok   $1"; else echo "FAIL $1: got $_swap_rate, wanted $2"; fail=1; fi
}
printf 'pswpin 100\npswpout 200\n' > "$VMSTAT_FILE"
rate_check "first sample reports 0" 0 1000
printf 'pswpin 1100\npswpout 1200\n' > "$VMSTAT_FILE"
rate_check "2000 pages over 10s is 200 pages/s" 200 1010
rate_check "same-second re-sample keeps the rate" 200 1010
rate_check "no change is 0 pages/s" 0 1020
printf 'pswpin 6020\npswpout 1200\n' > "$VMSTAT_FILE"
rate_check "second consecutive sample sees the delta" 492 1030

# fill_pool must read the rate without a command substitution
if grep -q 'swap_rate=\$(get_swap_rate_pages_sec' "$SRC"; then
  echo "FAIL fill_pool samples the rate in a subshell"; fail=1
else echo "ok   fill_pool samples the rate in the parent shell"; fi

# the gate must release the admission lock when it holds
if awk '/swap_reason=\$\(swap_gate_reason/ {p=1} p && /flock -u 9/ {ok=1} p && /break/ {exit} END {exit !ok}' "$SRC"; then
  echo "ok   swap hold releases the admission lock before break"
else echo "FAIL swap hold leaks the admission lock"; fail=1; fi
exit $fail
