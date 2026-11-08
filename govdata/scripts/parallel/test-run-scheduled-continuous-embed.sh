#!/usr/bin/env bash
# embed_continuous_decision: embedding runs back to back only in a historical window whose historical
# pool drained (exit 0), up to window_end minus the yield margin; every other case skips and says why.
# run_continuous_embedding_if_historical_done: calls vss-local.sh backlog --deadline only on "run" and
# logs the decision each time.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
fail=0
ok() { echo "PASS $1"; }
bad() { echo "FAIL $1"; fail=1; }
ts() { echo "T"; }
fmt_epoch() { echo "E$1"; }
log_error() { echo "ERROR: $*" >> "$tmp/errors"; }
mkdir -p "$tmp/scripts/parallel"
SCRIPT_DIR="$tmp/scripts/parallel"
eval "$(sed -n '/^embed_continuous_decision() {/,/^}/p' "$HERE/run-scheduled.sh")"
eval "$(sed -n '/^run_continuous_embedding_if_historical_done() {/,/^}/p' "$HERE/run-scheduled.sh")"

eval "$(sed -n '/^embed_daily_window_decision() {/,/^}/p' "$HERE/run-scheduled.sh")"
eval "$(sed -n '/^run_daily_window_embedding() {/,/^}/p' "$HERE/run-scheduled.sh")"
eval "$(sed -n '/^historical_is_complete() /p' "$HERE/run-scheduled.sh")"
eval "$(sed -n '/^record_historical_conclusion() {/,/^}/p' "$HERE/run-scheduled.sh")"
HISTORICAL_COMPLETE_FILE="$tmp/historical-complete"

d() { embed_continuous_decision "$@"; }
# args: mode current pool_exit now window_end margin
[ "$(d historical historical 0 1000 44200 600)" = "run 42600" ] && ok "historical done: runs to window_end - margin" || bad "run case: $(d historical historical 0 1000 44200 600)"
[ "$(d historical historical 0 1000 79200 600)" = "run 77600" ] && ok "weekend window: same rule, longer span" || bad "weekend"
d historical historical 2 1000 44200 600 | grep -q '^skip historical not complete' && ok "pool exit 2: historical work first" || bad "exit 2"
d historical historical 124 1000 44200 600 | grep -q '^skip historical not complete' && ok "window timeout: not complete" || bad "exit 124"
d daily daily 0 1000 44200 600 | grep -q '^skip not a historical window' && ok "daily window never embeds continuously" || bad "daily"
d daily historical 0 1000 44200 600 | grep -q '^skip not a historical window' && ok "historical fill inside a daily window does not embed" || bad "daily fill"
d historical historical 0 43700 44200 600 | grep -q '^skip the daily window starts' && ok "inside the margin: yields" || bad "margin"

printf '#!/usr/bin/env bash\necho "vss-local $*" >> "%s/calls"\n' "$tmp" > "$tmp/scripts/vss-local.sh"
: > "$tmp/calls"; : > "$tmp/w.log"
run_continuous_embedding_if_historical_done "$tmp/w.log" historical historical 0 $(( $(date +%s) + 7200 )) >/dev/null
grep -q '^vss-local backlog --deadline [0-9]*$' "$tmp/calls" && ok "launches backlog --deadline" || bad "call: $(cat "$tmp/calls")"
grep -q 'continuous embedding decision: run' "$tmp/w.log" && ok "decision logged" || bad "no decision log"
: > "$tmp/calls"; : > "$tmp/w.log"
run_continuous_embedding_if_historical_done "$tmp/w.log" daily daily 0 $(( $(date +%s) + 7200 )) >/dev/null
[ ! -s "$tmp/calls" ] && grep -q 'decision: skip' "$tmp/w.log" && ok "skip logs and launches nothing" || bad "skip case"

# daily-window decision: args historical_complete now window_end margin
[ "$(embed_daily_window_decision true 1000 8200 600)" = "run 6600" ] && ok "daily window, historical done: runs to end - margin" || bad "daily run"
embed_daily_window_decision false 1000 8200 600 | grep -q '^skip historical not complete' && ok "daily window, historical open: time-boxed step applies" || bad "daily open"
embed_daily_window_decision true 7700 8200 600 | grep -q '^skip the window ends' && ok "daily window inside margin: skips" || bad "daily margin"

# marker follows the latest historical conclusion
record_historical_conclusion "$tmp/w.log" 0
historical_is_complete && ok "exit 0 records historical complete" || bad "marker not written"
record_historical_conclusion "$tmp/w.log" 124
! historical_is_complete && ok "a later non-zero conclusion clears it" || bad "marker not cleared"

: > "$tmp/calls"; : > "$tmp/w.log"
run_daily_window_embedding "$tmp/w.log" $(( $(date +%s) + 7200 )) >/dev/null
[ ! -s "$tmp/calls" ] && grep -q 'daily window): skip historical not complete' "$tmp/w.log" && ok "daily window, marker absent: no continuous embed" || bad "daily absent"
record_historical_conclusion "$tmp/w.log" 0
: > "$tmp/calls"; : > "$tmp/w.log"
run_daily_window_embedding "$tmp/w.log" $(( $(date +%s) + 7200 )) >/dev/null
grep -q '^vss-local backlog --deadline [0-9]*$' "$tmp/calls" && grep -q 'daily window): run' "$tmp/w.log" && ok "daily window, marker present: deadline loop, decision logged" || bad "daily present"
exit $fail
