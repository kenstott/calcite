#!/usr/bin/env bash
# The ChunkOrganizer step of x-schema.sh gets a 2h limit by default (the end-of-window call from
# run-scheduled.sh sets none), honours an explicit value, and passes 0 through as "no limit".
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fail=0
ok() { echo "PASS $1"; }
bad() { echo "FAIL $1"; fail=1; }

# The limit argument the script hands run_step for the chunk step, taken from the script itself.
expr="$(grep -A2 'run_step "sweeping every registered source' "$HERE/x-schema.sh" \
  | grep -o '"\${GOVDATA_XSCHEMA_CHUNK_TIMEOUT:-[^}]*}"' | head -1)"
[ -n "$expr" ] && ok "chunk step passes a limit expression" || bad "no limit expression found"

run() { # env assignment (or empty) -> the `timeout` argument actually used
  (
    unset GOVDATA_XSCHEMA_CHUNK_TIMEOUT
    [ -n "$1" ] && export GOVDATA_XSCHEMA_CHUNK_TIMEOUT="$1"
    timeout() { echo "$1"; }
    JAR=x; GOVDATA_JAVA_BIN=java
    eval "$(sed -n '/^run_step() {/,/^}/p' "$HERE/x-schema.sh")"
    limit="$(eval echo "$expr")"
    run_step "step" "$limit" some.Main | grep -v '^\[x-schema\]'
  )
}
[ "$(run '')" = "2h" ] && ok "default is 2h" || bad "default is not 2h: $(run '')"
[ "$(run 30m)" = "30m" ] && ok "explicit value wins" || bad "explicit value ignored"
[ "$(run 0)" = "0" ] && ok "0 is passed through (timeout 0 = no limit)" || bad "0 not passed through"
exit $fail
