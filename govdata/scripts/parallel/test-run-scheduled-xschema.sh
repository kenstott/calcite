#!/usr/bin/env bash
# run_x_schema_if_needed (end of a daily window): when the pool's own trigger did not run the sweep it
# runs x-schema.sh and then the embeddings backlog (vss-local.sh), whether or not the sweep failed;
# when the sweep already ran this window it runs neither.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
fail=0
ok() { echo "PASS $1"; }
bad() { echo "FAIL $1"; fail=1; }

mkdir -p "$tmp/scripts/parallel"
fake() { # name exit-code
  printf '#!/usr/bin/env bash\necho "%s $*" >> "%s/calls"\nexit %s\n' "$1" "$tmp" "$2" > "$tmp/scripts/$1.sh"
}
ts() { echo "T"; }
log_error() { echo "ERROR: $*" >> "$tmp/errors"; }
SCRIPT_DIR="$tmp/scripts/parallel"
eval "$(sed -n '/^run_x_schema_if_needed() {/,/^}/p' "$HERE/run-scheduled.sh")"

: > "$tmp/calls"; fake x-schema 0; fake vss-local 0; : > "$tmp/w.log"
run_x_schema_if_needed "$tmp/w.log" >/dev/null
[ "$(tr '\n' ',' < "$tmp/calls")" = "x-schema ,vss-local backlog," ] \
  && ok "sweep then embeddings when the sweep has not run" || bad "calls: $(tr '\n' ',' < "$tmp/calls")"

: > "$tmp/calls"; fake x-schema 1; : > "$tmp/errors"; : > "$tmp/w.log"
run_x_schema_if_needed "$tmp/w.log" >/dev/null
[ "$(tr '\n' ',' < "$tmp/calls")" = "x-schema ,vss-local backlog," ] \
  && ok "embeddings still run when the sweep fails" || bad "after a failed sweep: $(tr '\n' ',' < "$tmp/calls")"
grep -q "x-schema.sh run" "$tmp/errors" && ok "the sweep failure is reported" || bad "no error line for the failed sweep"

: > "$tmp/calls"; fake x-schema 0; echo "x-schema: sweeping" > "$tmp/w.log"
run_x_schema_if_needed "$tmp/w.log" >/dev/null
[ ! -s "$tmp/calls" ] && ok "neither runs when the sweep already ran this window" || bad "ran again: $(cat "$tmp/calls")"
exit $fail
