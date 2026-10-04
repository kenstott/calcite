#!/bin/bash
# Tests for cutover-tmp-disk.sh --release-stale-claim (no root, no devices needed).
set -u
S="$(cd "$(dirname "$0")" && pwd)/cutover-tmp-disk.sh"; T=$(mktemp -d); fail=0
trap 'kill $(jobs -p) 2>/dev/null; rm -rf "$T"' EXIT
F="$T/pool-budget.conf"
t() { # name expect(removed|kept) setup-content
  printf "$3" > "$F"
  bash "$S" --release-stale-claim --budget-file "$F" >/dev/null 2>&1
  if [ -e "$F" ]; then got=kept; else got=removed; fi
  if [ "$got" = "$2" ]; then echo "ok   $1"; else echo "FAIL $1: $got (wanted $2)"; fail=1; fi
}
t "a whole-box claim nothing holds is removed"        removed 'RESERVE_MB=54237\nMAX_WORKERS=0\n'
t "a normal budget is left alone"                      kept    'RESERVE_MB=1500\nMAX_WORKERS=9\n'
# a live x-schema.sh (launched as `bash .../x-schema.sh`) means the claim is real
printf 'sleep 60\n' > "$T/x-schema.sh"; bash "$T/x-schema.sh" & sleep 0.5
t "a claim held by a live x-schema.sh is left alone"   kept    'RESERVE_MB=54237\nMAX_WORKERS=0\n'
kill %1 2>/dev/null; wait 2>/dev/null
rm -f "$F"; bash "$S" --release-stale-claim --budget-file "$F" >/dev/null 2>&1 && echo "ok   a missing budget file is not an error" || { echo "FAIL missing file errored"; fail=1; }
exit $fail
