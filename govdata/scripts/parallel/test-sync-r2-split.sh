#!/bin/bash
# Behavioural test for the R2 sync's per-unit split of large schemas: runs the real sync-to-r2.sh against a stub
# rclone (no network, throwaway $HOME). Schema "bigone" is split into units ok1/, ok2/, stuck/ and the root files;
# stuck/ hangs until STUB_FAST=1. fastone is an ordinary, unsplit schema.
set -u
SCRIPT="${1:-$(cd "$(dirname "$0")" && pwd)/sync-to-r2.sh}"
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
mkdir -p "$T/bin" "$T/home"
cat > "$T/bin/rclone" <<'STUB'
#!/bin/bash
case "$1" in
  lsf)
    if [[ " $* " == *" -R "* ]]; then echo "2026-10-01 00:00:00"; exit 0; fi
    last="${@: -1}"
    if   [[ "$last" =~ ^[a-z0-9_]+:[A-Za-z0-9._-]+$ ]];        then printf 'bigone/\nfastone/\n'
    elif [[ "$last" =~ ^[a-z0-9_]+:[A-Za-z0-9._-]+/bigone$ ]]; then printf 'ok1/\nok2/\nstuck/\n'
    fi
    exit 0 ;;
  copy)
    echo "$2" >> "$STUB_LOG"
    if [[ "$2" == */bigone/stuck && -z "${STUB_FAST:-}" ]]; then exec sleep 30; fi
    exit 0 ;;
  *) exit 0 ;;
esac
STUB
chmod +x "$T/bin/rclone"
export STUB_LOG="$T/copies.log" HOME="$T/home" PATH="$T/bin:$PATH"
export GOVDATA_R2_SYNC_SPLIT_SCHEMAS=bigone GOVDATA_R2_SYNC_UNIT_MAX=2s GOVDATA_R2_SYNC_PRIORITY_SCHEMAS=bigone
S="$HOME/.r2-sync-state"
pass() { timeout 150 bash "$SCRIPT" --schemas bigone,fastone 2>&1; }
count() { grep -c -E "$1" "$STUB_LOG" 2>/dev/null || true; }
fail=0
ck() { if eval "$2"; then echo "ok   $1"; else echo "FAIL $1"; fail=1; fi; }

s=$(date +%s); pass > "$T/p1.log"; took=$(( $(date +%s) - s ))
ck "pass 1: the stuck unit was stopped at its cap (took ${took}s, cap 2s, stub sleeps 30s)"  '[ "$took" -lt 25 ]'
ck "pass 1: the good units and the root files finished (cursors written)"   '[ -s "$S/.unit-cursor-bigone-ok1" ] && [ -s "$S/.unit-cursor-bigone-ok2" ] && [ -s "$S/.unit-cursor-bigone-_root" ]'
ck "pass 1: the stuck unit's cursor was NOT advanced"                       '[ ! -s "$S/.unit-cursor-bigone-stuck" ]'
ck "pass 1: it said which unit failed and that the others continue"          'grep -q "\[bigone/stuck\] unit failed" "$T/p1.log"'
ck "pass 1: the schema sentinel is held (a unit is still short)"             '[ ! -s "$S/bigone" ]'
ck "pass 1: the unit summary counts 3 copied, 1 failed"                      'grep -q "unit pass: 3 copied, 1 failed" "$T/p1.log"'
ck "pass 1: an unsplit schema after it still synced"                          'grep -q "\[fastone\] synced through" "$T/p1.log"'

pass > "$T/p2.log"
ck "pass 2: finished units are not copied again (ok1 copied $(count '/bigone/ok1$') time)"   '[ "$(count "/bigone/ok1$")" -eq 1 ]'
ck "pass 2: the stuck unit IS retried (copied $(count '/bigone/stuck$') times)"                '[ "$(count "/bigone/stuck$")" -eq 2 ]'
ck "pass 2: the schema sentinel is still held"                                                 '[ ! -s "$S/bigone" ]'

STUB_FAST=1 pass > "$T/p3.log"
ck "pass 3 (stuck unit now fast): every unit reaches the slice and the schema sentinel advances"  '[ -s "$S/bigone" ]'
ck "pass 3: the schema reports it is synced through now"                                           'grep -q "\[bigone\] synced through" "$T/p3.log"'
ck "pass 3: no unit was left behind"                                                                '! grep -q "unit failed" "$T/p3.log"'
[ $fail -eq 0 ] || { echo "---- pass 1 log"; cut -c1-190 "$T/p1.log" | tail -14; }
exit $fail
