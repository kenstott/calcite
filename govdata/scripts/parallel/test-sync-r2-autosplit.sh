#!/bin/bash
# Behavioural test for the R2 sync's automatic splitting, against a stub rclone (no network, throwaway $HOME).
# Schema "bigone" is NOT in the configured split list. Its whole-tree copy hangs, then (once split) its "stuck"
# directory hangs, then (once that is split) only its child "stuck/b" hangs; with STUB_FAST=1 everything is quick.
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
    if   [[ "$last" =~ ^[a-z0-9_]+:[A-Za-z0-9._-]+$ ]];              then printf 'bigone/\nfastone/\n'
    elif [[ "$last" =~ ^[a-z0-9_]+:[A-Za-z0-9._-]+/bigone$ ]];       then printf 'ok1/\nok2/\nstuck/\n'
    elif [[ "$last" =~ ^[a-z0-9_]+:[A-Za-z0-9._-]+/bigone/stuck$ ]]; then printf 'a/\nb/\n'
    fi
    exit 0 ;;
  copy)
    echo "$*" >> "$STUB_LOG"
    [ -n "${STUB_FAST:-}" ] && exit 0
    # hangs: the whole schema tree, the whole stuck/ directory, and stuck/b. Root-file copies (--max-depth) are quick.
    if [[ "$*" != *"--max-depth"* ]]; then
      case "$2" in */bigone|*/bigone/stuck|*/bigone/stuck/b) exec sleep 100 ;; esac
    fi
    exit 0 ;;
  *) exit 0 ;;
esac
STUB
chmod +x "$T/bin/rclone"
export STUB_LOG="$T/copies.log" HOME="$T/home" PATH="$T/bin:$PATH"
export GOVDATA_R2_SYNC_SLICE_MAX=6s GOVDATA_R2_SYNC_UNIT_MAX=2s GOVDATA_R2_SYNC_PRIORITY_SCHEMAS=bigone
S="$HOME/.r2-sync-state"
pass() { timeout 200 bash "$SCRIPT" --schemas bigone,fastone 2>&1; }
copies() { grep -c -E "$1" "$STUB_LOG" 2>/dev/null || true; }
fail=0; ck() { if eval "$2"; then echo "ok   $1"; else echo "FAIL $1"; fail=1; fi; }

s=$(date +%s); pass > "$T/p1.log"; took=$(( $(date +%s) - s ))
ck "pass 1: the whole-tree copy was stopped at the cap (took ${took}s, stub sleeps 100s)"  '[ "$took" -lt 40 ]'
ck "pass 1: the schema was switched to per-directory copying (marker + message)"          '[ -e "$S/.split-bigone" ] && grep -q "\[bigone\] switched to per-directory copying" "$T/p1.log"'
ck "pass 1: an unrelated schema was not split"                                             '[ ! -e "$S/.split-fastone" ]'

pass > "$T/p2.log"
ck "pass 2: it now copies per directory (ok1, ok2 each copied)"                            '[ "$(copies "/bigone/ok1 ")" -ge 1 ] && [ "$(copies "/bigone/ok2 ")" -ge 1 ]'
ck "pass 2: the directory that outran its cap is marked for a second-level split"           '[ -e "$S/.deep-bigone-stuck" ] && grep -q "\[bigone/stuck\] hit the unit cap" "$T/p2.log"'
ck "pass 2: the schema sentinel is held"                                                    '[ ! -s "$S/bigone" ]'

pass > "$T/p3.log"
ck "pass 3: stuck/ is now copied by subdirectory (stuck/a and stuck/b each tried)"          '[ "$(copies "/bigone/stuck/a ")" -ge 1 ] && [ "$(copies "/bigone/stuck/b ")" -ge 1 ]'
ck "pass 3: the whole stuck/ directory is not attempted again (one whole-directory copy in total)"  '[ "$(grep -v -- "--max-depth" "$STUB_LOG" | grep -c -E "/bigone/stuck r2:")" -eq 1 ]'
ck "pass 3: stuck/a finished (state file with a slash-safe name)"                           '[ -s "$S/.unit-cursor-bigone-stuck__a" ]'
ck "pass 3: stuck/b did not finish and was NOT split a third level"                         '[ ! -s "$S/.unit-cursor-bigone-stuck__b" ] && [ ! -e "$S/.deep-bigone-stuck__b" ]'
ck "pass 3: the files directly in stuck/ were copied (--max-depth 1)"                       'grep -q -E "/bigone/stuck r2:.*--max-depth 1" "$STUB_LOG"'

STUB_FAST=1 pass > "$T/p4.log"
ck "pass 4 (everything quick): the schema sentinel advances and it reports synced through"  '[ -s "$S/bigone" ] && grep -q "\[bigone\] synced through" "$T/p4.log"'
ck "pass 4: the slow marker is cleared by the clean slice"                                  '[ ! -e "$S/.slow-bigone" ]'
[ $fail -eq 0 ] || { echo "---- pass 2 log"; cut -c1-190 "$T/p2.log" | tail -12; }
exit $fail
