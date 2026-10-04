#!/bin/bash
# Behavioural test for the R2 sync's per-slice cap: runs the real sync-to-r2.sh against a stub rclone
# (no network, no MinIO, throwaway $HOME) with one schema whose copy hangs and one that is instant.
set -u
SCRIPT="${1:-$(cd "$(dirname "$0")" && pwd)/sync-to-r2.sh}"   # optional: path of a script to test (must sit beside common.sh)
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
mkdir -p "$T/bin" "$T/home"
cat > "$T/bin/rclone" <<'STUB'
#!/bin/bash
# stub: bucket root lists two schemas; oldest-file listing gives a fixed time; copy hangs for slowone
case "$1" in
  lsf)
    if [[ " $* " == *" -R "* ]]; then echo "2026-10-01 00:00:00"; exit 0; fi
    last="${@: -1}"
    if [[ "$last" =~ ^[a-z0-9_]+:[A-Za-z0-9._-]+$ ]]; then printf 'slowone/\nfastone/\n'; fi
    exit 0 ;;
  copy)
    echo "$*" >> "$STUB_LOG"
    if [[ "$2" == */slowone ]]; then exec sleep 30; fi
    exit 0 ;;
  *) exit 0 ;;
esac
STUB
chmod +x "$T/bin/rclone"
export STUB_LOG="$T/copies.log" HOME="$T/home" PATH="$T/bin:$PATH"
export GOVDATA_R2_SYNC_SLICE_MAX=2s GOVDATA_R2_SYNC_PRIORITY_SCHEMAS=slowone

pass() { timeout 120 bash "$SCRIPT" --schemas slowone,fastone 2>&1; }
fail=0
ck() { if eval "$2"; then echo "ok   $1"; else echo "FAIL $1"; fail=1; fi; }

s=$(date +%s); P1=$(pass); took=$(( $(date +%s) - s ))
echo "$P1" > "$T/pass1.log"
ck "pass 1 did not hang on the stuck schema (took ${took}s, cap 2s, stub sleeps 30s)"  '[ "$took" -lt 25 ]'
ck "the stuck schema's slice was stopped at the cap and said so"        'grep -q "\[slowone\] slice exceeded 2s" "$T/pass1.log"'
ck "its sentinel was held (not written)"                                 '[ ! -s "$HOME/.r2-sync-state/slowone" ]'
ck "it is marked slow"                                                   '[ -e "$HOME/.r2-sync-state/.slow-slowone" ]'
ck "the next schema still ran after it"                                  'grep -q "\[fastone\] synced through" "$T/pass1.log"'
ck "the fast schema's sentinel advanced"                                 '[ -s "$HOME/.r2-sync-state/fastone" ]'

pass > "$T/pass2.log"
first=$(grep -o -E "\[(slowone|fastone)\] slice 20" "$T/pass2.log" | head -1)
ck "pass 2 walks the slow schema last (first schema visited: ${first:-none})" '[[ "$first" == "[fastone]"* ]]'
ck "pass 2 announces the slow schema"                                    'grep -q "walked last (slow): slowone" "$T/pass2.log"'
ck "its copy is still retried in pass 2 (not dropped)"                   '[ "$(grep -c "/slowone" "$STUB_LOG")" -ge 2 ]'
[ $fail -eq 0 ] || { echo "---- pass 1 log"; cat "$T/pass1.log" | cut -c1-200 | tail -15; }
exit $fail
