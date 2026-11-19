#!/usr/bin/env bash
# The sync lock must be held only by the pass, never by its rclone descendants: a pass killed
# with TERM leaves nothing holding the lock, a held lock is reported as SKIPPED (exit 75), and a
# second pass can then take it. No network: stub rclone whose copy spawns a long-lived grandchild.
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
T=$(mktemp -d)
cleanup() { pkill -f "$T/bin" 2>/dev/null; rm -rf "$T"; }
trap cleanup EXIT
fail() { echo "FAIL: $*"; exit 1; }
mkdir -p "$T/bin" "$T/home/.r2-sync-state"
cat > "$T/bin/rclone" <<STUB
#!/bin/bash
case "\$1" in
  lsf)
    if [[ " \$* " == *" -R "* ]]; then echo "2026-10-01 00:00:00"; exit 0; fi
    last="\${@: -1}"
    if [[ "\$last" =~ ^[a-z0-9_]+:[A-Za-z0-9._-]+\$ ]]; then printf 'sch/\n'; fi
    exit 0 ;;
  copy) if [ -e "$T/block" ]; then exec sleep 300; fi; exit 0 ;;
  *) exit 0 ;;
esac
STUB
chmod +x "$T/bin/rclone"
run_sync() {
  ( export HOME="$T/home" PATH="$T/bin:$PATH" GOVDATA_R2_SYNC_STATE_DIR="$T/home/.r2-sync-state" \
      PROD_AWS_ACCESS_KEY_ID=x PROD_AWS_SECRET_ACCESS_KEY=x PROD_AWS_ENDPOINT_OVERRIDE=http://localhost:1 \
      GOVDATA_R2_SYNC_SPLIT_SCHEMAS=none
    exec bash "$DIR/sync-to-r2.sh" --schemas sch ) 
}
LOCK="$T/home/.r2-sync-state/.sync-to-r2.lock"

touch "$T/block"
run_sync > "$T/p1.out" 2>&1 & p1=$!
for _ in $(seq 50); do pgrep -f "^sleep 300$" >/dev/null && break; sleep 0.2; done
pgrep -f "^sleep 300$" >/dev/null || fail "stub copy never started: $(tail -5 "$T/p1.out")"

# 1. held lock -> second pass is SKIPPED with exit 75
run_sync > "$T/p2.out" 2>&1; rc=$?
[ "$rc" -eq 75 ] || fail "second pass rc=$rc, expected 75: $(cat "$T/p2.out")"
grep -q 'SKIPPED' "$T/p2.out" || fail "held lock not reported as SKIPPED"
echo "PASS held lock is a skip (75), not a success"

# 2. descendants do not hold the lock fd
holders=$(fuser "$LOCK" 2>/dev/null | wc -w)
for pid in $(fuser "$LOCK" 2>/dev/null); do
  case "$(tr '\0' ' ' < /proc/$pid/cmdline)" in *flock*) ;; *) fail "non-flock process $pid holds the lock: $(tr '\0' ' ' < /proc/$pid/cmdline)";; esac
done
echo "PASS only the flock wrapper holds the lock"

# 3. TERM to the pass leaves nothing running and nothing holding the lock
pkill -TERM -f "^bash $DIR/sync-to-r2.sh --schemas sch"
wait "$p1" 2>/dev/null
sleep 1
pgrep -f "^sleep 300$" >/dev/null && fail "rclone descendant survived TERM"
[ -z "$(fuser "$LOCK" 2>/dev/null)" ] || fail "lock still held after TERM: $(fuser "$LOCK" 2>&1)"
echo "PASS TERM leaves no descendant and no lock holder"

# 4. a second pass can take the lock
rm -f "$T/block"
run_sync > "$T/p3.out" 2>&1 || fail "pass after TERM failed: $(tail -5 "$T/p3.out")"
echo "PASS next pass takes the lock"
echo "ALL PASS"
