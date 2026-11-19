#!/bin/bash
# The sync lock must belong to the pass alone (govdata-ops#1356): a TERM'd pass leaves no process holding
# it, a pass that finds it held exits 75 and names the holder, and a second pass can then take it.
# Runs the real sync-to-r2.sh against a stub rclone (no network, no MinIO, throwaway $HOME).
set -u
SCRIPT="${1:-$(cd "$(dirname "$0")" && pwd)/sync-to-r2.sh}"
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
mkdir -p "$T/bin" "$T/home"
MARK="9.$$"
cat > "$T/bin/rclone" <<STUB
#!/bin/bash
case "\$1" in
  lsf)
    if [[ " \$* " == *" -R "* ]]; then echo "2026-10-01 00:00:00"; exit 0; fi
    last="\${@: -1}"
    if [[ "\$last" =~ ^[a-z0-9_]+:[A-Za-z0-9._-]+\$ ]]; then printf 'slowone/\n'; fi
    exit 0 ;;
  copy) echo "\$\$" > "\$STUB_PID"; sleep $MARK | sed 's/x/y/'; exit 0 ;;
  *) exit 0 ;;
esac
STUB
chmod +x "$T/bin/rclone"
export STUB_PID="$T/rclone.pid" HOME="$T/home" PATH="$T/bin:$PATH"
LOCK="$HOME/.r2-sync-state/.sync-to-r2.lock"
fail=0
ck() { if eval "$2"; then echo "ok   $1"; else echo "FAIL $1"; fail=1; fi; }
wait_for() { for _ in $(seq 50); do eval "$1" && return 0; sleep 0.2; done; return 1; }

bash "$SCRIPT" --schemas slowone > "$T/p1.log" 2>&1 &
P1=$!
wait_for '[ -s "$STUB_PID" ]'
ck "pass 1 reached the stubbed copy"                      '[ -s "$STUB_PID" ]'

bash "$SCRIPT" --schemas slowone > "$T/p2.log" 2>&1; rc=$?
ck "pass 2 exits 75 while pass 1 holds the lock (rc=$rc)" '[ "$rc" -eq 75 ]'
ck "pass 2 names the holder pid"                          'grep -q "holds the lock (pid $P1)" "$T/p2.log"'

kill -TERM "$P1"; wait "$P1" 2>/dev/null
wait_for '! fuser "$LOCK" >/dev/null 2>&1'
ck "after TERM no process holds the lock file"            '! fuser "$LOCK" >/dev/null 2>&1'
ck "the stubbed rclone and its pipeline are gone"         '! pgrep -f "sleep $MARK" >/dev/null'

bash "$SCRIPT" --schemas slowone > "$T/p3.log" 2>&1 &
P3=$!
sleep 2
ck "pass 3 takes the lock after the TERM'd pass"          '! grep -q "holds the lock" "$T/p3.log" && kill -0 "$P3" 2>/dev/null'
kill -TERM "$P3" 2>/dev/null; wait "$P3" 2>/dev/null
exit $fail
