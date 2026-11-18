#!/usr/bin/env bash
# fix-sec.sh must survive being rewritten on disk while it runs (#1318): bash reads scripts
# incrementally, so an in-place edit made it resume mid-line and run `_q` from `psql_q`.
# Runs the real script's snapshot prologue against a stand-in body that rewrites its own source
# (via $FIX_SEC_SELF) between two commands, and asserts the second command still runs intact.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
work="$(mktemp -d)"; trap 'rm -rf "$work"' EXIT
# prologue = everything in fix-sec.sh up to and including the SCRIPT_DIR assignment
awk '/^if \[ -z "\$\{FIX_SEC_SNAPSHOT/{p=1} p{print} /^SCRIPT_DIR=/{exit}' "$here/fix-sec.sh" > "$work/prologue"
[ -s "$work/prologue" ] || { echo "FAIL: snapshot prologue not found in fix-sec.sh" >&2; exit 1; }
{
  echo '#!/usr/bin/env bash'
  cat "$work/prologue"
  cat <<'BODY'
psql_q() { echo "ran:psql_q"; }
psql_q
{ echo '# padding added mid-run'; echo '# padding added mid-run'; cat "$FIX_SEC_SELF"; } > "$FIX_SEC_SELF.new" && cat "$FIX_SEC_SELF.new" > "$FIX_SEC_SELF"
psql_q
echo "done"
BODY
} > "$work/t.sh"
chmod +x "$work/t.sh"
out=$("$work/t.sh" 2>&1)
[ "$out" = "$(printf 'ran:psql_q\nran:psql_q\ndone')" ] || { echo "FAIL: got: $out" >&2; exit 1; }
ls "${TMPDIR:-/tmp}"/fix-sec.?????? >/dev/null 2>&1 && { echo "FAIL: snapshot leaked" >&2; exit 1; }
echo "PASS: script survived being rewritten mid-run"
