#!/usr/bin/env bash
# The three sec.gov fetch scripts send the declared User-Agent (resolved by the govdata jar from
# sec-schema.yaml) and stop before any request when it cannot be resolved. Runs against a local HTTP
# server, never sec.gov; java is stubbed, the resolver itself is covered by EdgarUserAgentTest.
set -uo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
work="$(mktemp -d)"; trap 'rm -rf "$work"' EXIT
mkdir "$work/tmp" "$work/out" "$work/feed" "$work/bin"; : > "$work/jar"
cat > "$work/server.py" <<'PY'
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
log = sys.argv[2]
class H(BaseHTTPRequestHandler):
    def do_GET(self):
        open(log, "a").write("UA=" + str(self.headers.get("User-Agent")) + "\n")
        self.send_response(404); self.send_header("Content-Length", "0"); self.end_headers()
    def log_message(self, *a): pass
HTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
PY
DECLARED="declared-agent contact@example.org"
stub() { printf '#!/usr/bin/env bash\n%s\n' "$1" > "$work/bin/java"; chmod +x "$work/bin/java"; }
export PATH="$work/bin:$PATH" GOVDATA_TMPDIR="$work/tmp" GOVDATA_JAR="$work/jar"
fail=0; port=18610
runscript() { # name port
  case "$1" in
    sub)  SUBMISSIONS_URL="http://127.0.0.1:$2/s.zip" bash "$here/fetch-submissions-bulk.sh" "$work/out" ;;
    13f)  F13_BASE_URL="http://127.0.0.1:$2" bash "$here/fetch-13f-bulk.sh" "$work/out" 2024q1 ;;
    feed) FEED_BASE_URL="http://127.0.0.1:$2" MAX_RETRIES=1 RETRY_DELAY=1 bash "$here/fetch-edgar-feed.sh" "$work/feed" 20240102 ;;
  esac
}
for s in sub 13f feed; do
  p=$((port++)); : > "$work/req"; rm -f "$work/tmp/"*.blocked
  stub "echo '$DECLARED'"
  timeout 20 python3 -I "$work/server.py" $p "$work/req" & sleep 1
  runscript $s $p >/dev/null 2>&1
  if [ "$(sort -u "$work/req")" = "UA=$DECLARED" ]; then echo "PASS $s sends the declared User-Agent"
  else echo "FAIL $s User-Agent: $(sort -u "$work/req" | tr '\n' ' ')"; fail=1; fi
  # unresolvable: stop before any request, say what is missing
  : > "$work/req"; stub "echo 'cannot resolve' >&2; exit 1"
  runscript $s $p >/dev/null 2>"$work/err"; rc=$?
  if [ $rc -ne 0 ] && [ ! -s "$work/req" ] && grep -q "User-Agent" "$work/err"; then echo "PASS $s unresolvable agent stops before any request"
  else echo "FAIL $s unresolvable rc=$rc requests=$(wc -l < "$work/req")"; cat "$work/err"; fail=1; fi
  # no jar configured: fails naming GOVDATA_JAR
  : > "$work/req"
  env -u GOVDATA_JAR bash -c "$(declare -f runscript); here='$here'; work='$work'; runscript $s $p" >/dev/null 2>"$work/err"; rc=$?
  if [ $s = feed ]; then :; elif [ $rc -ne 0 ] && [ ! -s "$work/req" ] && grep -q GOVDATA_JAR "$work/err"; then echo "PASS $s without GOVDATA_JAR fails naming it"
  else echo "FAIL $s no jar rc=$rc"; cat "$work/err"; fail=1; fi
done
# no placeholder or default agent anywhere under govdata/scripts
if grep -rnE 'apache-calcite@|Apache Calcite SEC|SEC_USER_AGENT' "$here" --include='*.sh' --exclude='test-fetch-sec-user-agent.sh' >/dev/null; then
  echo "FAIL placeholder User-Agent still under scripts"; fail=1; else echo "PASS no placeholder User-Agent under scripts"; fi
exit $fail
