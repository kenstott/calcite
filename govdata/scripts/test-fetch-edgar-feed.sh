#!/usr/bin/env bash
# Tests fetch-edgar-feed.sh against a local HTTP server (never sec.gov): 403/429 stop after one
# request and record the cool-down; 503 is retried; a present cool-down marker refuses to start.
set -uo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
work="$(mktemp -d)"; trap 'rm -rf "$work"' EXIT
mkdir "$work/tmp" "$work/feed" "$work/bin"; : > "$work/jar"
printf '#!/usr/bin/env bash\necho "test-agent test@example.org"\n' > "$work/bin/java"; chmod +x "$work/bin/java"
export PATH="$work/bin:$PATH"
cat > "$work/server.py" <<'PY'
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
status = int(sys.argv[2]); log = sys.argv[3]
class H(BaseHTTPRequestHandler):
    def do_GET(self):
        open(log, "a").write("x\n")
        self.send_response(status)
        if status == 429: self.send_header("Retry-After", "5")
        self.send_header("Content-Length", "0"); self.end_headers()
    def log_message(self, *a): pass
HTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
PY
fail=0
run() { # status port expected_requests expect_marker
  : > "$work/req"; rm -f "$work/tmp/"*.blocked
  # the server ends itself after 20 s (timeout), so nothing needs stopping
  timeout 20 python3 -I "$work/server.py" "$2" "$1" "$work/req" & sleep 1
  GOVDATA_TMPDIR="$work/tmp" GOVDATA_JAR="$work/jar" FEED_BASE_URL="http://127.0.0.1:$2" \
    MAX_RETRIES=2 RETRY_DELAY=1 bash "$here/fetch-edgar-feed.sh" "$work/feed" 20240102 >/dev/null 2>"$work/err"
  rc=$?
  n=$(wc -l < "$work/req"); m=no; [ -f "$work/tmp/calcite-ratelimit-sec.gov.blocked" ] && m=yes
  if [ "$rc" -ne 0 ] && [ "$n" -eq "$3" ] && [ "$m" = "$4" ]; then echo "PASS HTTP $1: $n request(s), marker=$m"
  else echo "FAIL HTTP $1: rc=$rc requests=$n (want $3) marker=$m (want $4)"; cat "$work/err"; fail=1; fi
}
run 403 18403 1 yes
run 429 18429 1 yes
run 503 18503 3 no
# cool-down marker present -> refuse to start, zero requests
printf '%s' $(( ($(date +%s) + 600) * 1000 )) > "$work/tmp/calcite-ratelimit-sec.gov.blocked"
GOVDATA_TMPDIR="$work/tmp" GOVDATA_JAR="$work/jar" FEED_BASE_URL="http://127.0.0.1:1" bash "$here/fetch-edgar-feed.sh" "$work/feed" 20240102 >/dev/null 2>"$work/err"
if [ $? -ne 0 ] && grep -q "cool-down in effect" "$work/err"; then echo "PASS marker refuses start"; else echo "FAIL marker"; fail=1; fi
# no GOVDATA_TMPDIR -> fail loudly naming it, no request, no marker written anywhere
env -u GOVDATA_TMPDIR -u BLOCK_DIR GOVDATA_JAR="$work/jar" FEED_BASE_URL="http://127.0.0.1:1" \
  bash "$here/fetch-edgar-feed.sh" "$work/feed" 20240102 >/dev/null 2>"$work/err"
if [ $? -ne 0 ] && grep -q "GOVDATA_TMPDIR" "$work/err"; then echo "PASS unset GOVDATA_TMPDIR fails naming it"; else echo "FAIL unset"; cat "$work/err"; fail=1; fi
# the pool gives the workers java.io.tmpdir=$GOVDATA_TMPDIR, the directory the script writes to
if grep -q 'Djava.io.tmpdir=\$GOVDATA_TMPDIR' "$here/parallel/common.sh"; then echo "PASS common.sh sets java.io.tmpdir to GOVDATA_TMPDIR"; else echo "FAIL common.sh tmpdir"; fail=1; fi
exit $fail
