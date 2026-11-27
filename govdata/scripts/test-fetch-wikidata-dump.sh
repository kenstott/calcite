#!/usr/bin/env bash
# Tests fetch-wikidata-dump.sh against a local HTTP server (never wikimedia.org): newest complete
# dump is chosen, segments resume, the file appears only after its MD5 matched, old dumps are
# pruned, and 429 stops the run.
set -uo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
work="$(mktemp -d)"; server=""
trap 'if [ -n "$server" ]; then kill "$server" 2>/dev/null; fi; rm -rf "$work"' EXIT
mkdir -p "$work/www/20260103" "$work/www/20260110" "$work/out"
head -c 100001 /dev/urandom > "$work/www/20260103/wikidata-20260103-all.json.gz"
( cd "$work/www/20260103" && md5sum wikidata-20260103-all.json.gz > wikidata-20260103-md5sums.txt )
# 20260110 is newer but has no checksum file yet, i.e. not finished.
cat > "$work/server.py" <<'PY'
import os, re, sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
root, flag = sys.argv[2], sys.argv[3]
class H(BaseHTTPRequestHandler):
    def reply(self, body_wanted):
        if self.path.endswith("/"):
            body = "".join('<a href="%s/">%s/</a>\n' % (d, d) for d in sorted(os.listdir(root))).encode()
            return self.send(200, body, body_wanted)
        path = os.path.join(root, self.path.lstrip("/"))
        if not os.path.isfile(path):
            return self.send(404, b"", body_wanted)
        if os.path.exists(flag) and path.endswith(".gz") and body_wanted:
            return self.send(429, b"slow down", body_wanted)
        data = open(path, "rb").read()
        m = re.match(r"bytes=(\d+)-(\d+)", self.headers.get("Range", ""))
        if m:
            return self.send(206, data[int(m.group(1)):int(m.group(2)) + 1], body_wanted)
        self.send(200, data, body_wanted)
    def send(self, status, body, body_wanted):
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body_wanted:
            self.wfile.write(body)
    def do_GET(self): self.reply(True)
    def do_HEAD(self): self.reply(False)
    def log_message(self, *a): pass
ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
PY
port=18611
timeout 120 python3 -I "$work/server.py" "$port" "$work/www" "$work/429" & server=$!
sleep 1
fail=0
run() {
  WIKIDATA_DUMP_BASE_URL="http://127.0.0.1:$port" WIKIDATA_DUMP_KEEP=2 MAX_RETRIES=1 RETRY_DELAY=1 \
    bash "$here/fetch-wikidata-dump.sh" "$work/out" "$@" >"$work/log" 2>&1
}
check() { if eval "$2"; then echo "PASS $1"; else echo "FAIL $1"; cat "$work/log"; fail=1; fi; }
src="$work/www/20260103/wikidata-20260103-all.json.gz"
dst="$work/out/wikidata-20260103-all.json.gz"

# A partial first segment from an earlier run is continued, not restarted.
mkdir -p "$work/out/.dl-20260103"
head -c 1234 "$src" > "$work/out/.dl-20260103/part0"
run; rc=$?
check "newest complete dump downloaded and resumed" '[ $rc -eq 0 ] && cmp -s "$src" "$dst"'
check "work directory removed" '[ ! -d "$work/out/.dl-20260103" ]'

rm -f "$dst"; touch "$work/429"
run 20260103; rc=$?
check "429 stops with no dump file" '[ $rc -ne 0 ] && [ ! -f "$dst" ] && grep -q "HTTP 429" "$work/log"'
rm -f "$work/429"; rm -rf "$work/out/.dl-20260103"

# A corrupt resumed segment must never be published.
mkdir -p "$work/out/.dl-20260103"
head -c 1234 /dev/zero > "$work/out/.dl-20260103/part0"
run 20260103; rc=$?
check "MD5 mismatch is refused" '[ $rc -ne 0 ] && [ ! -f "$dst" ] && grep -q "MD5 mismatch" "$work/log"'
rm -rf "$work/out/.dl-20260103"

: > "$work/out/wikidata-20251220-all.json.gz"
: > "$work/out/wikidata-20251227-all.json.gz"
run 20260103; rc=$?
check "keeps the newest two dumps" '[ $rc -eq 0 ] && [ -f "$dst" ] && [ -f "$work/out/wikidata-20251227-all.json.gz" ] && [ ! -f "$work/out/wikidata-20251220-all.json.gz" ]'
exit $fail
