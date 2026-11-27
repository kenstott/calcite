#!/usr/bin/env bash
# s3_purge_prefix.py must:
#   - exit non-zero within its timeout, naming the key, when the endpoint accepts
#     connections and never answers;
#   - wait out a LIST that answers slower than the per-object timeout.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORT_FILE=$(mktemp)
SLOW_PORT_FILE=$(mktemp)
PIDS=()
trap 'kill "${PIDS[@]}" 2>/dev/null || true; rm -f "$PORT_FILE" "$SLOW_PORT_FILE" /tmp/dq-purge-test.err' EXIT

python3 - "$PORT_FILE" <<'PY' &
import socket, sys
s = socket.socket(); s.bind(('127.0.0.1', 0)); s.listen(16)
open(sys.argv[1], 'w').write(str(s.getsockname()[1]))
conns = []
while True:
    conns.append(s.accept()[0])
PY
PIDS+=($!)

# LIST answers after 3s with one key; DELETE answers at once.
python3 - "$SLOW_PORT_FILE" <<'PY' &
import sys, time
from http.server import BaseHTTPRequestHandler, HTTPServer

class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        time.sleep(3)
        body = (b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                b'<Contents><Key>lands/a.parquet</Key></Contents></ListBucketResult>')
        self.send_response(200)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_DELETE(self):
        self.send_response(204)
        self.end_headers()

s = HTTPServer(('127.0.0.1', 0), H)
open(sys.argv[1], 'w').write(str(s.server_port))
s.serve_forever()
PY
PIDS+=($!)

for _ in $(seq 50); do [ -s "$PORT_FILE" ] && [ -s "$SLOW_PORT_FILE" ] && break; sleep 0.1; done
export AWS_ACCESS_KEY_ID=x AWS_SECRET_ACCESS_KEY=y

START=$(date +%s)
rc=0
timeout 30 python3 "$DIR/s3_purge_prefix.py" "http://127.0.0.1:$(cat "$PORT_FILE")" \
  bkt lands 1 2 1 2>/tmp/dq-purge-test.err || rc=$?
ELAPSED=$(( $(date +%s) - START ))
[ "$rc" -eq 1 ] || { echo "FAIL: expected exit 1, got $rc"; exit 1; }
[ "$ELAPSED" -le 10 ] || { echo "FAIL: took ${ELAPSED}s"; exit 1; }
grep -q 'S3 purge failed: LIST lands/' /tmp/dq-purge-test.err || { echo "FAIL: message"; cat /tmp/dq-purge-test.err; exit 1; }
echo "PASS: exit 1 after ${ELAPSED}s: $(cat /tmp/dq-purge-test.err)"

rc=0
out=$(timeout 30 python3 "$DIR/s3_purge_prefix.py" "http://127.0.0.1:$(cat "$SLOW_PORT_FILE")" \
  bkt lands 1 1 10 2>&1) || rc=$?
[ "$rc" -eq 0 ] || { echo "FAIL: slow LIST: exit $rc: $out"; exit 1; }
[ "$out" = "Purged 1 objects for schema=lands from bkt" ] || { echo "FAIL: slow LIST output: $out"; exit 1; }
echo "PASS: slow LIST waited out: $out"
