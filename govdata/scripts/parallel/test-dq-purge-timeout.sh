#!/usr/bin/env bash
# s3_purge_prefix.py must exit non-zero within its timeout, naming the key, when the
# endpoint accepts connections and never answers.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORT_FILE=$(mktemp)
python3 - "$PORT_FILE" <<'PY' &
import socket, sys, time
s = socket.socket(); s.bind(('127.0.0.1', 0)); s.listen(16)
open(sys.argv[1], 'w').write(str(s.getsockname()[1]))
conns = []
while True:
    conns.append(s.accept()[0])
PY
SRV=$!
trap 'kill $SRV 2>/dev/null || true; rm -f "$PORT_FILE" /tmp/dq-purge-test.err' EXIT
for _ in $(seq 50); do [ -s "$PORT_FILE" ] && break; sleep 0.1; done
PORT=$(cat "$PORT_FILE")
START=$(date +%s)
rc=0
AWS_ACCESS_KEY_ID=x AWS_SECRET_ACCESS_KEY=y timeout 30 \
  python3 "$DIR/s3_purge_prefix.py" "http://127.0.0.1:$PORT" bkt lands 1 2 2>/tmp/dq-purge-test.err || rc=$?
ELAPSED=$(( $(date +%s) - START ))
[ "$rc" -eq 1 ] || { echo "FAIL: expected exit 1, got $rc"; exit 1; }
[ "$ELAPSED" -le 10 ] || { echo "FAIL: took ${ELAPSED}s"; exit 1; }
grep -q 'S3 purge failed: LIST lands/' /tmp/dq-purge-test.err || { echo "FAIL: message"; cat /tmp/dq-purge-test.err; exit 1; }
echo "PASS: exit 1 after ${ELAPSED}s: $(cat /tmp/dq-purge-test.err)"
