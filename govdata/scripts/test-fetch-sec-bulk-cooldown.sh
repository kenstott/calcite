#!/usr/bin/env bash
# fetch-submissions-bulk.sh and fetch-13f-bulk.sh against a local HTTP server (never sec.gov): 403/429
# stop after one request and record the cool-down, 503 is not retried inside the script, a cool-down
# marker refuses to start, an unset GOVDATA_TMPDIR fails naming it; run-scheduled's retry loop waits
# for the marker's end instead of its backoff.
set -uo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
work="$(mktemp -d)"; trap 'rm -rf "$work"' EXIT
mkdir "$work/tmp" "$work/out"
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
BLOCK="$work/tmp/calcite-ratelimit-sec.gov.blocked"
run() { # script status port expected_requests expect_marker expect_rc
  local script="$1" st="$2" port="$3" want_n="$4" want_m="$5" want_rc="$6"
  : > "$work/req"; rm -f "$BLOCK"
  timeout 20 python3 -I "$work/server.py" "$port" "$st" "$work/req" & sleep 1
  if [ "$script" = sub ]; then
    GOVDATA_TMPDIR="$work/tmp" SUBMISSIONS_URL="http://127.0.0.1:$port/s.zip" \
      bash "$here/fetch-submissions-bulk.sh" "$work/out" >/dev/null 2>"$work/err"
  else
    GOVDATA_TMPDIR="$work/tmp" F13_BASE_URL="http://127.0.0.1:$port" \
      bash "$here/fetch-13f-bulk.sh" "$work/out" 2024q1 2024q2 >/dev/null 2>"$work/err"
  fi
  local rc=$? n m=no; n=$(wc -l < "$work/req"); [ -f "$BLOCK" ] && m=yes
  if [ "$rc" -eq "$want_rc" ] && [ "$n" -eq "$want_n" ] && [ "$m" = "$want_m" ]; then
    echo "PASS $script HTTP $st: rc=$rc, $n request(s), marker=$m"
  else echo "FAIL $script HTTP $st: rc=$rc (want $want_rc) requests=$n (want $want_n) marker=$m (want $want_m)"; cat "$work/err"; fail=1; fi
}
port=18510
for s in sub 13f; do
  run $s 403 $((port++)) 1 yes 75
  run $s 429 $((port++)) 1 yes 75
  run $s 503 $((port++)) 1 no 1
done
# 429 Retry-After longer than the default lengthens the cool-down
rm -f "$BLOCK"; . "$here/sec-cooldown.sh"; GOVDATA_TMPDIR="$work/tmp" sec_cooldown_init
printf 'Retry-After: 7200\r\n' > "$work/hdr"; sec_cooldown_on_status t 429 "$work/hdr" 2>/dev/null
left=$(sec_cooldown_remaining)
[ "$left" -gt 7000 ] && echo "PASS Retry-After 7200 honoured (${left}s)" || { echo "FAIL Retry-After: ${left}s"; fail=1; }
# marker present -> refuse, zero requests, names marker and end time
printf '%s' $(( ($(date +%s) + 600) * 1000 )) > "$BLOCK"
for s in sub 13f; do
  : > "$work/req"
  if [ $s = sub ]; then GOVDATA_TMPDIR="$work/tmp" SUBMISSIONS_URL="http://127.0.0.1:1/x" bash "$here/fetch-submissions-bulk.sh" "$work/out" >/dev/null 2>"$work/err"
  else GOVDATA_TMPDIR="$work/tmp" F13_BASE_URL="http://127.0.0.1:1" bash "$here/fetch-13f-bulk.sh" "$work/out" 2024q1 >/dev/null 2>"$work/err"; fi
  rc=$?
  if [ $rc -eq 75 ] && grep -q "cool-down in effect until .*calcite-ratelimit-sec.gov.blocked" "$work/err"; then echo "PASS $s marker refuses start"; else echo "FAIL $s marker rc=$rc"; cat "$work/err"; fail=1; fi
done
# unset GOVDATA_TMPDIR fails naming it
for s in fetch-submissions-bulk.sh fetch-13f-bulk.sh; do
  env -u GOVDATA_TMPDIR bash "$here/$s" "$work/out" $([ "$s" = fetch-13f-bulk.sh ] && echo 2024q1) >/dev/null 2>"$work/err"
  if [ $? -ne 0 ] && grep -q GOVDATA_TMPDIR "$work/err"; then echo "PASS $s unset GOVDATA_TMPDIR fails naming it"; else echo "FAIL $s unset"; fail=1; fi
done
# run-scheduled: exit 75 waits for the marker's end (window too short -> gives up without looping)
tmp="$work/rs"; mkdir -p "$tmp/scripts/parallel" "$tmp/bulk"
SCRIPT_DIR="$tmp/scripts/parallel"; ts() { echo T; }; log_error() { :; }
SUBMISSIONS_BULK_DIR="$tmp/bulk"; SUBMISSIONS_RETRY_MIN_SECS=1; SUBMISSIONS_RETRY_MAX_SECS=1
GOVDATA_TMPDIR="$work/tmp"; sec_cooldown_init
eval "$(sed -n '/^prepare_sec_submissions_bulk() {/,/^}/p' "$here/parallel/run-scheduled.sh")"
printf '#!/usr/bin/env bash\necho x >> "%s/n"; exit 75\n' "$tmp" > "$tmp/scripts/fetch-submissions-bulk.sh"; chmod +x "$tmp/scripts/fetch-submissions-bulk.sh"
printf '%s' $(( ($(date +%s) + 600) * 1000 )) > "$BLOCK"; : > "$tmp/w.log"
prepare_sec_submissions_bulk "$tmp/w.log" $(( $(date +%s) + 30 )); rc=$?
[ "$rc" -eq 1 ] && [ "$(wc -l < "$tmp/n")" -eq 1 ] && grep -q "cool-down" "$tmp/w.log" \
  && echo "PASS run-scheduled stops for the marker (1 fetch call)" || { echo "FAIL run-scheduled rc=$rc calls=$(wc -l < "$tmp/n")"; fail=1; }
exit $fail
