#!/bin/bash
# A script run under bash -x must never print a credential value from an env file, and log files
# created for runs/ must be owner-only.
set -u
P="$(cd "$(dirname "$0")" && pwd)"; fail=0
ok() { if eval "$2"; then echo "ok   $1"; else echo "FAIL $1"; fail=1; fi; }
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
SENTINEL="sentinel-secret-$$-ZXQ"
mkdir -p "$T/root/govdata"
printf 'AWS_ACCESS_KEY_ID=%s\r\nAWS_SECRET_ACCESS_KEY=%s\r\nPROD_AWS_ACCESS_KEY_ID=%s\nPROD_AWS_SECRET_ACCESS_KEY=%s\nPROD_AWS_ENDPOINT_OVERRIDE=https://r2.invalid\nGOVDATA_TRACKER_PG_PASSWORD=%s\n' \
  "$SENTINEL" "$SENTINEL" "$SENTINEL" "$SENTINEL" "$SENTINEL" > "$T/root/govdata/.env.prod"
printf 'GOVDATA_DQ_BUCKET=x-dq\nDQ_SECRET=%s\n' "$SENTINEL" > "$T/root/govdata/.env.dq"
C="$P/common.sh"; R="$T/root/govdata"

trace=$(GOVDATA_TRACKER_PG_PASSWORD="$SENTINEL" GOVDATA_DQ=true bash -x -c \
  "source '$C'; GOVDATA_ROOT='$R'; load_env; configure_r2_remote; [[ \${#AWS_SECRET_ACCESS_KEY} -gt 10 ]] && echo LOADED; case \$- in *x*) echo XTRACE_RESTORED;; esac" 2>&1)
ok "load_env still exports the values"                'echo "$trace" | grep -qx LOADED'
ok "xtrace is restored after load_env"                'echo "$trace" | grep -qx XTRACE_RESTORED'
ok "no sentinel in the trace of load_env/configure_r2_remote" '! echo "$trace" | grep -q "$SENTINEL"'
ok "the trace is still active for ordinary commands"  'echo "$trace" | grep -q "^+ source "'

trace=$(bash -x -c "source '$P/../env-source.sh'; source_env_file '$T/root/govdata/.env.prod'; echo \${#AWS_SECRET_ACCESS_KEY}" 2>&1)
ok "source_env_file keeps the sentinel out of the trace" '! echo "$trace" | grep -q "$SENTINEL"'

mkdir -p "$T/runs"
( source "$C"; private_dir "$T/runs/w"; private_log "$T/runs/w/etl.log" )
ok "private_dir is mode 700"  '[ "$(stat -c %a "$T/runs/w")" = 700 ]'
ok "private_log is mode 600"  '[ "$(stat -c %a "$T/runs/w/etl.log")" = 600 ]'
exit $fail
