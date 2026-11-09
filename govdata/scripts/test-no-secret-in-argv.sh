#!/usr/bin/env bash
# Fails if a secret value can appear on a command line or in a bash -x trace.
# 1) Runs orphan-file-cleanup.sh and idle-snapshot-expiry.sh under bash -x with a sentinel secret and stub
#    java/duckdb binaries that record their argv; the sentinel must be in neither the trace nor the argv.
# 2) Static scan: no script passes a secret as an option value or expands it inside a SQL heredoc.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SENTINEL="SENTINEL-SECRET-$$-zq9"
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
mkdir -p "$WORK/bin"
for tool in java duckdb; do
  printf '#!/usr/bin/env bash\nprintf "%%s\\n" "$@" >> "%s/argv.txt"\ncat > /dev/null < /dev/stdin 2>/dev/null || true\n' "$WORK" > "$WORK/bin/$tool"
  chmod +x "$WORK/bin/$tool"
done
touch "$WORK/fake.jar"
fail=0
for s in orphan-file-cleanup.sh idle-snapshot-expiry.sh; do
  env PATH="$WORK/bin:$PATH" AWS_ACCESS_KEY_ID=akid AWS_SECRET_ACCESS_KEY="$SENTINEL" \
      AWS_ENDPOINT_OVERRIDE=http://localhost:9000 GOVDATA_PARQUET_DIR="$WORK" GOVDATA_JAR="$WORK/fake.jar" \
      bash -x "$HERE/$s" > "$WORK/out.txt" 2>&1 < /dev/null
  if grep -q "$SENTINEL" "$WORK/out.txt"; then echo "FAIL: $s leaks the secret in its bash -x trace"; fail=1; fi
done
if [[ -f "$WORK/argv.txt" ]]; then
  grep -q "$SENTINEL" "$WORK/argv.txt" && { echo "FAIL: a secret reached a process argument"; fail=1; }
  grep -q -- "--s3-credentials-from-env" "$WORK/argv.txt" || { echo "FAIL: stub java never received --s3-credentials-from-env"; fail=1; }
else
  echo "FAIL: stub java/duckdb were never invoked"; fail=1
fi
hits="$(cd "$HERE" && grep -rnE --include='*.sh' -- "--s3-secret-key|--s3-secret-access-key|s3_secret_access_key *= *'\\$\\{|SECRET '\\$\\{AWS_SECRET" . | grep -v "test-no-secret-in-argv.sh")"
if [[ -n "$hits" ]]; then echo "FAIL: secret expanded into an argument or SQL text:"; echo "$hits"; fail=1; fi
[[ $fail -eq 0 ]] && echo "PASS: no secret in argv or bash -x trace"
exit $fail
