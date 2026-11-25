#!/usr/bin/env bash
# worker-dq-run.sh reaches the DQ object store only through GOVDATA_DQ_RCLONE_REMOTE: it stops when
# that is unset, and a failing rclone call stops the run. Runs a copy of the scripts in a temp tree
# (no credentials files) with every external command stubbed to record its call.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fail=0
ok() { echo "PASS $1"; }
bad() { echo "FAIL $1"; fail=1; }

T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
S="$T/govdata/scripts"; P="$S/parallel"; CALLS="$T/calls"
mkdir -p "$P" "$T/bin" "$T/claims"
cp "$HERE"/../*.sh "$S/"
cp "$HERE"/*.sh "$HERE"/*.py "$P/"
echo "SELECT 1;" > "$S/zzrem_dq.sql"
printf '#!/bin/sh\necho "worker.sh $*" >> "%s"\n' "$CALLS" > "$P/worker.sh"
for c in aws psql gh curl python3 duckdb; do
  printf '#!/bin/sh\necho "%s $*" >> "%s"\n' "$c" "$CALLS" > "$T/bin/$c"
done
# the rclone stub fails the subcommand named in RCLONE_FAIL
printf '#!/bin/sh\necho "rclone $*" >> "%s"\n[ "$1" != "${RCLONE_FAIL:-}" ]\n' "$CALLS" > "$T/bin/rclone"
chmod +x "$T/bin/"*

run() { # extra-env... -- args... -> output in $out, rc in $rc, external calls in $CALLS
  local envs=()
  while [ "$1" != "--" ]; do envs+=("$1"); shift; done
  shift
  : > "$CALLS"
  out=$(env -i HOME="$T" PATH="$T/bin:/usr/bin:/bin" GOVDATA_TMPDIR="$T/tmp" GOVDATA_CLAIM_DIR="$T/claims" \
    GOVDATA_DQ_BUCKET=govdata-parquet-v1-dq AWS_ACCESS_KEY_ID=x AWS_SECRET_ACCESS_KEY=y \
    GOVDATA_CACHE_DIR="s3://zz-raw" "${envs[@]}" \
    timeout 60 bash "$P/worker-dq-run.sh" zzrem "$@" 2>&1); rc=$?
}
EP="AWS_ENDPOINT_OVERRIDE=http://127.0.0.1:1"

for flags in "" "--rebuild" "--etl-resume"; do
  for prod_remote in "" "zzprod"; do
    # shellcheck disable=SC2086
    run "$EP" GOVDATA_RCLONE_REMOTE="$prod_remote" -- $flags
    [ $rc -eq 1 ] && [[ "$out" == *"GOVDATA_DQ_RCLONE_REMOTE is not set"* ]] \
      && ok "unset DQ remote (flags='$flags', prod remote='$prod_remote') stops the run" || bad "unset: rc=$rc $out"
    [ ! -s "$CALLS" ] && [ -z "$(ls "$T/claims")" ] \
      && ok "...with no external command and no claim left" || bad "ran: $(cat "$CALLS")"
  done
done

run "$EP" -- --rebuild --dry-run
[ $rc -eq 0 ] && ok "a rebuild dry run uses no remote and needs none" || bad "dry plan: rc=$rc $out"

run "$EP" GOVDATA_DQ_RCLONE_REMOTE=zzremote -- --rebuild
grep -q 'zzremote:govdata-tracker-v1-dq/dq-results/schema=zzrem' "$CALLS" \
  && [[ "$out" == *"no pipeline_tracker found"* ]] \
  && ok "a rebuild addresses the configured remote and goes on to the tracker step" || bad "rebuild: $(cat "$CALLS") $out"
grep -E '^rclone ' "$CALLS" | grep -qv ' zzremote:' \
  && bad "another remote was used: $(cat "$CALLS")" || ok "no other remote is used"

run "$EP" GOVDATA_DQ_RCLONE_REMOTE=zzremote RCLONE_FAIL=purge -- --rebuild
[ $rc -ne 0 ] && [[ "$out" != *"no pipeline_tracker found"* ]] && ! grep -q '^worker.sh' "$CALLS" \
  && ok "a failed results teardown stops the run before the tracker and ETL" || bad "teardown failure: rc=$rc $(cat "$CALLS")"

# no endpoint override: metadata-only teardown, listed through rclone
run GOVDATA_DQ_RCLONE_REMOTE=zzremote RCLONE_FAIL=lsd -- --rebuild
[ $rc -ne 0 ] && ! grep -q '^worker.sh' "$CALLS" \
  && ok "a failed table listing stops the run before the ETL" || bad "lsd failure: rc=$rc $(cat "$CALLS") $out"

run GOVDATA_DQ_RCLONE_REMOTE=zzremote RCLONE_FAIL=ls --
[ $rc -ne 0 ] && ! grep -q '^duckdb' "$CALLS" \
  && ok "a failed path check stops the run before DQ" || bad "ls failure: rc=$rc $(cat "$CALLS") $out"

exit $fail
