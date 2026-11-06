#!/usr/bin/env bash
# sec.gov cool-down outcomes, using the REAL functions from common.sh and run-pool.sh:
#  1. a worker that exits 75 is counted Deferred, not Failed, and the pool's exit decision ignores it
#  2. a real failure is still counted Failed and still makes the pool exit 2
#  3. a run that dies on the sec.gov refusal after the marker appears mid-run maps to 75 in BOTH
#     run_etl and run_etl_inline (a fake java stands in for EtlRunner and writes the marker itself)
#  4. the same refusal with no active marker, or any other failure, is not mapped
# Usage: test-pool-sec-cooldown-deferred.sh
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
POOL="$HERE/run-pool.sh"
fail=0; T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
ok() { echo "ok   $1"; }
bad() { echo "FAIL $1"; fail=1; }

export GOVDATA_TMPDIR="$T/tmp"; mkdir -p "$GOVDATA_TMPDIR"
MARKER="$GOVDATA_TMPDIR/calcite-ratelimit-sec.gov.blocked"
REFUSAL="IllegalStateException: sec.gov refused this host with HTTP 403/429; no requests until"

# ---- 1/2: pool counting and exit decision -------------------------------------------------
extract() { awk -v fn="$1" '$0 ~ "^"fn"\\(\\) \\{" {p=1} p {print} p && /^}/ {exit}' "$POOL"; }
exit_block=$(awk '/^if \[ "\$failed_count" -gt 0 \]; then$/ {p=1} p {print} p && /^fi$/ {exit}' "$POOL")
{ echo 'SEC_COOLDOWN_EXIT=75; log_info() { echo "$*"; }'
  echo 'deferred_count=0; failed_count=0; deferred_list=(); failed_list=()'
  extract count_unsuccessful_exit; } > "$T/pool-fns.sh"
grep -q "^count_unsuccessful_exit()" "$T/pool-fns.sh" && [ -n "$exit_block" ] \
  || { echo "FAIL: could not extract the pool functions from $POOL"; exit 1; }

pool_outcome() { # exit codes... -> "deferred=N failed=N pool_exit=N"
  ( source "$T/pool-fns.sh"
    n=0; for c in "$@"; do n=$((n+1)); count_unsuccessful_exit "worker-$n" "$c" 3 >/dev/null; done
    # the pool's real exit decision, run in a subshell so its `exit 2` is observable
    ( eval "$exit_block" ) >/dev/null 2>&1; rc=$?
    echo "deferred=$deferred_count failed=$failed_count pool_exit=$rc" )
}
[ "$(pool_outcome 75 75 75)" = "deferred=3 failed=0 pool_exit=0" ] \
  && ok "workers exiting 75 are Deferred, Failed stays 0, pool exit unaffected" \
  || bad "75-only: $(pool_outcome 75 75 75)"
[ "$(pool_outcome 75 1 75)" = "deferred=2 failed=1 pool_exit=2" ] \
  && ok "a real failure beside deferred workers is still Failed and the pool exits 2" \
  || bad "mixed: $(pool_outcome 75 1 75)"
[ "$(pool_outcome 137)" = "deferred=0 failed=1 pool_exit=2" ] \
  && ok "exit 137 is Failed" || bad "137: $(pool_outcome 137)"

# ---- 3/4: mid-run mapping through the real run_etl and run_etl_inline ----------------------
cat > "$T/fake-java" <<'JAVA'
#!/usr/bin/env bash
# stands in for EtlRunner: FAKE_MODE=refuse-and-mark writes the marker then dies on the refusal;
# refuse-only prints the refusal with no marker; crash dies with an unrelated error
case "$FAKE_MODE" in
  refuse-and-mark) printf '%s' $(( ($(date +%s) + 600) * 1000 )) > "$FAKE_MARKER"
                   echo "Failed to create schema: sec: $FAKE_REFUSAL" ;;
  refuse-only)     echo "Failed to create schema: sec: $FAKE_REFUSAL" ;;
  crash)           echo "java.lang.OutOfMemoryError: Java heap space" ;;
esac
exit 1
JAVA
chmod +x "$T/fake-java"

drive() { # runner mode -> exit status of the runner, run in a clean shell with stubs
  ( set -u
    source "$HERE/../sec-cooldown.sh"; sec_cooldown_init
    SCRIPT_DIR="$T/scripts"; mkdir -p "$SCRIPT_DIR"
    eval "$(awk -v fn=etl_exit_status '$0 ~ "^"fn"\\(\\) \\{" {p=1} p {print} p && /^}/ {exit}' "$HERE/common.sh")"
    eval "$(awk -v fn="$1" '$0 ~ "^"fn"\\(\\) \\{" {p=1} p {print} p && /^}/ {exit}' "$HERE/common.sh")"
    resolve_classpath() { echo fake.jar; }
    get_heap_config() { _HEAP_MIN=1m; _HEAP_MAX=1m; }
    assert_dq_target() { return 0; }
    GOVDATA_JAVA_BIN="$T/fake-java"; GOVDATA_JAVA_OPTS=""
    export FAKE_MODE="$2" FAKE_MARKER="$MARKER" FAKE_REFUSAL="$REFUSAL"
    "$1" "$3" "worker-sec_secondary-test" >/dev/null 2>&1
    echo $? )
}
for runner in run_etl run_etl_inline; do
  arg='{"inline":"model"}'; [ "$runner" = run_etl ] && { echo '{}' > "$T/model.json"; arg="$T/model.json"; }
  rm -f "$MARKER"
  [ "$(drive "$runner" refuse-and-mark "$arg")" = 75 ] \
    && ok "$runner: marker appears mid-run, refusal -> exit 75" || bad "$runner: marker appears mid-run"
  rm -f "$MARKER"
  [ "$(drive "$runner" refuse-only "$arg")" = 1 ] \
    && ok "$runner: refusal with no active marker stays a failure (1)" || bad "$runner: refusal without marker"
  printf '%s' $(( ($(date +%s) + 600) * 1000 )) > "$MARKER"
  [ "$(drive "$runner" crash "$arg")" = 1 ] \
    && ok "$runner: unrelated crash during an active marker stays a failure (1)" || bad "$runner: crash with marker"
  rm -f "$MARKER"
done
exit $fail
