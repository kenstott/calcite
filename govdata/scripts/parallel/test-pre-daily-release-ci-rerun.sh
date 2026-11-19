#!/usr/bin/env bash
# Tests pre-daily-release.sh's rerun-once-on-setup-failure CI gate in a scratch repository with a
# stubbed gh. A run whose failed jobs died resolving a plugin is rerun exactly once and waited for;
# any other failure, a second red, a failing rerun or log read, and --dry-run refuse without a rerun
# (or without a second one). A rerun that GitHub still reports as the old failed attempt is waited
# for, within a bound.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
fail=0
ok() { echo "PASS $1"; }
bad() { echo "FAIL $1"; fail=1; }

FAILED='{"status":"completed","conclusion":"failure","url":"http://ci/runs/101","createdAt":"2026-11-18T00:00:00Z"}'
GREEN='{"status":"completed","conclusion":"success","url":"http://ci/runs/101","createdAt":"2026-11-18T00:00:00Z"}'
RUNNING='{"status":"in_progress","conclusion":"","url":"http://ci/runs/101","createdAt":"2026-11-18T00:00:00Z"}'
SETUP_LOG="Plugin [id: 'com.github.autostyle', version: '3.0'] was not found: could not resolve plugin artifact 'com.github.autostyle:com.github.autostyle.gradle.plugin:3.0'"

setup() { # fresh scratch repo + bare origin; gh answers run list from a queue of JSON files
  rm -rf "$tmp/w" "$tmp/origin.git" "$tmp/bin" "$tmp/q"
  git init -q --bare "$tmp/origin.git"
  mkdir -p "$tmp/w/govdata/scripts/parallel" "$tmp/w/govdata/src/main/java" "$tmp/bin" "$tmp/q"
  cd "$tmp/w"; git init -q -b main .; git config user.email t@t; git config user.name t
  cp "$HERE/pre-daily-release.sh" govdata/scripts/parallel/
  cat > gradlew <<G
#!/usr/bin/env bash
mkdir -p govdata/build/libs
python3 -c "import zipfile;zipfile.ZipFile('govdata/build/libs/sih-govdata-1-SNAPSHOT.jar','w').writestr('x','y')"
G
  chmod +x gradlew
  echo x > govdata/src/main/java/A.java
  git add -A; git commit -qm base; git remote add origin "$tmp/origin.git"; git push -q origin main
  git tag engine-v0.1.0; git push -q origin --tags
  : > "$tmp/gh.log"; rm -f "$tmp/rerun-fail" "$tmp/log-fail"
  echo "$SETUP_LOG" > "$tmp/failed.log"
  # run list pops the first queued file while more than one remains, then keeps returning the last
  cat > "$tmp/bin/gh" <<G
#!/usr/bin/env bash
echo "\$*" >> "$tmp/gh.log"
case "\${1:-} \${2:-}" in
  "run list")
    f=\$(ls "$tmp/q" | head -1); n=\$(ls "$tmp/q" | wc -l)
    cat "$tmp/q/\$f"; [ "\$n" -gt 1 ] && rm "$tmp/q/\$f"; exit 0 ;;
  "run view") [ -e "$tmp/log-fail" ] && { echo "HTTP 500" >&2; exit 1; }; cat "$tmp/failed.log" ;;
  "run rerun") [ -e "$tmp/rerun-fail" ] && { echo "HTTP 403" >&2; exit 1; }; exit 0 ;;
esac
G
  chmod +x "$tmp/bin/gh"
}
queue() { local i=0 j; rm -f "$tmp/q"/*; for j in "$@"; do i=$((i+1)); echo "[$j]" > "$tmp/q/$(printf %02d $i)"; done; }
run() { (cd "$tmp/w" && CI_WAIT_SECS=20 CI_NONE_GRACE_SECS=0 CI_POLL_SECS=1 CI_RERUN_SETTLE_SECS=1 CI_RERUN_START_SECS="${START:-3}" \
  PATH="$tmp/bin:$PATH" bash govdata/scripts/parallel/pre-daily-release.sh "$@") > "$tmp/out.log" 2>&1; echo $? > "$tmp/rc"; }
reruns() { grep -c "^run rerun" "$tmp/gh.log"; }
staged() { [ -e "$tmp/w/govdata/build/libs/sih-govdata.jar" ]; }

# 1. setup-only failure, rerun goes green: rerun exactly once, gate passes, jar staged
setup; queue "$FAILED" "$RUNNING" "$GREEN"; run --build-only
[ "$(cat "$tmp/rc")" -eq 0 ] && [ "$(reruns)" -eq 1 ] && staged && grep -q "reran the failed jobs once" "$tmp/out.log" \
  && ok "setup failure: rerun once, green, staged" || bad "setup failure green: rc=$(cat "$tmp/rc") reruns=$(reruns) $(tail -4 "$tmp/out.log")"

# 2. compile/test failure: not rerun, refuses
setup; echo "error: cannot find symbol" > "$tmp/failed.log"; queue "$FAILED"; run --build-only
[ "$(cat "$tmp/rc")" -ne 0 ] && [ "$(reruns)" -eq 0 ] && ! staged && grep -q "CI not green" "$tmp/out.log" \
  && ok "code failure: no rerun, refused" || bad "code failure: rc=$(cat "$tmp/rc") reruns=$(reruns)"

# 3. second red after the one rerun: refuses, no second rerun
setup; queue "$FAILED" "$RUNNING" "$FAILED"; run --build-only
[ "$(cat "$tmp/rc")" -ne 0 ] && [ "$(reruns)" -eq 1 ] && ! staged && grep -q "CI not green" "$tmp/out.log" \
  && ok "second red: refused after one rerun" || bad "second red: rc=$(cat "$tmp/rc") reruns=$(reruns) $(tail -3 "$tmp/out.log")"

# 4. gh run rerun fails: refuses with the normal reason and an ERROR line
setup; touch "$tmp/rerun-fail"; queue "$FAILED"; run --build-only
[ "$(cat "$tmp/rc")" -ne 0 ] && ! staged && grep -q "ERROR: CI gate: gh run rerun failed" "$tmp/out.log" && grep -q "CI not green" "$tmp/out.log" \
  && ok "rerun fails: logged and refused" || bad "rerun fails: rc=$(cat "$tmp/rc") $(tail -3 "$tmp/out.log")"

# 5. gh run view --log-failed fails: refuses, no rerun
setup; touch "$tmp/log-fail"; queue "$FAILED"; run --build-only
[ "$(cat "$tmp/rc")" -ne 0 ] && [ "$(reruns)" -eq 0 ] && ! staged && grep -q "CI not green" "$tmp/out.log" \
  && ok "log read fails: no rerun, refused" || bad "log read fails: rc=$(cat "$tmp/rc") reruns=$(reruns)"

# 6. --dry-run never reruns
setup; queue "$FAILED"; run --dry-run
[ "$(reruns)" -eq 0 ] && ! staged && ok "--dry-run: no rerun" || bad "--dry-run: reruns=$(reruns)"

# 7. state race: GitHub still reports the old failed attempt for a while after the rerun, then runs and goes green
setup; queue "$FAILED" "$FAILED" "$FAILED" "$RUNNING" "$GREEN"; run --build-only
[ "$(cat "$tmp/rc")" -eq 0 ] && [ "$(reruns)" -eq 1 ] && staged && grep -q "still reports the failed attempt" "$tmp/out.log" \
  && ok "stale attempt after rerun: gate waits, then green" || bad "stale attempt: rc=$(cat "$tmp/rc") reruns=$(reruns) $(tail -5 "$tmp/out.log")"

# 8. the rerun never starts: bounded wait, then refuses with its own reason
setup; queue "$FAILED"; START=2 run --build-only
[ "$(cat "$tmp/rc")" -ne 0 ] && [ "$(reruns)" -eq 1 ] && ! staged && grep -q "did not start within 2s" "$tmp/out.log" \
  && ok "rerun never starts: bounded wait, refused" || bad "rerun never starts: rc=$(cat "$tmp/rc") $(tail -3 "$tmp/out.log")"

exit $fail
