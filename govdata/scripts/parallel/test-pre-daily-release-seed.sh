#!/usr/bin/env bash
# Phase 1 (no flag) never seeds or releases; phase 2 (--release) does. Tests pre-daily-release.sh's seed step in a scratch repository with stubbed gradle, gh and
# build-seed.sh: nothing is rebuilt, committed or released for the seed unless build-seed.sh says a
# schema model changed; when it does, the seed is regenerated, committed, pushed, and the release
# and the staged jar carry the new commit; a failed rebuild does not stop the release. The CI gate
# (stub gh run list) lets the release and the staged jar through only for a completed/success run
# of the code commit; any other state, or an unreadable one, releases and stages nothing and exits 1.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
fail=0
ok() { echo "PASS $1"; }
bad() { echo "FAIL $1"; fail=1; }

setup() { # fresh scratch repo + bare origin
  rm -rf "$tmp/w" "$tmp/origin.git" "$tmp/bin"
  git init -q --bare "$tmp/origin.git"
  mkdir -p "$tmp/w/govdata/scripts/parallel" "$tmp/w/govdata/src/main/resources/duckdb/seed" "$tmp/bin"
  cd "$tmp/w"; git init -q -b main .; git config user.email t@t; git config user.name t
  cp "$HERE/pre-daily-release.sh" govdata/scripts/parallel/
  cat > gradlew <<G
#!/usr/bin/env bash
echo "\$*" >> "$tmp/gradle-args.log"
mkdir -p govdata/build/libs
python3 -c "import zipfile;zipfile.ZipFile('govdata/build/libs/sih-govdata-1-SNAPSHOT.jar','w').writestr('x','y')"
G
  chmod +x gradlew
  echo old > govdata/src/main/resources/duckdb/seed/govdata-seed.zip
  git add -A; git commit -qm base; git remote add origin "$tmp/origin.git"; git push -q origin main
  git tag engine-v0.1.0
  cat > "$tmp/bin/gh" <<G
#!/usr/bin/env bash
echo "\$*" >> "$tmp/gh.log"
if [ "\${1:-} \${2:-}" = "run list" ]; then
  [ -e "$tmp/gh-fail" ] && { echo "HTTP 502" >&2; exit 1; }
  cat "$tmp/gh-runs.json"
fi
G
  echo '[{"status":"completed","conclusion":"success","url":"http://ci/1","createdAt":"2026-10-31T00:00:00Z"}]' > "$tmp/gh-runs.json"
  rm -f "$tmp/gh-fail"
  chmod +x "$tmp/bin/gh"; : > "$tmp/gh.log"
  git push -q origin --tags
}
stub_seed() { # check-rc, build-rc
  cat > "$tmp/w/govdata/scripts/build-seed.sh" <<S
#!/usr/bin/env bash
if [ "\${1:-}" = "--check" ]; then echo "stub: check"; exit $1; fi
echo "built with \$MODEL_VERIFY_JAR" >> "$tmp/seed-builds.log"
[ $2 -eq 0 ] && echo "new \$(date +%s%N)" > "$tmp/w/govdata/src/main/resources/duckdb/seed/govdata-seed.zip"
exit $2
S
  chmod +x "$tmp/w/govdata/scripts/build-seed.sh"
  (cd "$tmp/w" && git add -A govdata/scripts && git commit -qm "stub seed" && git push -q origin main)
  : > "$tmp/seed-builds.log"
}
run() { (cd "$tmp/w" && CI_WAIT_SECS="${CI_WAIT_SECS:-0}" CI_NONE_GRACE_SECS="${CI_NONE_GRACE_SECS:-0}" CI_POLL_SECS=1 PATH="$tmp/bin:$PATH" bash govdata/scripts/parallel/pre-daily-release.sh ${PHASE_ARGS---release}) > "$tmp/out.log" 2>&1; echo $? > "$tmp/rc"; }
run1() { PHASE_ARGS="" run; }

# 1. model unchanged: no seed build, no seed commit
setup; stub_seed 10 0; head0=$(git -C "$tmp/w" rev-parse HEAD); run
[ ! -s "$tmp/seed-builds.log" ] && ok "unchanged model: seed not built" || bad "unchanged model: seed built"
[ "$(git -C "$tmp/w" rev-parse HEAD)" = "$head0" ] && ok "unchanged model: no commit" || bad "unchanged model: commit made"
grep -q "INFO: seed not rebuilt" "$tmp/out.log" && ok "unchanged model: reported as INFO" || bad "unchanged model: no INFO line"

# 2. model changed: seed built, committed, pushed; release + staged jar at the new commit
setup; stub_seed 0 0; head0=$(git -C "$tmp/w" rev-parse HEAD); run
head1=$(git -C "$tmp/w" rev-parse HEAD)
[ "$head1" != "$head0" ] && ok "changed model: seed committed" || bad "changed model: no commit ($(cat "$tmp/rc")): $(tail -5 "$tmp/out.log")"
[ "$(git -C "$tmp/origin.git" rev-parse main)" = "$head1" ] && ok "changed model: pushed" || bad "changed model: not pushed"
grep -q "seedgen" "$tmp/seed-builds.log" && ok "changed model: seed generated from a fresh jar" || bad "changed model: wrong jar"
[ "$(cat "$tmp/w/govdata/build/libs/sih-govdata.jar.commit" 2>/dev/null)" = "$head1" ] && ok "changed model: staged jar built from the seed commit" || bad "changed model: stale staged jar"
[ "$(git -C "$tmp/origin.git" rev-parse 'engine-v0.1.1^{commit}' 2>/dev/null)" = "$head1" ] && ok "changed model: release targets the seed commit" || bad "changed model: release not at seed commit"
ls "$tmp/w/govdata/build/libs" | grep -q seedgen && bad "changed model: seedgen jar left behind" || ok "changed model: seedgen jar cleaned up"
[ -s "$tmp/gradle-args.log" ] && ! grep -qv -- "--no-daemon" "$tmp/gradle-args.log" \
  && ok "every build runs with --no-daemon" || bad "a build ran without --no-daemon: $(cat "$tmp/gradle-args.log")"

# 3. rebuild fails: reported, release continues on the committed seed
setup; stub_seed 0 1; head0=$(git -C "$tmp/w" rev-parse HEAD); run
grep -q "ERROR: build-seed.sh failed" "$tmp/out.log" && ok "failed rebuild: reported" || bad "failed rebuild: not reported"
[ "$(git -C "$tmp/w" rev-parse HEAD)" = "$head0" ] && ok "failed rebuild: nothing committed" || bad "failed rebuild: commit made"
[ "$(cat "$tmp/w/govdata/build/libs/sih-govdata.jar.commit" 2>/dev/null)" = "$head0" ] && ok "failed rebuild: jar still staged" || bad "failed rebuild: no staged jar"

# 4. --build-only skips the seed step entirely
setup; stub_seed 0 0; head0=$(git -C "$tmp/w" rev-parse HEAD)
(cd "$tmp/w" && PATH="$tmp/bin:$PATH" bash govdata/scripts/parallel/pre-daily-release.sh --build-only) > "$tmp/out.log" 2>&1
[ ! -s "$tmp/seed-builds.log" ] && ok "--build-only: seed step skipped" || bad "--build-only: seed built"

# 5. CI gate: release and staging only for a completed/success run of the code commit
ci_case() { # name, runs-json ('' = gh error)
  setup; stub_seed 10 0
  if [ -n "$2" ]; then echo "$2" > "$tmp/gh-runs.json"; else touch "$tmp/gh-fail"; fi
  : > "$tmp/gh.log"; run
  if [ "$(cat "$tmp/rc")" -ne 0 ] \
     && ! grep -q "release create" "$tmp/gh.log" \
     && [ ! -e "$tmp/w/govdata/build/libs/sih-govdata.jar" ] \
     && grep -q "CI gate" "$tmp/out.log"; then ok "CI $1: nothing released or staged, exit non-zero"
  else bad "CI $1: rc=$(cat "$tmp/rc") $(tail -3 "$tmp/out.log")"; fi
}
ci_case failure '[{"status":"completed","conclusion":"failure","url":"http://ci/2","createdAt":"2026-10-31T00:00:00Z"}]'
ci_case in-progress '[{"status":"in_progress","conclusion":"","url":"http://ci/3","createdAt":"2026-10-31T00:00:00Z"}]'
ci_case cancelled '[{"status":"completed","conclusion":"cancelled","url":"http://ci/4","createdAt":"2026-10-31T00:00:00Z"}]'
ci_case missing '[]'
ci_case gh-error ''
grep -q "HTTP 502" "$tmp/out.log" && ok "CI gh-error: reason says the CI could not be read" || bad "CI gh-error: reason missing"

# the newest run wins over an older failed one; success releases and stages
setup; stub_seed 10 0
echo '[{"status":"completed","conclusion":"failure","url":"http://ci/old","createdAt":"2026-10-30T00:00:00Z"},{"status":"completed","conclusion":"success","url":"http://ci/new","createdAt":"2026-10-31T00:00:00Z"}]' > "$tmp/gh-runs.json"
mkdir -p "$tmp/w/govdata/src/main/java"; echo x > "$tmp/w/govdata/src/main/java/A.java"
(cd "$tmp/w" && git add -A && git commit -qm "code change" && git push -q origin main)
code=$(git -C "$tmp/w" rev-parse HEAD); : > "$tmp/gh.log"; run
[ "$(cat "$tmp/rc")" -eq 0 ] && grep -q "release create engine-v0.1.1" "$tmp/gh.log" \
  && [ "$(cat "$tmp/w/govdata/build/libs/sih-govdata.jar.commit" 2>/dev/null)" = "$code" ] \
  && ok "CI success: released and staged" || bad "CI success: rc=$(cat "$tmp/rc") $(tail -3 "$tmp/out.log")"
grep -q -- "--commit $code" "$tmp/gh.log" && ok "CI success: gate looked up the head commit" || bad "CI success: wrong commit looked up"

# a generated seed commit on top: the gate judges the head, since the code commit's own run was
# cancelled by the seed push or never started
setup; stub_seed 0 0; : > "$tmp/gh.log"; run
head1=$(git -C "$tmp/w" rev-parse HEAD)
grep -q -- "--commit $head1" "$tmp/gh.log" && ! grep -q -- "--commit $(git -C "$tmp/w" rev-parse HEAD~1)" "$tmp/gh.log" \
  && ok "seed commit on top: CI judged on the head" || bad "seed commit on top: $(grep 'run list' "$tmp/gh.log")"
[ "$(cat "$tmp/rc")" -eq 0 ] && [ "$(cat "$tmp/w/govdata/build/libs/sih-govdata.jar.commit" 2>/dev/null)" = "$head1" ] \
  && ok "seed commit on top: green head releases and stages" || bad "seed commit on top: rc=$(cat "$tmp/rc") $(tail -3 "$tmp/out.log")"

# the code commit's own run is cancelled, the head's is green: passes (stub answers per sha)
setup; stub_seed 0 0
cat > "$tmp/bin/gh" <<G
#!/usr/bin/env bash
if [ "\${1:-} \${2:-}" = "run list" ]; then
  sha=""; while [ \$# -gt 0 ]; do [ "\$1" = --commit ] && sha="\$2"; shift; done
  if [ "\$sha" = "\$(git -C "$tmp/w" rev-parse HEAD)" ]; then cat "$tmp/gh-runs.json"
  else echo '[{"status":"completed","conclusion":"cancelled","url":"http://ci/c","createdAt":"2026-10-31T00:00:00Z"}]'; fi
fi
G
run
[ "$(cat "$tmp/rc")" -eq 0 ] && ok "code commit cancelled by the seed push, head green: gate passes" || bad "cancelled code commit: rc=$(cat "$tmp/rc") $(tail -3 "$tmp/out.log")"

# head in progress, then green: the gate waits
setup; stub_seed 10 0
echo '[{"status":"in_progress","conclusion":"","url":"http://ci/w","createdAt":"2026-10-31T00:00:00Z"}]' > "$tmp/gh-runs.json"
( sleep 3; echo '[{"status":"completed","conclusion":"success","url":"http://ci/w","createdAt":"2026-10-31T00:00:00Z"}]' > "$tmp/gh-runs.json" ) &
CI_WAIT_SECS=30 run; wait
[ "$(cat "$tmp/rc")" -eq 0 ] && grep -q "waiting" "$tmp/out.log" && ok "head in progress then green: waited and passed" || bad "wait: rc=$(cat "$tmp/rc") $(tail -3 "$tmp/out.log")"

# head stays in progress past the bound: fails naming the wait
setup; stub_seed 10 0
echo '[{"status":"in_progress","conclusion":"","url":"http://ci/w","createdAt":"2026-10-31T00:00:00Z"}]' > "$tmp/gh-runs.json"
CI_WAIT_SECS=2 run
[ "$(cat "$tmp/rc")" -ne 0 ] && grep -q "after waiting" "$tmp/out.log" && [ ! -e "$tmp/w/govdata/build/libs/sih-govdata.jar" ] \
  && ok "head in progress past the bound: fails, nothing staged" || bad "wait bound: rc=$(cat "$tmp/rc") $(tail -3 "$tmp/out.log")"

# --dry-run prints the decision and changes nothing
setup; stub_seed 10 0; echo '[]' > "$tmp/gh-runs.json"
(cd "$tmp/w" && PATH="$tmp/bin:$PATH" bash govdata/scripts/parallel/pre-daily-release.sh --release --dry-run) > "$tmp/out.log" 2>&1
grep -q "would not release or stage a new jar (CI gate)" "$tmp/out.log" && [ ! -e "$tmp/w/govdata/build/libs/sih-govdata.jar" ] \
  && ok "--dry-run: gate decision printed" || bad "--dry-run: $(tail -3 "$tmp/out.log")"

# generated commit created after the push (the coverage daemon, mid-run), above a pushed green code
# commit: the gate judges the pushed commit and passes
setup; stub_seed 10 0
pushed=$(git -C "$tmp/w" rev-parse HEAD)
cat > "$tmp/bin/gh" <<G
#!/usr/bin/env bash
echo "\$*" >> "$tmp/gh.log"
if [ "\${1:-} \${2:-}" = "run list" ]; then
  if [ ! -e "$tmp/chore-made" ]; then
    touch "$tmp/chore-made"
    (cd "$tmp/w" && echo x > cov.txt && git add cov.txt && git commit -qm "chore(govdata): refresh observed-coverage metadata for weather")
  fi
  cat "$tmp/gh-runs.json"
fi
G
rm -f "$tmp/chore-made"
run
local_head=$(git -C "$tmp/w" rev-parse HEAD)
[ "$local_head" != "$pushed" ] && grep -q -- "--commit $pushed" "$tmp/gh.log" && ! grep -q -- "--commit $local_head" "$tmp/gh.log" && grep -q "CI gate: CI green for $pushed" "$tmp/out.log" \
  && ok "unpushed chore above pushed green code commit: gate passes on the pushed commit" || bad "unpushed chore: rc=$(cat "$tmp/rc") $(tail -3 "$tmp/out.log")"

# gate refusal: distinct exit status and a stale-jar message
setup; stub_seed 10 0; echo '[{"status":"completed","conclusion":"failure","url":"http://ci/f","createdAt":"2026-10-31T00:00:00Z"}]' > "$tmp/gh-runs.json"
run
[ "$(cat "$tmp/rc")" -eq 3 ] && grep -q "shared jar is STALE" "$tmp/out.log" && ok "gate refusal: exit 3 and stale-jar message" || bad "refusal: rc=$(cat "$tmp/rc") $(tail -3 "$tmp/out.log")"

# ── phase 1: push, gate, build; no seed, no release ──
setup; stub_seed 0 0
mkdir -p "$tmp/w/govdata/src/main/java"; echo x > "$tmp/w/govdata/src/main/java/A.java"
(cd "$tmp/w" && git add -A && git commit -qm "code change"); head0=$(git -C "$tmp/w" rev-parse HEAD); : > "$tmp/gh.log"
run1
[ "$(cat "$tmp/rc")" -eq 0 ] && [ ! -s "$tmp/seed-builds.log" ] && [ "$(git -C "$tmp/w" rev-parse HEAD)" = "$head0" ] \
  && ok "phase 1: model changed but no seed built or committed" || bad "phase 1 seed: rc=$(cat "$tmp/rc") $(tail -3 "$tmp/out.log")"
[ "$(git -C "$tmp/origin.git" rev-parse main)" = "$head0" ] && ok "phase 1: pushed" || bad "phase 1: not pushed"
[ "$(cat "$tmp/w/govdata/build/libs/sih-govdata.jar.commit" 2>/dev/null)" = "$head0" ] && ok "phase 1: jar staged at the pushed head" || bad "phase 1: no staged jar"
! grep -q "release create" "$tmp/gh.log" && ! git -C "$tmp/origin.git" rev-parse engine-v0.1.1 >/dev/null 2>&1 \
  && ok "phase 1: nothing released" || bad "phase 1: released"

# phase 1 with a refused gate stages nothing and exits 3 with the stale message
setup; stub_seed 0 0; echo '[{"status":"completed","conclusion":"failure","url":"http://ci/f","createdAt":"2026-10-31T00:00:00Z"}]' > "$tmp/gh-runs.json"
run1
[ "$(cat "$tmp/rc")" -eq 3 ] && [ ! -e "$tmp/w/govdata/build/libs/sih-govdata.jar" ] && grep -q "shared jar is STALE" "$tmp/out.log" \
  && ok "phase 1: gate refusal stages nothing, exit 3" || bad "phase 1 gate: rc=$(cat "$tmp/rc")"

# phase 2 after phase 1 on the same head: the staged jar generates the seed; the release is still cut, once
setup; stub_seed 0 0
mkdir -p "$tmp/w/govdata/src/main/java"; echo x > "$tmp/w/govdata/src/main/java/A.java"
(cd "$tmp/w" && git add -A && git commit -qm "code change"); run1
: > "$tmp/gh.log"; run
grep -q "generating the seed with the jar staged" "$tmp/out.log" && ! grep -q seedgen "$tmp/seed-builds.log" \
  && ok "phase 2: seed generated with the jar phase 1 staged" || bad "phase 2 jar reuse: $(tail -5 "$tmp/out.log")"
git -C "$tmp/origin.git" rev-parse 'engine-v0.1.1^{commit}' >/dev/null 2>&1 && ok "phase 2: release cut though phase 1 had staged a jar" || bad "phase 2: no release"
stub_seed 10 0; run; ! grep -q "releasing engine-v0.1.2" "$tmp/out.log" && ok "phase 2 rerun: no second release" || bad "phase 2 rerun released again"

# phase 2: a seed that fails for tables with no data is reported by table and the release continues
setup; stub_seed 0 1
sed -i 's|^\[ 1 -eq 0 \]|echo "FAILED econ.state_ui_claims: no columns" >\&2\n[ 1 -eq 0 ]|' "$tmp/w/govdata/scripts/build-seed.sh"
mkdir -p "$tmp/w/govdata/src/main/java"; echo y > "$tmp/w/govdata/src/main/java/B.java"
(cd "$tmp/w" && git add -A && git commit -qm "code change" -q)
run
grep -q "FAILED econ.state_ui_claims" "$tmp/out.log" && grep -q "tables the seed could not be built for" "$tmp/out.log" \
  && git -C "$tmp/origin.git" rev-parse 'engine-v0.1.1' >/dev/null 2>&1 \
  && ok "phase 2: failed tables named, release not stopped" || bad "phase 2 failed tables: rc=$(cat "$tmp/rc") $(tail -6 "$tmp/out.log")"

# the flags are exclusive
(cd "$tmp/w" && bash govdata/scripts/parallel/pre-daily-release.sh --build-only --release) >/dev/null 2>&1
[ $? -eq 2 ] && ok "--build-only with --release is refused" || bad "--build-only --release accepted"
exit $fail
