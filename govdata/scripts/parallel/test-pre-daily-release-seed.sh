#!/usr/bin/env bash
# Tests pre-daily-release.sh's seed step in a scratch repository with stubbed gradle, gh and
# build-seed.sh: nothing is rebuilt, committed or released for the seed unless build-seed.sh says a
# schema model changed; when it does, the seed is regenerated, committed, pushed, and the release
# and the staged jar carry the new commit; a failed rebuild does not stop the release.
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
G
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
run() { (cd "$tmp/w" && PATH="$tmp/bin:$PATH" bash govdata/scripts/parallel/pre-daily-release.sh) > "$tmp/out.log" 2>&1; echo $? > "$tmp/rc"; }

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
grep -q -- "--target $head1" "$tmp/gh.log" && ok "changed model: release targets the seed commit" || bad "changed model: release not at seed commit"
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
exit $fail
