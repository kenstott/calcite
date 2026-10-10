#!/usr/bin/env bash
#
# pre-daily-release.sh — run at the start of each daily window: push main, release, build and
# stage the jar, so the daily ETL runs the code that was merged since the last window.
#
#   1. commit   nothing is committed here. Fixes reach main through fix-branch.sh (after DQ);
#               tracked changes still uncommitted in the shared tree are somebody's edit in
#               progress, so they are reported and left out of the release and the jar.
#   2. push     origin main (kenstott/calcite), only as a fast-forward.
#   2b. seed    build-seed.sh --check says whether a schema MODEL changed since the committed DuckDB
#               seed (exit 10: no, and nothing below happens). If so: build a jar from HEAD,
#               regenerate the seed with it, commit the seed files and push, so the release and
#               the jar below carry the new seed. A failed seed rebuild is reported and the
#               release continues on the committed seed.
#   3. release  engine-v<next patch> on kenstott/calcite when code changed since the last tag
#               AND we're about to build a fresh jar for this head_sha (step 4's own gate) —
#               a release always accompanies the build it corresponds to, not a wall-clock timer.
#   4. build    shadowJar from a clean checkout of HEAD, staged over the shared sih-govdata.jar
#               by atomic rename (JVMs that already loaded classes from the old file keep them).
#
# Usage: pre-daily-release.sh [--dry-run] [--build-only]
#   --dry-run     print what would be pushed, released and built; change nothing
#   --build-only  skip push and release (used to test the build and staging)
# Env: STAGE_DIR overrides the directory the jar is staged into (default govdata/build/libs).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GOVDATA_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
REPO_ROOT="$(cd "$GOVDATA_ROOT/.." && pwd)"
FORK="kenstott/calcite"
STAGE_DIR="${STAGE_DIR:-$GOVDATA_ROOT/build/libs}"
# Paths whose changes need a new engine release.
RELEASE_PATHS=(govdata/src file/src core/src linq4j/src askamerica-engine/src driver-base/src)

DRY_RUN=false
BUILD_ONLY=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --build-only) BUILD_ONLY=true ;;
    *) echo "Usage: $0 [--dry-run] [--build-only]" >&2; exit 2 ;;
  esac
done

ts() { date '+%Y-%m-%d %H:%M:%S'; }
log() { echo "[$(ts)] pre-daily-release: $*"; }

# build_jar <sha> <out>: shadowJar from a clean checkout of <sha>, validated, copied to <out>.
# Returns non-zero (after reporting) on any failure; the checkout is always removed.
build_jar() {
  local sha="$1" out="$2" wt built rc=0
  wt="$(mktemp -d "${TMPDIR:-/tmp}/pre-daily-build.XXXXXX")"
  git worktree add --detach --quiet "$wt" "$sha" || { rm -rf "$wt"; return 1; }
  log "building shadowJar from $sha in $wt"
  # --no-daemon: a daemon started from a shell in another mount namespace (one that predates the
  # /var/tmp/govdata mount) is reused by default and cannot chdir into a checkout under that path
  # ("could not setcwd()"), which failed the whole build. A one-shot build has no such state.
  if ! (cd "$wt" && ./gradlew --no-daemon :govdata:shadowJar --console=plain -q); then
    log "ERROR: the shadowJar build failed"; rc=1
  else
    built="$(find "$wt/govdata/build/libs" -maxdepth 1 -name 'sih-govdata-*-SNAPSHOT.jar' | head -1)"
    if [ -z "$built" ]; then
      log "ERROR: the build produced no sih-govdata-*-SNAPSHOT.jar"; rc=1
    elif ! unzip -tq "$built" >/dev/null; then
      log "ERROR: $built is not a valid jar"; rc=1
    elif ! cp "$built" "$out" || ! cmp -s "$built" "$out"; then
      log "ERROR: the copy of the build at $out differs from the build"; rc=1
    fi
  fi
  git -C "$REPO_ROOT" worktree remove --force "$wt" >/dev/null 2>&1 || rm -rf "$wt"
  return "$rc"
}

cd "$REPO_ROOT"
exec 9>"$(git rev-parse --git-common-dir)/pre-daily-release.lock"
flock -n 9 || { log "ERROR: another pre-daily-release is running"; exit 1; }

branch="$(git symbolic-ref --short HEAD)"
[ "$branch" = main ] || { log "ERROR: $REPO_ROOT is on '$branch', not main"; exit 1; }
head_sha="$(git rev-parse HEAD)"

# ── 1. commit ────────────────────────────────────────────────────────────────
dirty="$(git status --porcelain --untracked-files=no)"
if [ -n "$dirty" ]; then
  log "WARNING: uncommitted tracked changes are left out of this release and jar:"
  echo "$dirty" | sed 's/^/    /'
fi

# ── 2. push ──────────────────────────────────────────────────────────────────
release_ok=true
if $BUILD_ONLY; then
  release_ok=false
  log "--build-only: skipping push and release"
else
  git fetch --quiet origin main --tags
  if git merge-base --is-ancestor origin/main main; then
    ahead="$(git rev-list --count origin/main..main)"
    if [ "$ahead" -gt 0 ]; then
      if $DRY_RUN; then
        log "would push $ahead commit(s) to origin main"
      else
        log "pushing $ahead commit(s) to origin main"
        git push origin main
        [ "$(git rev-parse origin/main)" = "$head_sha" ] \
          || { log "ERROR: origin/main is not at $head_sha after the push"; exit 1; }
      fi
    else
      log "origin/main is up to date"
    fi
  else
    # origin/main has commits main lacks (pushed from elsewhere): fold them in with a merge
    # commit, which keeps every local sha, so worktree branches rebase onto it unchanged.
    if $DRY_RUN; then
      log "would merge origin/main ($(git rev-list --count main..origin/main) commit(s)) into main, then push"
    elif git merge --no-edit --quiet origin/main; then
      log "merged origin/main into main; pushing"
      git push origin main
      head_sha="$(git rev-parse HEAD)"
      [ "$(git rev-parse origin/main)" = "$head_sha" ] \
        || { log "ERROR: origin/main is not at $head_sha after the push"; exit 1; }
    else
      git merge --abort
      release_ok=false
      log "ERROR: origin/main does not merge cleanly into main; not pushing and not releasing"
    fi
  fi
fi

# ── 2b. seed ─────────────────────────────────────────────────────────────────
# build-seed.sh owns the question "did a schema model change since the committed seed?" (exit 10:
# no). Generated observedCoverage metadata and comments do not count, so on most days nothing here
# builds, commits or releases anything.
SEED_DIR_REL="govdata/src/main/resources/duckdb/seed"
SEED_NOT_NEEDED=10
if $BUILD_ONLY; then
  log "--build-only: skipping the seed check"
else
  seed_rc=0
  seed_note="$("$GOVDATA_ROOT/scripts/build-seed.sh" --check 2>&1)" || seed_rc=$?
  if [ "$seed_rc" -eq "$SEED_NOT_NEEDED" ]; then
    log "INFO: seed not rebuilt: $(tail -1 <<<"$seed_note")"
  elif [ "$seed_rc" -ne 0 ]; then
    log "ERROR: the seed check failed (rc=$seed_rc); continuing on the committed seed:"
    echo "$seed_note" | sed 's/^/    /'
  elif $DRY_RUN; then
    log "would rebuild the seed:"
    echo "$seed_note" | sed 's/^/    /'
  else
    log "seed rebuild needed:"
    echo "$seed_note" | sed 's/^/    /'
    mkdir -p "$STAGE_DIR"
    seed_jar="$STAGE_DIR/sih-govdata-seedgen.$$.jar"
    if ! build_jar "$head_sha" "$seed_jar"; then
      log "ERROR: no jar to generate the seed with; continuing on the committed seed"
    elif ! MODEL_VERIFY_JAR="$seed_jar" timeout 7200 "$GOVDATA_ROOT/scripts/build-seed.sh"; then
      log "ERROR: build-seed.sh failed; continuing on the committed seed"
    elif [ -z "$(git status --porcelain -- "$SEED_DIR_REL")" ]; then
      log "INFO: the rebuilt seed is identical to the committed one; nothing to commit"
    else
      git add -A -- "$SEED_DIR_REL"
      git commit -q -m "chore(govdata): rebuild the DuckDB seed for changed schema models" \
        -m "Auto-generated by pre-daily-release.sh via build-seed.sh." -- "$SEED_DIR_REL"
      head_sha="$(git rev-parse HEAD)"
      log "committed the rebuilt seed as $head_sha"
      if $release_ok; then
        if git push origin main; then
          log "pushed the seed commit"
        else
          release_ok=false
          log "ERROR: pushing the seed commit failed; not releasing"
        fi
      fi
    fi
    rm -f "$seed_jar"
  fi
fi

# ── 3. release ───────────────────────────────────────────────────────────────
# Gated on the same signal step 4 uses to decide whether to build: a real release only
# happens on a window where we're actually building a new jar for this head_sha, so it
# stays tied to genuine new code rather than a wall-clock timer. Every scheduler restart
# starts a new daily window, but a restart with no new commits leaves the stamp matching
# head_sha, so will_build is false and this doesn't re-release — the RELEASE_PATHS diff
# below already prevents releasing when nothing release-relevant changed since the last
# tag; will_build additionally prevents releasing a head_sha whose jar isn't being (re)built
# this run.
stamp="$STAGE_DIR/sih-govdata.jar.commit"
dest="$STAGE_DIR/sih-govdata.jar"
will_build=true
[ -f "$dest" ] && [ -f "$stamp" ] && [ "$(cat "$stamp")" = "$head_sha" ] && will_build=false

if $release_ok; then
  last_tag="$(git tag -l 'engine-v*' --sort=-v:refname | head -1)"
  [ -n "$last_tag" ] || { log "ERROR: no engine-v* tag to release after"; exit 1; }
  if git diff --quiet "$last_tag" HEAD -- "${RELEASE_PATHS[@]}"; then
    log "no engine code changed since $last_tag; no release"
  elif ! $will_build; then
    log "the staged jar is already built from $head_sha; not releasing again for the same commit"
  else
    ver="${last_tag#engine-v}"
    next="${ver%.*}.$(( ${ver##*.} + 1 ))"
    if $DRY_RUN; then
      log "would release engine-v$next (code changed since $last_tag)"
    else
      log "releasing engine-v$next on $FORK (code changed since $last_tag)"
      git tag "engine-v$next" "$head_sha" && git push origin "engine-v$next"
      gh release create "engine-v$next" --repo "$FORK" --verify-tag --draft \
        --title "AskAmerica Engine v$next" --generate-notes
    fi
  fi
fi

# ── pinned jar copies ────────────────────────────────────────────────────────
# Pinned copies (scripts/pin-jar.sh) stay while any process references them, and for a day
# after they were made; older unused ones are removed.
snap_dir="$GOVDATA_ROOT/.jars"
if [ -d "$snap_dir" ] && ! $DRY_RUN; then
  in_use="$(cat /proc/[0-9]*/environ /proc/[0-9]*/cmdline 2>/dev/null | tr '\0' '\n' \
            | grep -aoE "$snap_dir/[^ :]+\.jar" | sort -u || true)"
  while IFS= read -r old_jar; do
    if ! grep -qxF "$old_jar" <<<"$in_use"; then
      log "removing unused pinned jar $(basename "$old_jar")"
      rm -f "$old_jar"
    fi
  done < <(find "$snap_dir" -maxdepth 1 -name '*.jar' -mmin +1440)
fi

# ── 4. build ─────────────────────────────────────────────────────────────────
# stamp/dest/will_build are computed above, ahead of step 3, so the release step can gate on
# the same "are we building fresh for this head_sha" signal.

if ! $will_build; then
  log "the staged jar is already built from $head_sha"
  exit 0
fi
if $DRY_RUN; then
  log "would build shadowJar from $head_sha and stage it at $dest"
  exit 0
fi

mkdir -p "$STAGE_DIR"
tmp="$dest.new.$$"
build_jar "$head_sha" "$tmp" || { rm -f "$tmp"; exit 1; }
mv -f "$tmp" "$dest"
echo "$head_sha" > "$stamp"
log "staged $dest from $head_sha ($(du -h "$dest" | cut -f1))"
