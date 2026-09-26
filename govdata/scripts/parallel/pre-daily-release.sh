#!/usr/bin/env bash
#
# pre-daily-release.sh — run at the start of each daily window: push main, release, build and
# stage the jar, so the daily ETL runs the code that was merged since the last window.
#
#   1. commit   nothing is committed here. Fixes reach main through fix-branch.sh (after DQ);
#               tracked changes still uncommitted in the shared tree are somebody's edit in
#               progress, so they are reported and left out of the release and the jar.
#   2. push     origin main (kenstott/calcite), only as a fast-forward.
#   3. release  engine-v<next patch> on kenstott/calcite when code changed since the last tag.
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

# ── 3. release ───────────────────────────────────────────────────────────────
if $release_ok; then
  last_tag="$(git tag -l 'engine-v*' --sort=-v:refname | head -1)"
  [ -n "$last_tag" ] || { log "ERROR: no engine-v* tag to release after"; exit 1; }
  if git diff --quiet "$last_tag" HEAD -- "${RELEASE_PATHS[@]}"; then
    log "no engine code changed since $last_tag; no release"
  else
    ver="${last_tag#engine-v}"
    next="${ver%.*}.$(( ${ver##*.} + 1 ))"
    if $DRY_RUN; then
      log "would release engine-v$next (code changed since $last_tag)"
    else
      log "releasing engine-v$next on $FORK (code changed since $last_tag)"
      gh release create "engine-v$next" --repo "$FORK" --target "$head_sha" \
        --title "AskAmerica Engine v$next" --generate-notes
    fi
  fi
fi

# ── 4. build ─────────────────────────────────────────────────────────────────
stamp="$STAGE_DIR/sih-govdata.jar.commit"
dest="$STAGE_DIR/sih-govdata.jar"

# The DuckDB catalog seed bundled in the jar goes stale when a schema YAML changes without a
# reseed; workers then keep serving the old view definitions.
seed_commit="$(git log -1 --format=%H -- govdata/src/main/resources/duckdb/seed/govdata-seed.zip)"
stale_yaml=""
if [ -n "$seed_commit" ]; then
  stale_yaml="$(git diff --name-only "$seed_commit" HEAD -- 'govdata/src/main/resources/*-schema.yaml' 'govdata/src/main/resources/**/*-schema.yaml' | sort -u)"
fi
if [ -n "$stale_yaml" ]; then
  log "ERROR: govdata-seed.zip predates changes to these schema YAMLs; views built from them serve the old definitions until build-seed.sh is run and its output committed:"
  echo "$stale_yaml" | sed 's/^/    /'
fi

if [ -f "$dest" ] && [ -f "$stamp" ] && [ "$(cat "$stamp")" = "$head_sha" ]; then
  log "the staged jar is already built from $head_sha"
  exit 0
fi
if $DRY_RUN; then
  log "would build shadowJar from $head_sha and stage it at $dest"
  exit 0
fi

wt="$(mktemp -d "${TMPDIR:-/tmp}/pre-daily-build.XXXXXX")"
cleanup() { git -C "$REPO_ROOT" worktree remove --force "$wt" >/dev/null 2>&1 || rm -rf "$wt"; }
trap cleanup EXIT
git worktree add --detach --quiet "$wt" "$head_sha"
log "building shadowJar from $head_sha in $wt"
(cd "$wt" && ./gradlew :govdata:shadowJar --console=plain -q)

built="$(find "$wt/govdata/build/libs" -maxdepth 1 -name 'sih-govdata-*-SNAPSHOT.jar' | head -1)"
[ -n "$built" ] || { log "ERROR: the build produced no sih-govdata-*-SNAPSHOT.jar"; exit 1; }
unzip -tq "$built" >/dev/null || { log "ERROR: $built is not a valid jar"; exit 1; }

mkdir -p "$STAGE_DIR"
tmp="$dest.new.$$"
cp "$built" "$tmp"
cmp -s "$built" "$tmp" || { rm -f "$tmp"; log "ERROR: staged copy differs from the build"; exit 1; }
mv -f "$tmp" "$dest"
echo "$head_sha" > "$stamp"
log "staged $dest from $head_sha ($(du -h "$dest" | cut -f1))"
