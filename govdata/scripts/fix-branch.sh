#!/usr/bin/env bash
# Feature-branch workflow for autonomous fixes (defect / sourcing runners).
#
#   fix-branch.sh start <slug>       create branch fix/<slug> in its own worktree, print the path
#   fix-branch.sh dq-passed <slug>   record that DQ passed for the branch's current change
#   fix-branch.sh merge <slug>       rebase on main and fast-forward main to the branch
#
# All edits, builds and DQ runs for a fix happen in the worktree, so nothing unvalidated ever
# reaches the shared working tree. `merge` refuses unless the change that DQ validated is
# byte-for-byte the change being merged (compared by patch-id, which survives a rebase).
# In guard/enforce mode (.claude/lead/mode) `merge` is also lead-only and needs CI green for the exact
# commit being merged: the branch must be pushed, every check run on its tip must have succeeded, and
# the rebase onto main must leave the commit unchanged (otherwise push and let CI run again). In
# shadow mode each of these only warns.
# A fix whose DQ does not pass is simply never merged: the branch and worktree stay as the
# resume point.
set -euo pipefail

usage() { echo "Usage: $0 start|dq-passed|merge <slug>" >&2; exit 2; }
[ $# -eq 2 ] || usage
cmd="$1"; slug="$2"
case "$slug" in
  ''|*[!A-Za-z0-9._-]*) echo "slug must match [A-Za-z0-9._-]+" >&2; exit 2 ;;
esac

MAIN_TREE="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && git rev-parse --show-toplevel)"
BRANCH="fix/$slug"
WT="$(dirname "$MAIN_TREE")/$(basename "$MAIN_TREE")-fixes/$slug"
MARK_DIR="$(git -C "$MAIN_TREE" rev-parse --git-common-dir)/fix-branch-dq"
[[ "$MARK_DIR" = /* ]] || MARK_DIR="$MAIN_TREE/$MARK_DIR"

patch_id() {
  local base
  base="$(git -C "$WT" merge-base main HEAD)"
  git -C "$WT" diff "$base" HEAD | git patch-id --stable | cut -d' ' -f1
}

require_worktree() {
  [ -d "$WT" ] || { echo "no worktree for '$slug' (run: $0 start $slug)" >&2; exit 1; }
  if [ -n "$(git -C "$WT" status --porcelain --untracked-files=no)" ]; then
    echo "worktree $WT has uncommitted changes; commit them on $BRANCH first" >&2
    exit 1
  fi
}

lead_mode() { cat "$MAIN_TREE/.claude/lead/mode" 2>/dev/null || echo shadow; }
gate_fail() {
  if [ "$(lead_mode)" = shadow ]; then echo "WARN (shadow mode): $1" >&2; else echo "$1" >&2; exit 1; fi
}

# CI must be green for the exact tip being merged.
require_ci_green() {
  local sha remote_sha repo runs bad
  sha="$(git -C "$WT" rev-parse HEAD)"
  remote_sha="$(git -C "$WT" ls-remote origin "refs/heads/$BRANCH" | cut -f1)"
  [ "$remote_sha" = "$sha" ] || { gate_fail "$BRANCH is not pushed at $sha (remote: ${remote_sha:-none}); push it so CI runs"; return 0; }
  repo="$(git -C "$MAIN_TREE" remote get-url origin | sed -E 's#.*github\.com[:/]##; s#\.git$##')"
  runs="$(gh api "repos/$repo/commits/$sha/check-runs" --jq '.check_runs[] | "\(.name)\t\(.conclusion // "pending")"')" \
    || { gate_fail "could not read CI check runs for $sha"; return 0; }
  # A push to fix/<slug> does not start the main CI (its branch filter '*' skips names with a slash);
  # a pull request does. Require the real CI job, not just whichever light check ran on the push.
  printf '%s\n' "$runs" | grep -Eq "^${FIX_BRANCH_REQUIRED_CHECK:-Linux \(JDK 21\)}" \
    || { gate_fail "the required CI check is not on $sha; open a pull request for $BRANCH so CI runs"; return 0; }
  bad="$(printf '%s\n' "$runs" | awk -F'\t' '$2 != "success" && $2 != "skipped" && $2 != "neutral" {print $1 ": " $2}')"
  [ -z "$bad" ] || { gate_fail "CI is not green on $sha: $(echo "$bad" | head -3 | tr '\n' ';')"; return 0; }
  echo "$sha"
}

case "$cmd" in
  start)
    if [ ! -d "$WT" ]; then
      mkdir -p "$(dirname "$WT")"
      git -C "$MAIN_TREE" worktree add -b "$BRANCH" "$WT" main >&2
    fi
    # Credential files are untracked, so a fresh worktree lacks them.
    for f in .env.prod .env.dq; do
      [ -e "$WT/govdata/$f" ] || { [ -e "$MAIN_TREE/govdata/$f" ] && ln -s "$MAIN_TREE/govdata/$f" "$WT/govdata/$f"; } || true
    done
    echo "$WT"
    ;;
  dq-passed)
    require_worktree
    pid="$(patch_id)"
    [ -n "$pid" ] || { echo "branch $BRANCH has no change relative to main" >&2; exit 1; }
    mkdir -p "$MARK_DIR"
    echo "$pid" > "$MARK_DIR/$slug"
    echo "recorded DQ pass for $BRANCH (patch-id $pid)"
    ;;
  merge)
    require_worktree
    [ -f "$MARK_DIR/$slug" ] || { echo "DQ pass not recorded for $BRANCH; run DQ then: $0 dq-passed $slug" >&2; exit 1; }
    [ -z "${CLAUDE_ROLE:-}" ] || gate_fail "merge is the lead's step (CLAUDE_ROLE=$CLAUDE_ROLE)"
    ci_sha="$(require_ci_green)"
    if ! git -C "$WT" rebase main >&2; then
      git -C "$WT" rebase --abort >&2 || true
      echo "rebase of $BRANCH onto main conflicts; resolve it in $WT, re-run DQ, then dq-passed again" >&2
      exit 1
    fi
    if [ -n "$ci_sha" ] && [ "$(git -C "$WT" rev-parse HEAD)" != "$ci_sha" ]; then
      gate_fail "the rebase onto main changed the commit CI validated; push $BRANCH and wait for CI again"
    fi
    if [ "$(patch_id)" != "$(cat "$MARK_DIR/$slug")" ]; then
      echo "the change on $BRANCH differs from the one DQ validated; re-run DQ, then: $0 dq-passed $slug" >&2
      exit 1
    fi
    [ "$(git -C "$MAIN_TREE" symbolic-ref --short HEAD)" = main ] \
      || { echo "$MAIN_TREE is not on main" >&2; exit 1; }
    git -C "$MAIN_TREE" merge --ff-only "$BRANCH" >&2
    sha="$(git -C "$MAIN_TREE" rev-parse --short HEAD)"
    git -C "$MAIN_TREE" worktree remove "$WT" >&2
    git -C "$MAIN_TREE" branch -d "$BRANCH" >&2
    rm -f "$MARK_DIR/$slug"
    echo "merged $BRANCH into main at $sha"
    ;;
  *) usage ;;
esac
