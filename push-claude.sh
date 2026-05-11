#!/usr/bin/env bash
# push-claude.sh — Commit any changes in .claude/ with a timestamp message and push.

set -euo pipefail

if [[ ! -d ".claude/.git" ]]; then
  echo "ERROR: .claude/ is not a git repository. Run ./setup.sh first." >&2
  exit 1
fi

cd .claude

if git diff --quiet && git diff --cached --quiet && [[ -z "$(git status --porcelain)" ]]; then
  echo "No changes in .claude/ — nothing to commit."
  exit 0
fi

TIMESTAMP="$(date -u +"%Y-%m-%d %H:%M:%S UTC")"
git add -A
git commit -m "chore: sync claude config — $TIMESTAMP"
git push

echo "Done. .claude/ pushed to remote."
