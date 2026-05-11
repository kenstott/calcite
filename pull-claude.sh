#!/usr/bin/env bash
# pull-claude.sh — Pull latest changes for the main repo and the .claude nested repo.

set -euo pipefail

echo "==> Pulling main repo..."
git pull

echo ""
echo "==> Pulling .claude/ repo..."
if [[ ! -d ".claude/.git" ]]; then
  echo "ERROR: .claude/ is not a git repository. Run ./setup.sh first." >&2
  exit 1
fi
git -C .claude pull

echo ""
echo "Done."
