#!/usr/bin/env bash
# setup.sh — Clone the private Claude configuration repo into .claude/ if not already present.

set -euo pipefail

REPO="git@github.com:kenstott/calcite-private.git"
TARGET=".claude"

if [[ -d "$TARGET/.git" ]]; then
  echo ".claude/ already initialised — skipping clone."
  echo "Run ./pull-claude.sh to fetch the latest changes."
  exit 0
fi

if [[ -d "$TARGET" ]] && [[ -n "$(ls -A "$TARGET" 2>/dev/null)" ]]; then
  echo "ERROR: $TARGET exists and is non-empty but is not a git repository." >&2
  echo "Remove or rename it before running setup.sh." >&2
  exit 1
fi

echo "Cloning $REPO into $TARGET/ ..."
git clone "$REPO" "$TARGET"
echo "Done. Claude configuration is ready in $TARGET/."
