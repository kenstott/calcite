#!/usr/bin/env bash
#
# Stage a retired-artifact banner for pasting: find the most recently saved copy of the
# artifact, inject the banner, and put the result on the clipboard.
#
#   ./stage-retirement-banner.sh register
#   ./stage-retirement-banner.sh ledger
#
# Then open that artifact's editor, select all (Ctrl+A) and paste (Ctrl+V).
#
# This script cannot fetch the artifact itself — only Claude can, via Artifact action:"read",
# which saves the page under the session's tool-results directory. What this does is find the
# newest such copy and warn you how old it is. If nobody has read the artifact recently, or
# the age below looks stale, ask Claude to re-read it before pasting: a copy taken before a
# concurrent session's edit will silently revert that edit.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SEARCH_ROOT="${GOVDATA_ARTIFACT_CACHE:-$HOME/.claude/projects/-home-kstott-calcite}"
STALE_MINUTES="${GOVDATA_ARTIFACT_STALE_MINUTES:-30}"

case "${1:-}" in
  register) ARTIFACT_ID="e5262dc4"; NAME="Govdata Defect Register" ;;
  ledger)   ARTIFACT_ID="4a9d427b"; NAME="Govdata Remediation Ledger" ;;
  ""|-h|--help)
    sed -n '2,16p' "${BASH_SOURCE[0]}" | sed 's/^# \?//'
    exit 0 ;;
  *)
    echo "unknown artifact: $1 (expected 'register' or 'ledger')" >&2
    exit 2 ;;
esac

echo "artifact : $NAME"

# Newest saved copy wins. -printf '%T@ %p' then sort numerically: mtime is what tells us
# which read is most recent, and the filenames carry a version stamp that does not sort.
NEWEST=$(find "$SEARCH_ROOT" -type f -name "artifact-${ARTIFACT_ID}-*.html" -printf '%T@ %p\n' 2>/dev/null \
         | sort -rn | head -1 | cut -d' ' -f2-)

if [ -z "$NEWEST" ]; then
  cat >&2 <<EOF
    x No saved copy of this artifact found under
      $SEARCH_ROOT

      Ask Claude to read it first:
          Artifact action:"read" on the $NAME
      then re-run this script.
EOF
  exit 1
fi

AGE_MIN=$(( ( $(date +%s) - $(stat -c %Y "$NEWEST") ) / 60 ))
echo "source   : $NEWEST"
echo "saved    : $(date -r "$NEWEST" '+%Y-%m-%d %H:%M') (${AGE_MIN}m ago)"

if [ "$AGE_MIN" -gt "$STALE_MINUTES" ]; then
  echo
  echo -e "\033[33m    ! This copy is ${AGE_MIN} minutes old.\033[0m"
  echo "      These artifacts are edited by concurrent sessions. Pasting a stale copy reverts"
  echo "      anything filed since. Ask Claude to re-read the artifact, then re-run this."
  echo
  printf '    Paste an %sm-old copy anyway? [y/N] ' "$AGE_MIN"
  if [ -t 0 ]; then
    read -r reply
  else
    reply=""
    echo "(no terminal — assuming no)"
  fi
  case "$reply" in
    [yY]*) ;;
    *) echo "    stopped — nothing copied to the clipboard"; exit 1 ;;
  esac
fi

echo
"$SCRIPT_DIR/inject-retirement-banner.py" "$NEWEST" \
  --out "/tmp/$(basename "${1}")-bannered.html" --clip
