#!/usr/bin/env bash
#
# pin-jar.sh <jar> — print the path of an immutable, content-addressed copy of <jar>.
#
# Every ETL/DQ process runs from such a copy rather than from the shared or a private jar path, so
# staging or overwriting the original never reaches a running job: its JVMs keep the jar they
# opened, and any worker it launches later still finds the same bytes. The copy is made once per
# (name, mtime, size) and shared by every process that pins that version; an already-pinned path
# is returned unchanged. Old copies are collected by pre-daily-release.sh once nothing uses them.
set -euo pipefail

[ $# -eq 1 ] || { echo "Usage: $0 <jar>" >&2; exit 2; }
src="$1"
[ -f "$src" ] || { echo "ERROR: jar not found: $src" >&2; exit 1; }

SNAP_DIR="${GOVDATA_JAR_SNAP_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/.jars}"
case "$src" in "$SNAP_DIR"/*) echo "$src"; exit 0 ;; esac

snap="$SNAP_DIR/$(basename "$src" .jar)-$(stat -c '%Y-%s' "$src").jar"
if [ ! -f "$snap" ]; then
  mkdir -p "$SNAP_DIR"
  tmp="$snap.tmp.$$"
  cp "$src" "$tmp"
  cmp -s "$src" "$tmp" || { rm -f "$tmp"; echo "ERROR: $src changed while it was being pinned" >&2; exit 1; }
  mv -f "$tmp" "$snap"
fi
echo "$snap"
