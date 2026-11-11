#!/usr/bin/env bash
# The unit pass of sync-to-r2.sh may use a table's metadata/ directory as the copy source, which
# puts version-hint.text at the ROOT of that source. The exclude must hold at every depth.
set -euo pipefail
d=$(mktemp -d); trap 'rm -rf "$d"' EXIT
mkdir -p "$d/table/metadata"
touch "$d/table/version-hint.text" "$d/table/metadata/version-hint.text" "$d/table/metadata/v1.metadata.json"
fail=0
for src in "$d/table" "$d/table/metadata"; do
  if rclone lsf -R "$src" --exclude "version-hint.text" | grep -q 'version-hint.text'; then
    echo "FAIL: version-hint.text not excluded for source $src"; fail=1
  fi
done
[ "$fail" -eq 0 ] && echo "PASS: version-hint.text excluded at every depth"
exit "$fail"
