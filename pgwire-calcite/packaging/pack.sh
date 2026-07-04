#!/usr/bin/env bash
# Copyright (c) 2026 Kenneth Stott
# Licensed under the Business Source License 1.1 (see ../LICENSE, ../NOTICE).
#
# Assemble a per-OS/arch airgap tarball (PGW-030, simplified delivery).
#
# No OS code-signing: this is a CLI/driver tool delivered via package managers
# (Homebrew/Scoop) or `curl | sh`, which are NOT Gatekeeper/SmartScreen quarantined
# (see packaging/README.md, PGW-031 revised). Signed DMG/MSI is an optional add-on.
#
# Inputs are staged into a bundle dir by the build machine; this packs + hashes it.
set -euo pipefail

VARIANT="${1:?usage: pack.sh <variant> <bundle-dir> [out-dir]}"   # e.g. macos-arm64
BUNDLE="${2:?bundle dir with jre/ cpython/ wheelhouse/ jars/ natives/ model/ bin/}"
OUT="${3:-dist}"
VERSION="${PGWIRE_VERSION:-0.0.0}"

for d in jre cpython wheelhouse jars bin; do
  [ -d "$BUNDLE/$d" ] || { echo "missing $BUNDLE/$d"; exit 1; }
done

mkdir -p "$OUT"
name="pgwire-calcite-${VERSION}-${VARIANT}"
tar="$OUT/${name}.tar.gz"
echo "== packing $tar =="
tar -czf "$tar" -C "$(dirname "$BUNDLE")" "$(basename "$BUNDLE")"

# sha256 for the Homebrew formula / Scoop manifest.
if command -v sha256sum >/dev/null; then
  sha256sum "$tar" | awk '{print $1}' > "$tar.sha256"
else
  shasum -a 256 "$tar" | awk '{print $1}' > "$tar.sha256"
fi
echo "sha256: $(cat "$tar.sha256")"
echo "== done: $tar"
