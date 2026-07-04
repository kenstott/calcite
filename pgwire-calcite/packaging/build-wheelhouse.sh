#!/usr/bin/env bash
# Copyright (c) 2026 Kenneth Stott
# Licensed under the Business Source License 1.1 (see ../LICENSE, ../NOTICE).
#
# Build-time cross-platform wheelhouse fetch (PGW-028/030/031).
#
# Downloads platform wheels for every target OS/arch into per-variant wheelhouses,
# pinned to ONE CPython version/ABI, from a lockfile — for a fully airgapped,
# reproducible first-run install (PGW-027/029: no network at install time).
#
# This runs at BUILD time (needs network); the resulting wheelhouses are bundled
# into the per-OS installers. Signing/notarization is a separate, credentialed
# step — see packaging/README.md.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
PY_VERSION="${PGWIRE_PY_VERSION:-3.12}"
OUT="${1:-$ROOT/dist/wheelhouse}"
LOCK="${PGWIRE_LOCK:-$HERE/requirements.lock}"

# uv target triples for the 3–4 shipped OS/arch variants (PGW-030).
TARGETS=(
  "linux-x86_64:x86_64-manylinux2014"
  "macos-arm64:aarch64-apple-darwin"
  "macos-x86_64:x86_64-apple-darwin"
  "windows-x86_64:x86_64-pc-windows-msvc"
)

command -v uv >/dev/null || { echo "uv is required (https://astral.sh/uv)"; exit 1; }
[ -f "$LOCK" ] || { echo "missing lockfile $LOCK — run: uv pip compile pyproject.toml -o $LOCK"; exit 1; }

echo "== wheelhouse: CPython $PY_VERSION, lock $LOCK -> $OUT =="
for entry in "${TARGETS[@]}"; do
  variant="${entry%%:*}"; platform="${entry##*:}"
  dest="$OUT/$variant"
  echo "-- $variant ($platform)"
  mkdir -p "$dest"
  # --only-binary :all: => wheels only (no source builds on the target); pinned CPython.
  uv pip download \
    --python-version "$PY_VERSION" \
    --python-platform "$platform" \
    --only-binary :all: \
    -r "$LOCK" \
    -d "$dest"
done

echo "== done. Each variant wheelhouse installs offline with:"
echo "   uv pip install --no-index --find-links <variant> -r $LOCK"
