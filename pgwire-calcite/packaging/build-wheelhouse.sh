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

# pip is the cross-platform wheel fetcher (uv has no `download`); PIP may point at
# any pip-enabled interpreter (e.g. `python3 -m pip`).
PIP="${PIP:-python3 -m pip}"
$PIP --version >/dev/null 2>&1 || { echo "need a pip-enabled python (set PIP=...)"; exit 1; }
[ -f "$LOCK" ] || { echo "missing lockfile $LOCK — run: uv pip compile pyproject.toml -o $LOCK"; exit 1; }

# pip download can't fetch the editable local buenavista; build its wheel first.
BV_OUT="$OUT/_local"
mkdir -p "$BV_OUT"
( command -v uv >/dev/null && uv build "$ROOT/vendor/buenavista" --wheel -o "$BV_OUT" ) || \
  echo "note: build vendor/buenavista wheel separately into each variant"

# pip download --platform tags for the shipped OS/arch variants (PGW-028/031).
declare -A PLATTAGS=(
  ["linux-x86_64"]="manylinux2014_x86_64"
  ["macos-arm64"]="macosx_11_0_arm64"
  ["macos-x86_64"]="macosx_10_9_x86_64"
  ["windows-x86_64"]="win_amd64"
)
PYVER_NODOT="${PY_VERSION//./}"

echo "== wheelhouse: CPython $PY_VERSION, lock $LOCK -> $OUT =="
for entry in "${TARGETS[@]}"; do
  variant="${entry%%:*}"
  dest="$OUT/$variant"
  tag="${PLATTAGS[$variant]}"
  echo "-- $variant ($tag)"
  mkdir -p "$dest"
  cp "$BV_OUT"/*.whl "$dest"/ 2>/dev/null || true
  # --only-binary :all: => wheels only (no source builds on the target); pinned CPython/ABI.
  $PIP download \
    --python-version "$PY_VERSION" \
    --platform "$tag" \
    --implementation cp \
    --abi "cp${PYVER_NODOT}" \
    --only-binary :all: \
    -r "$LOCK" \
    -d "$dest" || echo "!! some wheels missing for $variant (source-only dep?)"
done
rm -rf "$BV_OUT"

echo "== done. Each variant wheelhouse installs offline with:"
echo "   uv pip install --no-index --find-links <variant> -r $LOCK"
