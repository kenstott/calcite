#!/usr/bin/env bash
# Copyright (c) 2026 Kenneth Stott
# Licensed under the Business Source License 1.1 (see ../LICENSE, ../NOTICE).
#
# Build the OS-agnostic Java launcher jar (PGW-025) -> packaging/pgwire-launcher.jar.
# Run: java -Dpgwire.python=<cpython> -jar pgwire-launcher.jar <server args>
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="$HERE/launcher"
OUT="$HERE/pgwire-launcher.jar"
REL="${PGWIRE_UDF_RELEASE:-17}"

command -v javac >/dev/null || { echo "javac (JDK) required"; exit 1; }
rm -rf "$SRC/classes" && mkdir -p "$SRC/classes"
javac --release "$REL" -d "$SRC/classes" "$SRC/pgwire/launch/Launcher.java"
jar --create --file "$OUT" --main-class pgwire.launch.Launcher -C "$SRC/classes" .
echo "built $OUT (Java $REL)"
