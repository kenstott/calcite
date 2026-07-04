#!/usr/bin/env bash
# Copyright (c) 2026 Kenneth Stott
# Licensed under the Business Source License 1.1 (see ../LICENSE, ../NOTICE).
#
# Build the pgvector distance UDF jar (PGW-047) into vendor/jars/, which the
# classpath resolver auto-includes. Target Java 17 to match the pinned Calcite
# JVM (class file version 61) — a newer -target fails with UnsupportedClassVersionError.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="$HERE/udf"
OUT="$HERE/../vendor/jars/pgwire-vecdist.jar"
REL="${PGWIRE_UDF_RELEASE:-17}"

command -v javac >/dev/null || { echo "javac (JDK) required"; exit 1; }
rm -rf "$SRC/classes" && mkdir -p "$SRC/classes"
javac --release "$REL" -d "$SRC/classes" "$SRC"/pgwire/vec/*.java
jar cf "$OUT" -C "$SRC/classes" .
echo "built $OUT (Java $REL)"
