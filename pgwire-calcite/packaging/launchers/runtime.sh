#!/usr/bin/env bash
# Copyright (c) 2026 Kenneth Stott
# Licensed under the Business Source License 1.1 (see LICENSE, NOTICE).
#
# pgwire-calcite runtime launcher (runtimes/<version>/bin/pgwire-calcite-runtime). A pack's
# launcher starts it with `--pack <pack-dir>`; it runs the server from this runtime's JRE and
# CPython. Nothing is written under the install tree while the server runs.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PGWIRE_CALCITE_RUNTIME_HOME="$HERE"
# The Calcite JVM starts from the runtime's JRE, so the host needs neither a JDK nor JAVA_HOME.
export PGWIRE_CALCITE_JAVA_HOME="$HERE/jre"
exec "$HERE/jre/bin/java" \
  "-Dpgwire.python=$HERE/cpython/bin/python" \
  -jar "$HERE/pgwire-launcher.jar" "$@"
