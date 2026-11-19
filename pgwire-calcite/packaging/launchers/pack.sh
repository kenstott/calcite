#!/usr/bin/env bash
# Copyright (c) 2026 Kenneth Stott
# Licensed under the Business Source License 1.1 (see LICENSE, NOTICE).
#
# pgwire-@ADAPTER@ pack launcher (packs/@ADAPTER@/@VERSION@/bin/pgwire-@ADAPTER@). The pack holds
# the adapter's jars and model; the runtime it requires, @RUNTIME@, is installed beside it under
# the same root (runtimes/@RUNTIME@). The caller's arguments follow the pack's flags and win.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNTIME="$(cd "$HERE/../../.." && pwd)/runtimes/@RUNTIME@"
if [ ! -f "$RUNTIME/runtime.json" ]; then
  echo "pgwire-@ADAPTER@ @VERSION@ requires pgwire-calcite runtime @RUNTIME@, which is not installed (expected at $RUNTIME)." >&2
  exit 2
fi
# What the server writes goes to the state directory, which a caller sets per server process.
export PGWIRE_CALCITE_STATE_DIR="${PGWIRE_CALCITE_STATE_DIR:-${XDG_STATE_HOME:-$HOME/.local/state}/pgwire-@ADAPTER@}"
exec "$RUNTIME/bin/pgwire-calcite-runtime" --pack "$HERE" "$@"
