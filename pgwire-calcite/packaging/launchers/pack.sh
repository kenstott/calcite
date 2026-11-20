#!/usr/bin/env bash
# Copyright (c) 2026 Kenneth Stott
# Licensed under the Business Source License 1.1 (see LICENSE, NOTICE).
#
# pgwire-@ADAPTER@ pack launcher (packs/@ADAPTER@/@VERSION@/bin/pgwire-@ADAPTER@). The pack holds
# the adapter's jars and model; it starts the newest pgwire-calcite runtime installed on runtime
# line @LINE@ under runtimes/ beside the packs (layout: pgwire_calcite/packs.py). The caller's
# arguments follow the pack's flags and win.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# A package manager that installs the runtime in its own prefix names its runtimes directory
# (REQ-2254, amended 2026-11-20: one runtime formula plus one formula per pack).
RUNTIMES="${PGWIRE_CALCITE_RUNTIMES:-$(cd "$HERE/../../.." && pwd)/runtimes}"
newest=""
for manifest in "$RUNTIMES"/*/runtime.json; do
  [ -f "$manifest" ] || continue
  grep -qF '"line": @LINE@,' "$manifest" || continue
  newest="$( { [ -z "$newest" ] || echo "$newest"; basename "$(dirname "$manifest")"; } \
    | sort -t. -k1,1n -k2,2n -k3,3n | tail -n 1)"
done
if [ -z "$newest" ]; then
  echo "pgwire-@ADAPTER@ @VERSION@ requires a pgwire-calcite runtime on line @LINE@, and none is installed under $RUNTIMES." >&2
  exit 2
fi
# What the server writes goes to the state directory, which a caller sets per server process.
export PGWIRE_CALCITE_STATE_DIR="${PGWIRE_CALCITE_STATE_DIR:-${XDG_STATE_HOME:-$HOME/.local/state}/pgwire-@ADAPTER@}"
exec "$RUNTIMES/$newest/bin/pgwire-calcite-runtime" --pack "$HERE" "$@"
