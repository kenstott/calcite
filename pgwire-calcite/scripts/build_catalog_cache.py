# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Walk an adapter's catalog live and write the catalog cache a release ships beside its model.

The cache is keyed by model.json and the engine release (catalog_populate.cache_key), so it is
built by the release's own engine, from the release's own jars, never committed. Run with the
classpath and state directory the server itself would have:

    PGWIRE_CALCITE_CLASSPATH_DIR=<jars> PGWIRE_CALCITE_STATE_DIR=<out> \
        PYTHONPATH=pgwire-calcite/src python3 build_catalog_cache.py <model.json>

Prints the written file's path. Exits non-zero when the walk was incomplete: build_and_cache_
context writes only a complete walk, and a release must not ship without its cache.
"""

from __future__ import annotations

import logging
import os
import sys

from pgwire_calcite.calcite_backend import CalciteBackend
from pgwire_calcite.catalog_populate import build_and_cache_context, catalog_cache_write_path


def main(model_path: str) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    backend = CalciteBackend(model_path=model_path)
    ctx, _ = build_and_cache_context(backend.connection, model_path)
    written = catalog_cache_write_path(model_path)
    if not os.path.isfile(written):
        print(f"the catalog walk was incomplete; no cache was written at {written}", file=sys.stderr)
        return 1
    print(f"{written} ({len(ctx.tables)} tables)")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(f"usage: {sys.argv[0]} <model.json>")
    raise SystemExit(main(sys.argv[1]))
