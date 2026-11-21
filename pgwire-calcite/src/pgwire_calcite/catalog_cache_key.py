# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The catalog cache's key: model.json and the engine release that walked it.

Standard library only: the release's pack step computes the key with a runner's bare python3,
which has none of the server's dependencies.
"""

from __future__ import annotations

import hashlib
import os

#: The engine release a runtime belongs to, written into its installed package by the runtime
#: build (pgwire-calcite-release.yml, build-bundle.sh). A checkout has no stamp; its engine
#: release is the repo's version.txt, the one release-please keeps.
ENGINE_VERSION_STAMP = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ENGINE_VERSION")
_CHECKOUT_VERSION = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), os.pardir, os.pardir, os.pardir, "version.txt"
)


def engine_version() -> str:
    """The engine release this server belongs to (see ENGINE_VERSION_STAMP)."""
    path = ENGINE_VERSION_STAMP if os.path.isfile(ENGINE_VERSION_STAMP) else _CHECKOUT_VERSION
    with open(path, encoding="utf-8") as f:
        return f.read().strip()


def cache_key(model_path: str, version: str) -> str:
    """A catalog cache's key: the model AND the engine release that walked it. The catalog's
    column types come from the engine as much as from model.json -- a fix that serves a column
    as a new type changes them with model.json untouched -- so a cache another engine wrote
    must never match."""
    digest = hashlib.sha256()
    with open(model_path, "rb") as f:
        digest.update(f.read())
    digest.update(b"\0engine=" + version.encode("utf-8"))
    return digest.hexdigest()[:16]
