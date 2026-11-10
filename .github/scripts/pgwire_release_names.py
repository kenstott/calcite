#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE-BSL.txt file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The pgwire asset names an engine release must carry, one per line (REQ-294).

  pgwire_release_names.py tarballs <version>   every pgwire-<name>-<version>-<variant>.tar.gz
  pgwire_release_names.py central <version>    every pgwire Maven Central bundle zip

The adapters come from pgwire-calcite/packaging/adapters.json and the platforms from
pgwire-calcite/packaging/platforms.json, the tables pgwire-adapters-release.yml builds from; an
entry with "since" counts from that release on. release-assets-complete.sh and central-bundles.sh
read this, so the release, its completeness check and the Maven Central promote name the same set.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

PACKAGING = Path(__file__).resolve().parents[2] / "pgwire-calcite" / "packaging"
#: The first release whose pgwire-adapters run lays out and attaches the Maven Central bundles.
CENTRAL_SINCE = "0.128.0"


def _key(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split("."))


def _current(entries: list[dict], version: str) -> list[dict]:
    return [e for e in entries if "since" not in e or _key(version) >= _key(e["since"])]


def adapters(version: str) -> list[str]:
    return [a["name"] for a in _current(json.loads((PACKAGING / "adapters.json").read_text()), version)]


def variants(version: str) -> list[str]:
    return [p["variant"] for p in _current(json.loads((PACKAGING / "platforms.json").read_text()), version)]


def tarballs(version: str) -> list[str]:
    """The pgwire-calcite server and every adapter bundle, for every platform."""
    return [
        f"pgwire-{name}-{version}-{variant}.tar.gz"
        for name in ["calcite", *adapters(version)]
        for variant in variants(version)
    ]


def central(version: str) -> list[str]:
    """Per platform: each adapter's Central bundle and the platform's pgwire-shared bundle."""
    if _key(version) < _key(CENTRAL_SINCE):
        return []
    return [
        f"{artifact}-{variant}-{version}-central-bundle.zip"
        for variant in variants(version)
        for artifact in ["pgwire-shared", *(f"pgwire-{name}" for name in adapters(version))]
    ]


def main(argv: list[str]) -> int:
    if len(argv) != 3 or argv[1] not in ("tarballs", "central"):
        print(__doc__, file=sys.stderr)
        return 2
    names = tarballs(argv[2]) if argv[1] == "tarballs" else central(argv[2])
    for name in names:
        print(name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
