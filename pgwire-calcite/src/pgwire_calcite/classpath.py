# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Resolve the Calcite JVM runtime classpath for the JPype backend.

Phase 1 loads the classpath from (in order): an explicit list, the
``PGWIRE_CALCITE_CLASSPATH`` env var (os.pathsep-separated), or a file named by
``PGWIRE_CALCITE_CLASSPATH_FILE`` / the default ``.calcite-classpath.txt`` beside
the project root. The file is produced from the Calcite build's runtime
classpath (a Gradle init task; see scripts/print-calcite-classpath.gradle).

There is no silent fallback (CLAUDE.md rule 6): if no classpath can be resolved,
this raises with the exact remediation, rather than starting a JVM that will fail
opaquely later. Phase 6 replaces this with a bundled, pinned wheelhouse+jars set.
"""

from __future__ import annotations

import os
import pathlib
from typing import List, Optional

_DEFAULT_FILE = "calcite-classpath.txt"


def _project_root() -> pathlib.Path:
    # src/pgwire_calcite/classpath.py -> project root is two parents up from src.
    return pathlib.Path(__file__).resolve().parents[2]


class ClasspathError(RuntimeError):
    """Raised when the Calcite classpath cannot be resolved. Never swallowed."""


def resolve_classpath(explicit: Optional[List[str]] = None) -> List[str]:
    if explicit:
        return _validate(list(explicit), source="explicit argument")

    env = os.environ.get("PGWIRE_CALCITE_CLASSPATH")
    if env:
        return _validate(env.split(os.pathsep), source="PGWIRE_CALCITE_CLASSPATH")

    file_env = os.environ.get("PGWIRE_CALCITE_CLASSPATH_FILE")
    candidates = []
    if file_env:
        candidates.append(pathlib.Path(file_env))
    candidates.append(_project_root() / _DEFAULT_FILE)
    candidates.append(_project_root() / ("." + _DEFAULT_FILE))

    for path in candidates:
        if path.is_file():
            text = path.read_text(encoding="utf-8").strip()
            if not text:
                continue
            entries = [e for e in text.replace("\n", os.pathsep).split(os.pathsep) if e.strip()]
            return _validate(entries, source=str(path))

    raise ClasspathError(
        "No Calcite classpath resolved. Set PGWIRE_CALCITE_CLASSPATH, or "
        "PGWIRE_CALCITE_CLASSPATH_FILE, or place a classpath file at "
        f"{_project_root() / _DEFAULT_FILE}. Generate one with the Gradle init task "
        "in scripts/print-calcite-classpath.gradle."
    )


def _validate(entries: List[str], source: str) -> List[str]:
    missing = [e for e in entries if e and not pathlib.Path(e).exists()]
    if missing:
        raise ClasspathError(
            f"Classpath from {source} references {len(missing)} missing jar(s); "
            f"first missing: {missing[0]}. Rebuild Calcite or regenerate the classpath file."
        )
    if not entries:
        raise ClasspathError(f"Classpath from {source} is empty.")
    return entries
