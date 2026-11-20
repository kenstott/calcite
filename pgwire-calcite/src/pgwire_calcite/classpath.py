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
``PGWIRE_CALCITE_CLASSPATH`` env var (os.pathsep-separated), the jar directories named by
``PGWIRE_CALCITE_CLASSPATH_DIRS`` (os.pathsep-separated; every ``*.jar`` in each, in order: a
runtime's jars, then its pack's, see packs.py), or a file named by
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
import re

_DEFAULT_FILE = "calcite-classpath.txt"
CLASSPATH_DIRS_ENV = "PGWIRE_CALCITE_CLASSPATH_DIRS"
#: name-version.jar; the same reading as packaging/runtime_packs.py, which refuses the same clash
#: when a pack is built (that script runs on the release runner without this package).
_JAR_VERSION_RE = re.compile(r"^(.+)-([0-9][0-9A-Za-z_.]*)\.jar$")


def _project_root() -> pathlib.Path:
    # src/pgwire_calcite/classpath.py -> project root is two parents up from src.
    return pathlib.Path(__file__).resolve().parents[2]


class ClasspathError(RuntimeError):
    """Raised when the Calcite classpath cannot be resolved. Never swallowed."""


def _vendored_jars() -> list[str]:
    """Jars bundled with pgwire-calcite (e.g. arrow-jdbc, absent from the Calcite
    build). Always prepended so they are present for airgapped installs (Phase 6)."""
    jar_dir = _project_root() / "vendor" / "jars"
    if not jar_dir.is_dir():
        return []
    return [str(p) for p in sorted(jar_dir.glob("*.jar"))]


def _artifact(name: str) -> str:
    m = _JAR_VERSION_RE.match(name)
    return m.group(1) if m else name[: -len(".jar")]


def jars_in_dirs(dirs: list[str]) -> list[str]:
    """Every ``*.jar`` in each directory, sorted within it, directories in the order given. A jar in
    two directories, by name or as two versions of one artifact, is refused: a pack never carries a
    jar its runtime already has, and a newer runtime on the pack's line may have gained one."""
    jars: list[str] = []
    seen: dict = {}
    for d in dirs:
        jar_dir = pathlib.Path(d)
        if not jar_dir.is_dir():
            raise ClasspathError(f"{CLASSPATH_DIRS_ENV} names {d}, which is not a directory.")
        here: dict = {}
        for p in sorted(jar_dir.glob("*.jar")):
            artifact = _artifact(p.name)
            if artifact in seen:
                other, other_dir = seen[artifact]
                raise ClasspathError(
                    f"{p.name} in {jar_dir} and {other} in {other_dir} are one jar; a jar belongs "
                    "to one directory."
                )
            here[artifact] = (p.name, jar_dir)
            jars.append(str(p))
        seen.update(here)
    return jars


def resolve_classpath(explicit: list[str] | None = None) -> list[str]:
    vendored = _vendored_jars()
    if explicit:
        return _validate(vendored + list(explicit), source="explicit argument")

    env = os.environ.get("PGWIRE_CALCITE_CLASSPATH")
    if env:
        return _validate(vendored + env.split(os.pathsep), source="PGWIRE_CALCITE_CLASSPATH")

    # A runtime and its pack name their jar directories: the classpath is read from them at
    # start, so nothing is written into the install tree (it may be read-only).
    dirs_env = os.environ.get(CLASSPATH_DIRS_ENV)
    if dirs_env:
        return _validate(vendored + jars_in_dirs(dirs_env.split(os.pathsep)),
                         source=f"{CLASSPATH_DIRS_ENV} ({dirs_env})")

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
            return _validate(vendored + entries, source=str(path))

    raise ClasspathError(
        "No Calcite classpath resolved. Set PGWIRE_CALCITE_CLASSPATH, or "
        f"{CLASSPATH_DIRS_ENV}, or "
        "PGWIRE_CALCITE_CLASSPATH_FILE, or place a classpath file at "
        f"{_project_root() / _DEFAULT_FILE}. Generate one with the Gradle init task "
        "in scripts/print-calcite-classpath.gradle."
    )


def _validate(entries: list[str], source: str) -> list[str]:
    missing = [e for e in entries if e and not pathlib.Path(e).exists()]
    if missing:
        raise ClasspathError(
            f"Classpath from {source} references {len(missing)} missing jar(s); "
            f"first missing: {missing[0]}. Rebuild Calcite or regenerate the classpath file."
        )
    if not entries:
        raise ClasspathError(f"Classpath from {source} is empty.")
    return entries
