# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One shared runtime per engine version, one pack per adapter.

The runtime holds what every pgwire adapter needs: CPython with ``pgwire_calcite`` installed, the
JRE, the Java launcher and the jars common to every adapter. A pack holds one adapter's own jars,
its ``model.json`` and, optionally, Python packages only that adapter uses (``site-packages/``).
The pack names the runtime line it is compatible with (an ABI number, ``line`` in runtime.json,
bumped when the runtime's jar set or the pack contract changes); the two are installed under one
root::

    <root>/runtimes/<runtime-version>/runtime.json
                                     jre/ python/ cpython/ jars/ pgwire-launcher.jar
                                     bin/pgwire-calcite-runtime
    <root>/packs/<adapter>/<pack-version>/pack.json
                                          jars/ model/model.json [site-packages/]
                                          bin/pgwire-<adapter>

Each release tarball holds its part of that tree (``runtimes/<v>/...`` or
``packs/<adapter>/<v>/...``), so extracting a runtime and a pack into one directory installs them.

The pack's launcher scans ``<root>/runtimes/`` and starts the newest installed runtime on its line,
so a runtime release ships with the packs unchanged; with none on its line it refuses to start,
naming the line. It then starts the runtime's launcher with ``--pack <pack-dir>``; the Python
launcher calls :func:`prepare` to check the pairing again and to build the classpath (runtime jars
then pack jars), the Python path (the pack's extras) and the server arguments the pack declares.
"""

from __future__ import annotations

import json
import os
import pathlib
import re
import sys
from dataclasses import dataclass

RUNTIME_MANIFEST = "runtime.json"
PACK_MANIFEST = "pack.json"
RUNTIMES_DIR = "runtimes"
PACKS_DIR = "packs"
RUNTIME_NAME = "pgwire-calcite-runtime"
FORMAT = 1

#: Set by the runtime's launcher to the runtime directory it starts from.
RUNTIME_HOME_ENV = "PGWIRE_CALCITE_RUNTIME_HOME"
#: The jar directories the Calcite JVM's classpath is read from (os.pathsep-separated).
CLASSPATH_DIRS_ENV = "PGWIRE_CALCITE_CLASSPATH_DIRS"
EMBED_CACHE_ENV = "EMBED_MODEL_CACHE_DIR"
#: The runtimes directory when a package manager installs the runtime and each pack in separate
#: prefixes (REQ-2254, amended 2026-11-20: one runtime formula plus one formula per pack); the
#: pack formula's wrapper sets it. Unset, the runtimes are <root>/runtimes beside the packs.
RUNTIMES_ENV = "PGWIRE_CALCITE_RUNTIMES"
VERSION_RE = re.compile(r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$")


class PackError(RuntimeError):
    """A runtime or pack that cannot be started. Never swallowed."""


@dataclass(frozen=True)
class Runtime:
    home: pathlib.Path
    version: str
    line: int
    variant: str

    @property
    def jars(self) -> pathlib.Path:
        return self.home / "jars"


@dataclass(frozen=True)
class Pack:
    home: pathlib.Path
    name: str
    version: str
    runtime_line: int
    variant: str
    args: list[str]

    @property
    def jars(self) -> pathlib.Path:
        return self.home / "jars"

    @property
    def model(self) -> pathlib.Path:
        return self.home / "model" / "model.json"

    @property
    def site_packages(self) -> pathlib.Path:
        return self.home / "site-packages"

    @property
    def embedder(self) -> pathlib.Path:
        return self.home / "model" / "embedder"


def _read_manifest(path: pathlib.Path, kind: str) -> dict:
    if not path.is_file():
        raise PackError(f"{path.parent} is not a pgwire {kind}: it has no {path.name}.")
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("format") != FORMAT:
        raise PackError(
            f"{path} has format {data.get('format')!r}; this runtime reads format {FORMAT}."
        )
    return data


def _field(data: dict, key: str, path: pathlib.Path) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value:
        raise PackError(f"{path} names no {key!r}.")
    return value


def _line(data: dict, key: str, path: pathlib.Path) -> int:
    value = data.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise PackError(f"{path} names no runtime line {key!r} (a positive integer).")
    return value


def version_key(version: str) -> tuple[int, int, int]:
    m = VERSION_RE.match(version)
    if not m:
        raise PackError(f"{version!r} is not a runtime version (major.minor.patch).")
    return int(m.group(1)), int(m.group(2)), int(m.group(3))


def load_runtime(home: pathlib.Path) -> Runtime:
    path = home / RUNTIME_MANIFEST
    data = _read_manifest(path, "runtime")
    if data.get("name") != RUNTIME_NAME:
        raise PackError(f"{path} names {data.get('name')!r}, not {RUNTIME_NAME!r}.")
    version = _field(data, "version", path)
    version_key(version)
    return Runtime(home=home, version=version, line=_line(data, "line", path),
                   variant=_field(data, "variant", path))


def load_pack(home: pathlib.Path) -> Pack:
    path = home / PACK_MANIFEST
    data = _read_manifest(path, "pack")
    args = data.get("args")
    if not isinstance(args, list) or not all(isinstance(a, str) for a in args):
        raise PackError(f"{path} has no 'args' list of strings.")
    return Pack(
        home=home,
        name=_field(data, "name", path),
        version=_field(data, "version", path),
        runtime_line=_line(data, "runtime_line", path),
        variant=_field(data, "variant", path),
        args=list(args),
    )


def install_root(pack: Pack) -> pathlib.Path:
    """The directory holding ``runtimes/`` and ``packs/``: ``<root>/packs/<adapter>/<version>``."""
    return pack.home.parents[2]


def runtimes_dir(pack: Pack) -> pathlib.Path:
    named = os.environ.get(RUNTIMES_ENV)
    return pathlib.Path(named).resolve() if named else install_root(pack) / RUNTIMES_DIR


def runtime_home_for(pack: Pack) -> pathlib.Path:
    """The newest installed runtime on the pack's line; refused, naming the line, when none is."""
    root = runtimes_dir(pack)
    on_line = []
    if root.is_dir():
        for manifest in root.glob(f"*/{RUNTIME_MANIFEST}"):
            runtime = load_runtime(manifest.parent)
            if runtime.line == pack.runtime_line:
                on_line.append(runtime)
    if not on_line:
        raise PackError(
            f"pgwire-{pack.name} {pack.version} requires a pgwire-calcite runtime on line "
            f"{pack.runtime_line}, and none is installed under {root}."
        )
    return max(on_line, key=lambda r: version_key(r.version)).home


def check_pairing(runtime: Runtime, pack: Pack) -> None:
    if pack.runtime_line != runtime.line:
        raise PackError(
            f"pgwire-{pack.name} {pack.version} requires a pgwire-calcite runtime on line "
            f"{pack.runtime_line}, but was started by runtime {runtime.version} on line "
            f"{runtime.line} ({runtime.home})."
        )
    if pack.variant != runtime.variant:
        raise PackError(
            f"pgwire-{pack.name} {pack.version} is built for {pack.variant}; runtime "
            f"{runtime.version} at {runtime.home} is {runtime.variant}."
        )
    newest = runtime_home_for(pack)
    if newest != runtime.home:
        raise PackError(
            f"pgwire-{pack.name} {pack.version} starts the newest runtime on line {pack.runtime_line}, "
            f"which is {newest.name} ({newest}), but was started by runtime {runtime.version}."
        )


def classpath_dirs(runtime: Runtime, pack: Pack) -> list[str]:
    """The runtime's jars, then the pack's (a pack with no jars of its own has no directory)."""
    dirs = [str(runtime.jars)]
    if pack.jars.is_dir():
        dirs.append(str(pack.jars))
    return dirs


def server_argv(pack: Pack, rest: list[str]) -> list[str]:
    """The calcite backend over the pack's model, the pack's own flags, then the caller's (which win)."""
    return ["--backend", "calcite", "--model", str(pack.model), *pack.args, *rest]


def prepare(pack_dir: str, rest: list[str]) -> list[str]:
    """Check the pack against the runtime this process runs from, point the process at both, and
    return the server's argv. The runtime's launcher names its directory in RUNTIME_HOME_ENV."""
    runtime_env = os.environ.get(RUNTIME_HOME_ENV)
    if not runtime_env:
        raise PackError(
            f"--pack needs {RUNTIME_HOME_ENV}, which the runtime's launcher "
            f"(bin/pgwire-calcite-runtime) sets; start the pack through it."
        )
    runtime = load_runtime(pathlib.Path(runtime_env).resolve())
    pack = load_pack(pathlib.Path(pack_dir).resolve())
    check_pairing(runtime, pack)

    os.environ[CLASSPATH_DIRS_ENV] = os.pathsep.join(classpath_dirs(runtime, pack))
    os.environ[EMBED_CACHE_ENV] = str(pack.embedder)
    if pack.site_packages.is_dir():
        extras = str(pack.site_packages)
        sys.path.insert(0, extras)
        current = os.environ.get("PYTHONPATH")
        os.environ["PYTHONPATH"] = extras if not current else os.pathsep.join([extras, current])
    return server_argv(pack, rest)
