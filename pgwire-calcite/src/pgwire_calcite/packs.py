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
The pack names the runtime version it requires; the two are installed under one root::

    <root>/runtimes/<runtime-version>/runtime.json
                                     jre/ python/ cpython/ jars/ pgwire-launcher.jar
                                     bin/pgwire-calcite-runtime
    <root>/packs/<adapter>/<pack-version>/pack.json
                                          jars/ model/model.json [site-packages/]
                                          bin/pgwire-<adapter>

Each release tarball holds its part of that tree (``runtimes/<v>/...`` or
``packs/<adapter>/<v>/...``), so extracting a runtime and a pack into one directory installs them.

The pack's launcher finds ``<root>/runtimes/<required>`` and refuses to start, naming the version,
when it is absent. It then starts the runtime's launcher with ``--pack <pack-dir>``; the Python
launcher calls :func:`prepare` to check the pairing again and to build the classpath (runtime jars
then pack jars), the Python path (the pack's extras) and the server arguments the pack declares.
"""

from __future__ import annotations

import json
import os
import pathlib
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


class PackError(RuntimeError):
    """A runtime or pack that cannot be started. Never swallowed."""


@dataclass(frozen=True)
class Runtime:
    home: pathlib.Path
    version: str
    variant: str

    @property
    def jars(self) -> pathlib.Path:
        return self.home / "jars"


@dataclass(frozen=True)
class Pack:
    home: pathlib.Path
    name: str
    version: str
    runtime: str
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


def load_runtime(home: pathlib.Path) -> Runtime:
    path = home / RUNTIME_MANIFEST
    data = _read_manifest(path, "runtime")
    if data.get("name") != RUNTIME_NAME:
        raise PackError(f"{path} names {data.get('name')!r}, not {RUNTIME_NAME!r}.")
    return Runtime(home=home, version=_field(data, "version", path), variant=_field(data, "variant", path))


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
        runtime=_field(data, "runtime", path),
        variant=_field(data, "variant", path),
        args=list(args),
    )


def install_root(pack: Pack) -> pathlib.Path:
    """The directory holding ``runtimes/`` and ``packs/``: ``<root>/packs/<adapter>/<version>``."""
    return pack.home.parents[2]


def runtime_home_for(pack: Pack) -> pathlib.Path:
    """The runtime directory the pack requires; refused by version when it is not installed."""
    home = install_root(pack) / RUNTIMES_DIR / pack.runtime
    if not (home / RUNTIME_MANIFEST).is_file():
        raise PackError(
            f"pgwire-{pack.name} {pack.version} requires pgwire-calcite runtime {pack.runtime}, "
            f"which is not installed (expected at {home})."
        )
    return home


def check_pairing(runtime: Runtime, pack: Pack) -> None:
    if pack.runtime != runtime.version:
        raise PackError(
            f"pgwire-{pack.name} {pack.version} requires pgwire-calcite runtime {pack.runtime}, "
            f"but was started by runtime {runtime.version} ({runtime.home})."
        )
    if pack.variant != runtime.variant:
        raise PackError(
            f"pgwire-{pack.name} {pack.version} is built for {pack.variant}; runtime "
            f"{runtime.version} at {runtime.home} is {runtime.variant}."
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
