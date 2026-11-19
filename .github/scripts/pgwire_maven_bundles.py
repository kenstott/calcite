#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE-BSL.txt file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Lay out the pgwire runtime and adapter packs of one engine release for Maven Central (REQ-294).

Per platform, the release carries one pgwire-calcite runtime and one pack per adapter (layout:
pgwire-calcite/src/pgwire_calcite/packs.py). Each tarball becomes, unchanged, one Central artifact:
``pgwire-calcite-runtime-<variant>`` and ``pgwire-<adapter>-<variant>``; a pack and the runtime it
names, extracted into one directory, are an install. Publishes nothing: it writes one Central
bundle directory per artifact and ``sizes.json``; the maven-bundles job of
.github/workflows/pgwire-adapters-release.yml signs and zips them and attaches them to the release,
and "Promote to Maven Central" (promote-maven-central.yml) is the separate, manual upload.

Usage: pgwire_maven_bundles.py <tarball_dir> <out_dir> <version>
  <tarball_dir> holds ``pgwire-calcite-runtime-<version>-<variant>.tar.gz`` and
  ``pgwire-<adapter>-pack-<version>-<variant>.tar.gz``.
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import sys
import zipfile
from pathlib import Path

GROUP = "io.simpleishard"
_POM = """<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>{group}</groupId>
  <artifactId>{artifact}</artifactId>
  <version>{version}</version>
  <packaging>tar.gz</packaging>
  <name>{artifact}</name>
  <description>{description}</description>
  <url>https://github.com/kenstott/calcite</url>
  <licenses>
    <license>
      <name>Apache License, Version 2.0</name>
      <url>https://www.apache.org/licenses/LICENSE-2.0.txt</url>
    </license>
  </licenses>
  <developers>
    <developer><name>Kenneth Stott</name></developer>
  </developers>
  <scm>
    <url>https://github.com/kenstott/calcite</url>
    <connection>scm:git:https://github.com/kenstott/calcite.git</connection>
  </scm>
</project>
"""


def _digest(path: Path) -> str:
    with path.open("rb") as content:
        return hashlib.file_digest(content, "sha256").hexdigest()


def _placeholder_jar(out: Path, artifact: str, kind: str) -> None:
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as jar:
        jar.writestr(
            "README.txt",
            f"{artifact} is a packaged runtime bundle with no Java API of its own; its {kind} are "
            "those of the engine release it is built from: https://github.com/kenstott/calcite\n",
        )


def central_dir(artifact: str, version: str, tarball: Path, description: str, out: Path) -> Path:
    """One Central bundle directory: the tarball, its pom, sources and javadoc jars, sha256."""
    base = out / artifact / GROUP.replace(".", "/") / artifact / version
    base.mkdir(parents=True)
    stem = f"{artifact}-{version}"
    shutil.move(tarball, base / f"{stem}.tar.gz")
    (base / f"{stem}.pom").write_text(
        _POM.format(group=GROUP, artifact=artifact, version=version, description=description)
    )
    _placeholder_jar(base / f"{stem}-sources.jar", artifact, "sources")
    _placeholder_jar(base / f"{stem}-javadoc.jar", artifact, "javadoc")
    for name in (f"{stem}.tar.gz", f"{stem}.pom", f"{stem}-sources.jar", f"{stem}-javadoc.jar"):
        (base / f"{name}.sha256").write_text(_digest(base / name) + "\n")
    return base / f"{stem}.tar.gz"


RUNTIME = "pgwire-calcite-runtime"


def main(tarball_dir: Path, out: Path, version: str) -> int:
    v = re.escape(version)
    runtime = re.compile(rf"{RUNTIME}-{v}-(?P<v>[a-z0-9_-]+)\.tar\.gz")
    pack = re.compile(rf"pgwire-(?P<a>[a-z]+)-pack-{v}-(?P<v>[a-z0-9_-]+)\.tar\.gz")
    artifacts: dict[str, dict[str, tuple[Path, str]]] = {}
    for tarball in sorted(tarball_dir.glob("pgwire-*.tar.gz")):
        if m := runtime.fullmatch(tarball.name):
            artifacts.setdefault(m["v"], {})[f"{RUNTIME}-{m['v']}"] = (
                tarball, f"The pgwire-calcite runtime every pgwire adapter pack starts from ({m['v']})")
        elif m := pack.fullmatch(tarball.name):
            artifacts.setdefault(m["v"], {})[f"pgwire-{m['a']}-{m['v']}"] = (
                tarball, f"The {m['a']} pgwire adapter pack ({m['v']})")
    if not artifacts:
        print(f"no pgwire runtime or packs of {version} under {tarball_dir}", file=sys.stderr)
        return 1
    missing = [variant for variant, named in artifacts.items() if f"{RUNTIME}-{variant}" not in named]
    if missing:
        print(f"packs of {version} without their runtime for {', '.join(missing)}", file=sys.stderr)
        return 1
    out.mkdir(parents=True, exist_ok=True)
    report = {
        variant: {
            artifact: central_dir(artifact, version, tarball, description, out).stat().st_size
            for artifact, (tarball, description) in sorted(named.items())
        }
        for variant, named in sorted(artifacts.items())
    }
    (out / "sizes.json").write_text(json.dumps(report, indent=1) + "\n")
    for variant, sizes in report.items():
        print(f"{variant}: {sum(sizes.values()) / 1e6:.0f} MB")
        for artifact, size in sorted(sizes.items()):
            print(f"  {artifact}: {size / 1e6:.0f} MB")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(__doc__)
        raise SystemExit(2)
    raise SystemExit(main(Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3]))
