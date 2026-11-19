#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Lay out a pgwire-calcite runtime and its adapter packs (layout: src/pgwire_calcite/packs.py).

Standard library only: the release workflows run it with the runner's python3.

  runtime_packs.py common-jars <out> <adapter-jars>...
      The jars (by file name) every adapter's jar directory holds: the runtime's jars.
  runtime_packs.py pack-jars <runtime-jars> <adapter-jars> <out>
      The adapter's jars the runtime does not hold. An artifact the runtime holds in another
      version is refused by name: the pack would put two versions of it on one classpath.
  runtime_packs.py runtime <runtime-dir> <version> <variant>
      Writes runtime.json and bin/pgwire-calcite-runtime(.bat) into a staged runtime directory
      (jre/ python/ cpython/ jars/ pgwire-launcher.jar already in it).
  runtime_packs.py pack <pack-dir> <adapter> <version> <runtime-version> <variant> [arg...]
      Writes pack.json and bin/pgwire-<adapter>(.bat) into a staged pack directory (jars/ and
      model/model.json already in it). The args are the server flags the pack's launcher passes.
  runtime_packs.py pack-extras <runtime-python> <pack-dir> <requirement>...
      Installs the requirements into <pack-dir>/site-packages with the runtime's interpreter,
      pinned to the runtime's versions of what it already has, then removes every distribution
      the runtime already has: the pack carries only what the runtime lacks.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
LAUNCHERS = HERE / "launchers"
RUNTIME_NAME = "pgwire-calcite-runtime"
FORMAT = 1
#: name-version.jar, as scripts/dedupe_jars.py reads it.
VERSION_RE = re.compile(r"^(.+)-([0-9][0-9A-Za-z_.]*)\.jar$")


def _jars(directory: Path) -> dict[str, Path]:
    found = {p.name: p for p in directory.glob("*.jar")}
    if not found:
        raise SystemExit(f"{directory} holds no jars")
    return found


def common_jars(out: Path, adapter_dirs: list[Path]) -> list[str]:
    sets = [_jars(d) for d in adapter_dirs]
    common = sorted(set.intersection(*(set(s) for s in sets)))
    out.mkdir(parents=True, exist_ok=True)
    for name in common:
        shutil.copy2(sets[0][name], out / name)
    return common


def _artifact(name: str) -> str:
    m = VERSION_RE.match(name)
    return m.group(1) if m else name[: -len(".jar")]


def pack_jars(runtime_dir: Path, adapter_dir: Path, out: Path) -> list[str]:
    runtime = _jars(runtime_dir)
    adapter = _jars(adapter_dir)
    by_artifact = {_artifact(n): n for n in runtime}
    own = sorted(n for n in adapter if n not in runtime)
    clashes = [(n, by_artifact[_artifact(n)]) for n in own if _artifact(n) in by_artifact]
    if clashes:
        listed = ", ".join(f"{n} (runtime has {r})" for n, r in clashes)
        raise SystemExit(f"{adapter_dir}: the adapter needs another version of a runtime jar: {listed}")
    out.mkdir(parents=True, exist_ok=True)
    for name in own:
        shutil.copy2(adapter[name], out / name)
    return own


def _windows(variant: str) -> bool:
    return variant.startswith("windows-")


def _write_launcher(template: str, dest: Path, variant: str, values: dict[str, str]) -> Path:
    suffix = ".bat" if _windows(variant) else ".sh"
    text = (LAUNCHERS / f"{template}{suffix}").read_text(encoding="utf-8")
    for key, value in values.items():
        text = text.replace(f"@{key}@", value)
    leftover = re.findall(r"@[A-Z]+@", text)
    if leftover:
        raise SystemExit(f"{template}{suffix}: unfilled placeholders {sorted(set(leftover))}")
    dest = dest.with_suffix(".bat") if _windows(variant) else dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    if _windows(variant):
        dest.write_bytes(text.replace("\r\n", "\n").replace("\n", "\r\n").encode("ascii"))
    else:
        dest.write_text(text, encoding="utf-8")
        dest.chmod(0o755)
    return dest


def _require(directory: Path, *names: str) -> None:
    missing = [n for n in names if not (directory / n).exists()]
    if missing:
        raise SystemExit(f"{directory} is missing {', '.join(missing)}")


def write_runtime(runtime_dir: Path, version: str, variant: str) -> None:
    _require(runtime_dir, "jre", "cpython", "jars", "pgwire-launcher.jar")
    manifest = {"format": FORMAT, "name": RUNTIME_NAME, "version": version, "variant": variant}
    (runtime_dir / "runtime.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    _write_launcher("runtime", runtime_dir / "bin" / "pgwire-calcite-runtime", variant, {})


def write_pack(pack_dir: Path, adapter: str, version: str, runtime: str, variant: str,
               args: list[str]) -> None:
    _require(pack_dir, "model/model.json")
    for shared in ("jre", "cpython", "python", "pgwire-launcher.jar"):
        if (pack_dir / shared).exists():
            raise SystemExit(f"{pack_dir} holds {shared}, which belongs to the runtime")
    manifest = {"format": FORMAT, "name": adapter, "version": version, "runtime": runtime,
                "variant": variant, "args": args}
    (pack_dir / "pack.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    _write_launcher("pack", pack_dir / "bin" / f"pgwire-{adapter}", variant,
                    {"ADAPTER": adapter, "VERSION": version, "RUNTIME": runtime})


def _canonical(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _freeze(python: str) -> dict[str, str]:
    out = subprocess.run(["uv", "pip", "freeze", "--python", python],
                         check=True, capture_output=True, text=True).stdout
    pins = {}
    for line in out.splitlines():
        if "==" in line:
            name, version = line.split("==", 1)
            pins[_canonical(name)] = version.strip()
    return pins


def pack_extras(runtime_python: str, pack_dir: Path, requirements: list[str]) -> list[str]:
    runtime = _freeze(runtime_python)
    target = pack_dir / "site-packages"
    if target.exists():
        raise SystemExit(f"{target} already exists")
    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as constraints:
        constraints.write("".join(f"{n}=={v}\n" for n, v in runtime.items()))
    try:
        subprocess.run(["uv", "pip", "install", "--python", runtime_python, "--target", str(target),
                        "--constraint", constraints.name, *requirements], check=True)
    finally:
        os.unlink(constraints.name)
    kept = []
    for info in sorted(target.glob("*.dist-info")):
        name = _canonical(info.name[: -len(".dist-info")].rsplit("-", 1)[0])
        if name not in runtime:
            kept.append(name)
            continue
        for line in (info / "RECORD").read_text(encoding="utf-8").splitlines():
            rel = line.split(",", 1)[0]
            path = target / rel
            if path.is_file() or path.is_symlink():
                path.unlink()
        # RECORD lists the dist-info's own files; the directory itself is left.
        shutil.rmtree(info)
    # Directories left empty by the removals (package dirs, __pycache__) go too.
    for d in sorted((p for p in target.rglob("*") if p.is_dir()), key=lambda p: len(p.parts), reverse=True):
        if not any(d.iterdir()):
            d.rmdir()
    return kept


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2
    cmd, rest = argv[1], argv[2:]
    if cmd == "common-jars" and len(rest) >= 2:
        names = common_jars(Path(rest[0]), [Path(d) for d in rest[1:]])
        print(f"runtime jars: {len(names)}")
    elif cmd == "pack-jars" and len(rest) == 3:
        names = pack_jars(Path(rest[0]), Path(rest[1]), Path(rest[2]))
        print(f"pack jars: {len(names)}")
    elif cmd == "runtime" and len(rest) == 3:
        write_runtime(Path(rest[0]), rest[1], rest[2])
    elif cmd == "pack" and len(rest) >= 5:
        write_pack(Path(rest[0]), rest[1], rest[2], rest[3], rest[4], rest[5:])
    elif cmd == "pack-extras" and len(rest) >= 3:
        kept = pack_extras(rest[0], Path(rest[1]), rest[2:])
        print(f"pack extras: {', '.join(kept)}")
    else:
        print(__doc__, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
