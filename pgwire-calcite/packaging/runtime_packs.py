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
      The adapter's jars the runtime does not hold. A pack jar the runtime also holds, by name or
      as another version of the same artifact (a jar already in <out> included), is refused by
      name: the pack would put the jar on the classpath twice.
  runtime_packs.py runtime <runtime-dir> <version> <variant>
      Writes runtime.json (with the runtime line in runtime-line) and bin/pgwire-calcite-runtime
      (.bat) into a staged runtime directory (jre/ python/ cpython/ jars/ pgwire-launcher.jar
      already in it).
  runtime_packs.py pack <pack-dir> <adapter> <version> <variant> <runtime-jars> [arg...]
      Writes pack.json (naming the runtime line in runtime-line and the runtime's jar names, read
      from <runtime-jars>, as built) and bin/pgwire-<adapter>(.bat)
      into a staged pack directory (jars/ and model/model.json already in it). The args are the
      server flags the pack's launcher passes.
  runtime_packs.py formulas <out> <version> <sums-dir> [adapter...]
      Writes the Homebrew formula and Scoop manifest of the runtime and of each adapter's pack
      (every adapter in adapters.json released at <version> when none is named): one runtime
      formula plus one formula per pack depending on it. Each tarball's sha256 is read from
      <sums-dir>/<tarball>.sha256, the release's own sha256 asset.
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
#: The runtime line (ABI number) this checkout's runtime provides and its packs require. Bump it
#: when the runtime's jar set or the runtime/pack contract changes; a runtime release on the same
#: line starts the packs already installed (REQ-2254, amended 2026-11-20).
LINE_FILE = HERE / "runtime-line"
VERSION_FORMAT = re.compile(r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$")
REPO = "https://github.com/kenstott/calcite"
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


def _runtime_clashes(runtime: dict[str, Path], names) -> list[tuple[str, str]]:
    by_artifact = {_artifact(n): n for n in runtime}
    return [(n, by_artifact[_artifact(n)]) for n in sorted(names) if _artifact(n) in by_artifact]


def pack_jars(runtime_dir: Path, adapter_dir: Path, out: Path) -> list[str]:
    runtime = _jars(runtime_dir)
    adapter = _jars(adapter_dir)
    own = sorted(n for n in adapter if n not in runtime)
    clashes = _runtime_clashes(runtime, own)
    if clashes:
        listed = ", ".join(f"{n} (runtime has {r})" for n, r in clashes)
        raise SystemExit(f"{adapter_dir}: the adapter needs another version of a runtime jar: {listed}")
    present = [p.name for p in out.glob("*.jar")] if out.is_dir() else []
    clashes = _runtime_clashes(runtime, present)
    if clashes:
        listed = ", ".join(f"{n} (runtime has {r})" for n, r in clashes)
        raise SystemExit(f"{out} holds a jar the runtime also holds: {listed}")
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


def read_line() -> int:
    text = LINE_FILE.read_text(encoding="utf-8").strip()
    if not text.isdigit() or int(text) < 1:
        raise SystemExit(f"{LINE_FILE} holds {text!r}, not a positive integer")
    return int(text)


def _check_line(line: int) -> None:
    if not isinstance(line, int) or isinstance(line, bool) or line < 1:
        raise SystemExit(f"runtime line {line!r} is not a positive integer")


def _check_version(version: str) -> None:
    if not VERSION_FORMAT.match(version):
        raise SystemExit(f"{version!r} is not a release version (major.minor.patch)")


def write_runtime(runtime_dir: Path, version: str, variant: str, line: int) -> None:
    _require(runtime_dir, "jre", "cpython", "jars", "pgwire-launcher.jar")
    _check_version(version)
    _check_line(line)
    # "line" stays before the last key: the launchers find it as the text `"line": <n>,`.
    manifest = {"format": FORMAT, "name": RUNTIME_NAME, "line": line, "version": version,
                "variant": variant}
    (runtime_dir / "runtime.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    _write_launcher("runtime", runtime_dir / "bin" / "pgwire-calcite-runtime", variant, {})


def write_pack(pack_dir: Path, adapter: str, version: str, line: int, variant: str,
               runtime_jars: list[str], args: list[str]) -> None:
    _require(pack_dir, "model/model.json")
    if not runtime_jars:
        raise SystemExit("a pack records the runtime jars it was built against; none given")
    _check_version(version)
    _check_line(line)
    for shared in ("jre", "cpython", "python", "pgwire-launcher.jar"):
        if (pack_dir / shared).exists():
            raise SystemExit(f"{pack_dir} holds {shared}, which belongs to the runtime")
    manifest = {"format": FORMAT, "name": adapter, "version": version, "runtime_line": line,
                "variant": variant, "runtime_jars": sorted(runtime_jars), "args": args}
    (pack_dir / "pack.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    _write_launcher("pack", pack_dir / "bin" / f"pgwire-{adapter}", variant,
                    {"ADAPTER": adapter, "VERSION": version, "LINE": str(line),
                     "JARS": " ".join(sorted(runtime_jars))})


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


# --- package-manager formulas ------------------------------------------------------------
# One runtime formula plus one formula per pack depending on it (REQ-2254, amended 2026-11-20).
# Homebrew and Scoop install each formula in its own prefix, so a pack's wrapper names the
# runtime formula's runtimes directory in PGWIRE_CALCITE_RUNTIMES (packs.py RUNTIMES_ENV); a
# runtime upgrade on the same line is then started by the packs already installed.

#: Homebrew's platform blocks, by release variant. Windows ships through Scoop.
BREW_PLATFORMS = {
    "macos-arm64": ("on_macos", "on_arm"),
    "linux-x86_64": ("on_linux", "on_intel"),
    "linux-aarch64": ("on_linux", "on_arm"),
}
SCOOP_VARIANT = "windows-x86_64"


def _version_key(version: str) -> tuple[int, ...]:
    _check_version(version)
    return tuple(int(x) for x in version.split("."))


def _released(entries: list[dict], version: str) -> list[dict]:
    return [e for e in entries if "since" not in e or _version_key(e["since"]) <= _version_key(version)]


def _class_name(formula: str) -> str:
    return "".join(part.capitalize() for part in formula.split("-"))


def _tarball(part: str, version: str, variant: str) -> str:
    return f"{part}-{version}-{variant}.tar.gz"


def _url(part: str, version: str, variant: str) -> str:
    return f"{REPO}/releases/download/engine-v{version}/{_tarball(part, version, variant)}"


def _sha256(sums: Path, tarball: str) -> str:
    path = sums / f"{tarball}.sha256"
    if not path.is_file():
        raise SystemExit(f"{path} is missing: every tarball a formula names needs its sha256 asset")
    fields = path.read_text(encoding="utf-8").split()
    digest = fields[0] if fields else ""
    if not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise SystemExit(f"{path} holds no sha256 digest")
    return digest


def _brew_platforms(part: str, version: str, sums: Path, variants: list[str]) -> str:
    blocks: dict[str, list[str]] = {}
    for variant in variants:
        if variant not in BREW_PLATFORMS:
            continue
        os_block, arch_block = BREW_PLATFORMS[variant]
        tarball = _tarball(part, version, variant)
        blocks.setdefault(os_block, []).append(
            f"    {arch_block} do\n"
            f"      url \"{_url(part, version, variant)}\"\n"
            f"      sha256 \"{_sha256(sums, tarball)}\"\n"
            f"    end\n")
    if not blocks:
        raise SystemExit(f"no Homebrew platform is released at {version}")
    return "\n".join(f"  {os_block} do\n{''.join(arch)}  end\n" for os_block, arch in blocks.items())


_HEADER = "# Generated by pgwire-calcite/packaging/runtime_packs.py formulas; do not edit.\n"


def _brew_runtime(version: str, sums: Path, variants: list[str]) -> str:
    return (
        f"{_HEADER}"
        f"# The pgwire-calcite runtime (CPython, pgwire_calcite, the JRE and the jars every adapter\n"
        f"# uses), installed once and shared by every pgwire-<adapter> formula that depends on it.\n"
        f"class {_class_name(RUNTIME_NAME)} < Formula\n"
        f"  desc \"Shared runtime of the pgwire-calcite PostgreSQL wire-protocol adapters\"\n"
        f"  homepage \"{REPO}\"\n"
        f"  version \"{version}\"\n"
        f"  license \"BUSL-1.1\"\n\n"
        f"{_brew_platforms(RUNTIME_NAME, version, sums, variants)}\n"
        f"  def install\n"
        f"    # The tarball's single top-level directory is runtimes/; Homebrew stages inside it.\n"
        f"    (libexec/\"runtimes\").install Dir[\"*\"]\n"
        f"  end\n\n"
        f"  test do\n"
        f"    assert_predicate libexec/\"runtimes/#{{version}}/runtime.json\", :exist?\n"
        f"  end\n"
        f"end\n")


def _brew_pack(adapter: str, version: str, sums: Path, variants: list[str]) -> str:
    formula = f"pgwire-{adapter}"
    return (
        f"{_HEADER}"
        f"# The {adapter} adapter's pack: its jars, model and launcher. It starts the newest\n"
        f"# {RUNTIME_NAME} installed on its runtime line.\n"
        f"class {_class_name(formula)} < Formula\n"
        f"  desc \"PostgreSQL wire-protocol server over Apache Calcite's {adapter} adapter\"\n"
        f"  homepage \"{REPO}\"\n"
        f"  version \"{version}\"\n"
        f"  license \"BUSL-1.1\"\n\n"
        f"  depends_on \"{RUNTIME_NAME}\"\n\n"
        f"{_brew_platforms(f'{formula}-pack', version, sums, variants)}\n"
        f"  def install\n"
        f"    # The tarball's single top-level directory is packs/; Homebrew stages inside it.\n"
        f"    (libexec/\"packs\").install Dir[\"*\"]\n"
        f"    (bin/\"{formula}\").write <<~SH\n"
        f"      #!/bin/bash\n"
        f"      export PGWIRE_CALCITE_RUNTIMES=\"#{{Formula[\"{RUNTIME_NAME}\"].opt_libexec}}/runtimes\"\n"
        f"      exec \"#{{libexec}}/packs/{adapter}/#{{version}}/bin/{formula}\" \"$@\"\n"
        f"    SH\n"
        f"    chmod 0755, bin/\"{formula}\"\n"
        f"  end\n\n"
        f"  test do\n"
        f"    assert_match \"pgwire-calcite\", shell_output(\"#{{bin}}/{formula} --help\")\n"
        f"  end\n"
        f"end\n")


def _scoop_runtime(version: str, sums: Path) -> dict:
    tarball = _tarball(RUNTIME_NAME, version, SCOOP_VARIANT)
    return {
        "//": "Generated by pgwire-calcite/packaging/runtime_packs.py formulas; do not edit. The "
              "pgwire-calcite runtime, shared by every pgwire-<adapter> manifest that depends on it.",
        "version": version,
        "description": "Shared runtime of the pgwire-calcite PostgreSQL wire-protocol adapters",
        "homepage": REPO,
        "license": "BUSL-1.1",
        "architecture": {"64bit": {"url": _url(RUNTIME_NAME, version, SCOOP_VARIANT),
                                   "hash": _sha256(sums, tarball)}},
        "checkver": "github",
        "autoupdate": {"architecture": {"64bit": {
            "url": f"{REPO}/releases/download/engine-v$version/{RUNTIME_NAME}-$version-{SCOOP_VARIANT}.tar.gz"}}},
    }


def _scoop_pack(adapter: str, version: str, sums: Path) -> dict:
    formula = f"pgwire-{adapter}"
    part = f"{formula}-pack"
    tarball = _tarball(part, version, SCOOP_VARIANT)
    # The wrapper runs from apps/<formula>/<version>/ (or current/); the runtime formula's
    # current version is apps/pgwire-calcite-runtime/current.
    wrapper = [
        "$wrapper = @(",
        "  '@echo off',",
        f"  'set \"PGWIRE_CALCITE_RUNTIMES=%~dp0..\\..\\{RUNTIME_NAME}\\current\\runtimes\"',",
        f"  ('call \"%~dp0packs\\{adapter}\\' + $version + '\\bin\\{formula}.bat\" %*'),",
        "  'exit /b %ERRORLEVEL%'",
        ")",
        f"Set-Content -Path \"$dir\\{formula}.bat\" -Value $wrapper -Encoding Ascii",
    ]
    return {
        "//": f"Generated by pgwire-calcite/packaging/runtime_packs.py formulas; do not edit. The "
              f"{adapter} adapter's pack; it starts the newest {RUNTIME_NAME} on its runtime line.",
        "version": version,
        "description": f"PostgreSQL wire-protocol server over Apache Calcite's {adapter} adapter",
        "homepage": REPO,
        "license": "BUSL-1.1",
        "depends": RUNTIME_NAME,
        "architecture": {"64bit": {"url": _url(part, version, SCOOP_VARIANT),
                                   "hash": _sha256(sums, tarball)}},
        "installer": {"script": wrapper},
        "bin": f"{formula}.bat",
        "checkver": "github",
        "autoupdate": {"architecture": {"64bit": {
            "url": f"{REPO}/releases/download/engine-v$version/{part}-$version-{SCOOP_VARIANT}.tar.gz"}}},
    }


def write_formulas(out: Path, version: str, sums: Path, adapters: list[str]) -> list[Path]:
    _check_version(version)
    released = [a["name"] for a in _released(json.loads((HERE / "adapters.json").read_text()), version)]
    unknown = [a for a in adapters if a not in released]
    if unknown:
        raise SystemExit(f"no adapter released at {version} is named {', '.join(unknown)}")
    chosen = adapters or released
    variants = [p["variant"] for p in _released(json.loads((HERE / "platforms.json").read_text()), version)]
    brew, scoop = out / "homebrew", out / "scoop"
    brew.mkdir(parents=True, exist_ok=True)
    scoop.mkdir(parents=True, exist_ok=True)
    written = [brew / f"{RUNTIME_NAME}.rb"]
    written[0].write_text(_brew_runtime(version, sums, variants), encoding="utf-8")
    windows = SCOOP_VARIANT in variants
    if windows:
        path = scoop / f"{RUNTIME_NAME}.json"
        path.write_text(json.dumps(_scoop_runtime(version, sums), indent=2) + "\n", encoding="utf-8")
        written.append(path)
    for adapter in chosen:
        path = brew / f"pgwire-{adapter}.rb"
        path.write_text(_brew_pack(adapter, version, sums, variants), encoding="utf-8")
        written.append(path)
        if windows:
            path = scoop / f"pgwire-{adapter}.json"
            path.write_text(json.dumps(_scoop_pack(adapter, version, sums), indent=2) + "\n",
                            encoding="utf-8")
            written.append(path)
    return written


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
        write_runtime(Path(rest[0]), rest[1], rest[2], read_line())
    elif cmd == "pack" and len(rest) >= 5:
        write_pack(Path(rest[0]), rest[1], rest[2], read_line(), rest[3], sorted(_jars(Path(rest[4]))),
                   rest[5:])
    elif cmd == "formulas" and len(rest) >= 3:
        for path in write_formulas(Path(rest[0]), rest[1], Path(rest[2]), rest[3:]):
            print(path)
    elif cmd == "pack-extras" and len(rest) >= 3:
        kept = pack_extras(rest[0], Path(rest[1]), rest[2:])
        print(f"pack extras: {', '.join(kept)}")
    else:
        print(__doc__, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
