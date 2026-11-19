# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One runtime per engine version, one pack per adapter: the layout, the launchers' runtime
lookup and the pairing check, with a stand-in JRE that reports what the server would start with.
The live start of two packs over one runtime is test_runtime_packs_live.py."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import pathlib
import subprocess
import sys

import pytest

from pgwire_calcite import classpath, launcher, packs

PACKAGING = pathlib.Path(__file__).resolve().parents[2] / "packaging"
VERSION = "0.123.2"
VARIANT = "linux-x86_64"


def _load_runtime_packs():
    spec = importlib.util.spec_from_file_location("runtime_packs", PACKAGING / "runtime_packs.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


runtime_packs = _load_runtime_packs()

#: Stands in for the runtime's java: it runs what Launcher.java would start (the runtime's
#: CPython with the launcher's arguments), but asks packs.prepare for the server's argv and
#: prints it with the environment the server would read, instead of starting a JVM.
_FAKE_JAVA = """#!/usr/bin/env bash
py=""
args=()
skip_next=0
for a in "$@"; do
  if [ "$skip_next" = 1 ]; then skip_next=0; continue; fi
  case "$a" in
    -Dpgwire.python=*) py="${a#-Dpgwire.python=}" ;;
    -jar) skip_next=1 ;;
    *) args+=("$a") ;;
  esac
done
exec "$py" - "${args[@]}" <<'PY'
import json, os, sys
from pgwire_calcite.packs import PackError, prepare
assert sys.argv[1] == "--pack", sys.argv
try:
    argv = prepare(sys.argv[2], sys.argv[3:])
except PackError as e:
    print(e, file=sys.stderr)
    sys.exit(2)
print(json.dumps({
    "argv": argv,
    "classpath_dirs": os.environ["PGWIRE_CALCITE_CLASSPATH_DIRS"].split(os.pathsep),
    "java_home": os.environ["PGWIRE_CALCITE_JAVA_HOME"],
    "pythonpath": os.environ.get("PYTHONPATH"),
    "state_dir": os.environ["PGWIRE_CALCITE_STATE_DIR"],
}))
PY
"""

COMMON = ["calcite-core-1.41.0.jar", "calcite-linq4j-1.41.0.jar", "avatica-core-1.26.0.jar",
          "jackson-core-2.18.4.jar"]
OWN = {
    "file": ["calcite-file-1.41.0.jar", "poi-5.4.0.jar"],
    "jdbc": ["hsqldb-2.7.2.jar", "scott-data-hsqldb-0.2.jar"],
}


def _jar_dir(path: pathlib.Path, names) -> pathlib.Path:
    path.mkdir(parents=True)
    for n in names:
        (path / n).write_bytes(n.encode())
    return path


def _stand_in_python(path: pathlib.Path) -> None:
    """The runtime's CPython: the test's interpreter (a wrapper, since a symlink outside its
    virtualenv would not find the virtualenv's packages)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f'#!/bin/sh\nexec "{sys.executable}" "$@"\n')
    path.chmod(0o755)


def _stage_runtime(root: pathlib.Path, build: pathlib.Path, version: str = VERSION) -> pathlib.Path:
    """runtimes/<version>/ with a stand-in JRE, the test's interpreter as its CPython, and the
    jars the two adapters share."""
    home = root / "runtimes" / version
    (home / "jre" / "bin").mkdir(parents=True)
    java = home / "jre" / "bin" / "java"
    java.write_text(_FAKE_JAVA)
    java.chmod(0o755)
    _stand_in_python(home / "cpython" / "bin" / "python")
    (home / "pgwire-launcher.jar").write_bytes(b"launcher")
    runtime_packs.common_jars(home / "jars", [build / "file", build / "jdbc"])
    runtime_packs.write_runtime(home, version, VARIANT)
    return home


def _stage_pack(root: pathlib.Path, build: pathlib.Path, adapter: str, args) -> pathlib.Path:
    home = root / "packs" / adapter / VERSION
    runtime_packs.pack_jars(root / "runtimes" / VERSION / "jars", build / adapter, home / "jars")
    (home / "model").mkdir(parents=True)
    (home / "model" / "model.json").write_text(json.dumps({"version": "1.0", "schemas": []}))
    runtime_packs.write_pack(home, adapter, VERSION, VERSION, VARIANT, list(args))
    return home


@pytest.fixture
def installed(tmp_path):
    build = tmp_path / "build"
    for adapter, own in OWN.items():
        _jar_dir(build / adapter, COMMON + own)
    root = tmp_path / "root"
    _stage_runtime(root, build)
    _stage_pack(root, build, "file", ["--database", "file"])
    _stage_pack(root, build, "jdbc", ["--database", "jdbc", "--allow-writes"])
    return root


def _start(pack_home: pathlib.Path, *args, env=None) -> subprocess.CompletedProcess:
    run_env = dict(os.environ, **(env or {}))
    run_env.pop("PGWIRE_CALCITE_RUNTIME_HOME", None)
    return subprocess.run([str(pack_home / "bin" / f"pgwire-{pack_home.parent.name}"), *args],
                          check=False, capture_output=True, text=True, env=run_env, timeout=60)


def _plan(pack_home: pathlib.Path, *args) -> dict:
    done = _start(pack_home, *args, env={"PGWIRE_CALCITE_STATE_DIR": "/state"})
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout)


def _tree_digest(path: pathlib.Path) -> str:
    h = hashlib.sha256()
    for p in sorted(path.rglob("*")):
        h.update(str(p.relative_to(path)).encode())
        if p.is_file():
            h.update(p.read_bytes())
    return h.hexdigest()


# --- layout ------------------------------------------------------------------------------


def test_two_packs_share_one_runtime_and_hold_nothing_of_it(installed):
    runtime = installed / "runtimes" / VERSION
    assert sorted(p.name for p in (runtime / "jars").iterdir()) == sorted(COMMON)
    for adapter, own in OWN.items():
        pack = installed / "packs" / adapter / VERSION
        assert sorted(p.name for p in (pack / "jars").iterdir()) == sorted(own)
        for shared in ("jre", "cpython", "python", "pgwire-launcher.jar"):
            assert not (pack / shared).exists(), f"{adapter} pack holds {shared}"
    assert list((installed / "runtimes").iterdir()) == [runtime], "one runtime directory"


def test_each_pack_starts_from_the_shared_runtime_with_its_own_jars_after_the_runtimes(installed):
    runtime = installed / "runtimes" / VERSION
    for adapter, flags in (("file", ["--database", "file"]),
                           ("jdbc", ["--database", "jdbc", "--allow-writes"])):
        pack = installed / "packs" / adapter / VERSION
        plan = _plan(pack, "--port", "6543")
        assert plan["classpath_dirs"] == [str(runtime / "jars"), str(pack / "jars")]
        assert plan["java_home"] == str(runtime / "jre")
        assert plan["argv"] == ["--backend", "calcite", "--model", str(pack / "model" / "model.json"),
                                *flags, "--port", "6543"]
        assert plan["state_dir"] == "/state"


def test_the_classpath_is_the_runtime_jars_then_the_pack_jars(installed, monkeypatch):
    runtime = installed / "runtimes" / VERSION
    pack = installed / "packs" / "jdbc" / VERSION
    monkeypatch.delenv("PGWIRE_CALCITE_CLASSPATH", raising=False)
    monkeypatch.setenv("PGWIRE_CALCITE_CLASSPATH_DIRS", os.pathsep.join(_plan(pack)["classpath_dirs"]))
    monkeypatch.setattr(classpath, "_vendored_jars", list)
    assert classpath.resolve_classpath() == (
        [str(runtime / "jars" / n) for n in sorted(COMMON)]
        + [str(pack / "jars" / n) for n in sorted(OWN["jdbc"])])


def test_a_pack_with_python_extras_puts_them_on_the_python_path(installed):
    pack = installed / "packs" / "file" / VERSION
    (pack / "site-packages").mkdir()
    assert _plan(pack)["pythonpath"].split(os.pathsep)[0] == str(pack / "site-packages")
    assert _plan(installed / "packs" / "jdbc" / VERSION)["pythonpath"] in (None, os.environ.get("PYTHONPATH"))


# --- the runtime the pack requires -------------------------------------------------------


def test_a_pack_whose_runtime_version_is_absent_is_refused_at_start_naming_it(installed):
    (installed / "runtimes" / VERSION).rename(installed / "runtimes" / "0.123.1")
    done = _start(installed / "packs" / "file" / VERSION)
    assert done.returncode == 2
    assert f"requires pgwire-calcite runtime {VERSION}, which is not installed" in done.stderr
    assert done.stdout == ""


def test_the_python_side_refuses_a_runtime_other_than_the_one_the_pack_names(installed, monkeypatch):
    other = installed / "runtimes" / "0.124.0"
    (installed / "runtimes" / VERSION).rename(other)
    runtime_packs.write_runtime(other, "0.124.0", VARIANT)
    monkeypatch.setenv(packs.RUNTIME_HOME_ENV, str(other))
    with pytest.raises(packs.PackError, match=f"requires pgwire-calcite runtime {VERSION}, but was "
                                              "started by runtime 0.124.0"):
        packs.prepare(str(installed / "packs" / "file" / VERSION), [])


def test_the_python_side_refuses_a_pack_built_for_another_platform(installed, monkeypatch):
    pack = installed / "packs" / "file" / VERSION
    manifest = json.loads((pack / "pack.json").read_text())
    (pack / "pack.json").write_text(json.dumps(dict(manifest, variant="macos-arm64")))
    monkeypatch.setenv(packs.RUNTIME_HOME_ENV, str(installed / "runtimes" / VERSION))
    with pytest.raises(packs.PackError, match="is built for macos-arm64"):
        packs.prepare(str(pack), [])


def test_runtime_home_for_names_the_missing_version(installed):
    pack = packs.load_pack(installed / "packs" / "jdbc" / VERSION)
    assert packs.runtime_home_for(pack) == installed / "runtimes" / VERSION
    (installed / "runtimes" / VERSION / "runtime.json").unlink()
    with pytest.raises(packs.PackError, match=f"runtime {VERSION}, which is not installed"):
        packs.runtime_home_for(pack)


def test_launcher_main_reports_a_pack_error_by_name_without_a_traceback(installed, monkeypatch, capsys):
    monkeypatch.delenv(packs.RUNTIME_HOME_ENV, raising=False)
    assert launcher.main(["--pack", str(installed / "packs" / "file" / VERSION)]) == 2
    assert packs.RUNTIME_HOME_ENV in capsys.readouterr().err


# --- a Python-only runtime update --------------------------------------------------------


def test_a_python_only_runtime_update_leaves_both_packs_untouched_and_both_start(installed):
    pack_dirs = [installed / "packs" / a / VERSION for a in OWN]
    before = {p: _tree_digest(p) for p in pack_dirs}
    runtime = installed / "runtimes" / VERSION
    jars_before = _tree_digest(runtime / "jars")

    # The update replaces the runtime's CPython (pgwire_calcite inside it) and nothing else.
    _stand_in_python(runtime / "cpython" / "bin" / "python")
    (runtime / "cpython" / "UPDATED").write_text("pgwire_calcite build 2")

    assert {p: _tree_digest(p) for p in pack_dirs} == before
    assert _tree_digest(runtime / "jars") == jars_before
    for pack in pack_dirs:
        assert _plan(pack)["classpath_dirs"][0] == str(runtime / "jars")


# --- building the layout -----------------------------------------------------------------


def test_pack_jars_refuses_another_version_of_a_runtime_jar(tmp_path):
    runtime = _jar_dir(tmp_path / "runtime", COMMON)
    adapter = _jar_dir(tmp_path / "adapter", ["calcite-core-1.41.0.jar", "jackson-core-2.19.0.jar"])
    with pytest.raises(SystemExit, match=r"jackson-core-2.19.0.jar \(runtime has jackson-core-2.18.4.jar\)"):
        runtime_packs.pack_jars(runtime, adapter, tmp_path / "out")


def test_a_pack_holding_a_runtime_part_is_refused(tmp_path):
    pack = tmp_path / "pack"
    (pack / "model").mkdir(parents=True)
    (pack / "model" / "model.json").write_text("{}")
    (pack / "jre").mkdir()
    with pytest.raises(SystemExit, match="holds jre, which belongs to the runtime"):
        runtime_packs.write_pack(pack, "file", VERSION, VERSION, VARIANT, [])


def test_windows_launchers_are_batch_files_with_the_pack_values_filled(tmp_path):
    pack = tmp_path / "pack"
    (pack / "model").mkdir(parents=True)
    (pack / "model" / "model.json").write_text("{}")
    runtime_packs.write_pack(pack, "govdata", VERSION, VERSION, "windows-x86_64",
                             ["--auth", "askamerica-key"])
    bat = (pack / "bin" / "pgwire-govdata.bat").read_bytes().decode("ascii")
    assert "\r\n" in bat and "@" + "RUNTIME@" not in bat
    assert f"runtimes\\{VERSION}" in bat
    assert f"requires pgwire-calcite runtime {VERSION}" in bat
