# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One runtime per engine version, one pack per adapter: the layout, the launchers' lookup of the
newest runtime on the pack's line, the pairing check, the duplicate-jar refusals and the
package-manager formulas, with a stand-in JRE that reports what the server would start with.
The live start of two packs over one runtime is test_runtime_packs_live.py."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import pathlib
import shutil
import subprocess
import sys

import pytest

from pgwire_calcite import classpath, launcher, packs

PACKAGING = pathlib.Path(__file__).resolve().parents[2] / "packaging"
VERSION = "0.123.2"
VARIANT = "linux-x86_64"
LINE = 1


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
    "runtime_home": os.environ["PGWIRE_CALCITE_RUNTIME_HOME"],
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


def _stage_runtime(root: pathlib.Path, build: pathlib.Path, version: str = VERSION,
                   line: int = LINE) -> pathlib.Path:
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
    runtime_packs.write_runtime(home, version, VARIANT, line)
    return home


def _stage_pack(root: pathlib.Path, build: pathlib.Path, adapter: str, args) -> pathlib.Path:
    home = root / "packs" / adapter / VERSION
    runtime_packs.pack_jars(root / "runtimes" / VERSION / "jars", build / adapter, home / "jars")
    (home / "model").mkdir(parents=True)
    (home / "model" / "model.json").write_text(json.dumps({"version": "1.0", "schemas": []}))
    runtime_jars = sorted(j.name for j in (root / "runtimes" / VERSION / "jars").glob("*.jar"))
    runtime_packs.write_pack(home, adapter, VERSION, LINE, VARIANT, runtime_jars, list(args))
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


# --- the runtime line the pack requires --------------------------------------------------


def test_the_manifests_name_the_line_not_an_exact_runtime_version(installed):
    runtime = json.loads((installed / "runtimes" / VERSION / "runtime.json").read_text())
    pack = json.loads((installed / "packs" / "file" / VERSION / "pack.json").read_text())
    assert runtime["line"] == LINE and runtime["version"] == VERSION
    assert pack["runtime_line"] == LINE and "runtime" not in pack


def test_the_checked_in_line_is_what_the_cli_stamps(tmp_path):
    line = runtime_packs.read_line()
    assert line >= 1
    pack = tmp_path / "packs" / "file" / VERSION
    (pack / "model").mkdir(parents=True)
    (pack / "model" / "model.json").write_text("{}")
    jars = _jar_dir(tmp_path / "runtime-jars", COMMON)
    assert runtime_packs.main(["runtime_packs.py", "pack", str(pack), "file", VERSION, VARIANT,
                               str(jars)]) == 0
    manifest = json.loads((pack / "pack.json").read_text())
    assert manifest["runtime_line"] == line
    assert manifest["runtime_jars"] == sorted(COMMON)


def test_a_pack_starts_the_newest_runtime_installed_on_its_line(installed, tmp_path):
    build = tmp_path / "build"
    newer = _stage_runtime(installed, build, "0.130.1")
    _stage_runtime(installed, build, "0.124.9")
    _stage_runtime(installed, build, "0.200.0", line=LINE + 1)  # newest of all, off the line
    for adapter in OWN:
        plan = _plan(installed / "packs" / adapter / VERSION)
        assert plan["runtime_home"] == str(newer)
        assert plan["java_home"] == str(newer / "jre")
        assert plan["classpath_dirs"][0] == str(newer / "jars")


def test_versions_compare_numerically_not_as_text(installed, tmp_path):
    newer = _stage_runtime(installed, tmp_path / "build", "0.1000.0")
    assert _plan(installed / "packs" / "jdbc" / VERSION)["runtime_home"] == str(newer)
    pack = packs.load_pack(installed / "packs" / "jdbc" / VERSION)
    assert packs.runtime_home_for(pack) == newer


def test_a_runtime_release_on_the_line_starts_the_packs_unchanged(installed, tmp_path):
    pack_dirs = [installed / "packs" / a / VERSION for a in OWN]
    before = {p: _tree_digest(p) for p in pack_dirs}
    shutil.rmtree(installed / "runtimes" / VERSION)
    released = _stage_runtime(installed, tmp_path / "build", "0.124.0")
    assert {p: _tree_digest(p) for p in pack_dirs} == before
    for pack in pack_dirs:
        assert _plan(pack)["runtime_home"] == str(released)


def test_a_pack_with_no_runtime_on_its_line_is_refused_at_start_naming_the_line(installed, tmp_path):
    shutil.rmtree(installed / "runtimes" / VERSION)
    _stage_runtime(installed, tmp_path / "build", "0.130.0", line=LINE + 1)
    done = _start(installed / "packs" / "file" / VERSION)
    assert done.returncode == 2
    assert (f"pgwire-file {VERSION} requires a pgwire-calcite runtime on line {LINE}, and none is "
            f"installed under {installed / 'runtimes'}") in done.stderr
    assert done.stdout == ""


def test_a_package_manager_names_the_runtimes_directory(installed, tmp_path):
    """Homebrew and Scoop install the runtime formula in its own prefix (PGWIRE_CALCITE_RUNTIMES)."""
    elsewhere = tmp_path / "runtime-formula" / "runtimes"
    elsewhere.parent.mkdir()
    (installed / "runtimes").rename(elsewhere)
    done = _start(installed / "packs" / "file" / VERSION, env={"PGWIRE_CALCITE_RUNTIMES": str(elsewhere),
                                                              "PGWIRE_CALCITE_STATE_DIR": "/state"})
    assert done.returncode == 0, done.stderr
    assert json.loads(done.stdout)["runtime_home"] == str(elsewhere / VERSION)
    assert _start(installed / "packs" / "file" / VERSION).returncode == 2


def test_the_python_side_refuses_a_runtime_off_the_packs_line(installed, monkeypatch, tmp_path):
    other = _stage_runtime(installed, tmp_path / "build", "0.124.0", line=LINE + 1)
    monkeypatch.setenv(packs.RUNTIME_HOME_ENV, str(other))
    with pytest.raises(packs.PackError, match=f"requires a pgwire-calcite runtime on line {LINE}, but "
                                              f"was started by runtime 0.124.0 on line {LINE + 1}"):
        packs.prepare(str(installed / "packs" / "file" / VERSION), [])


def test_the_python_side_refuses_an_older_runtime_on_the_line(installed, monkeypatch, tmp_path):
    _stage_runtime(installed, tmp_path / "build", "0.124.0")
    monkeypatch.setenv(packs.RUNTIME_HOME_ENV, str(installed / "runtimes" / VERSION))
    with pytest.raises(packs.PackError, match=f"newest runtime on line {LINE}, which is 0.124.0"):
        packs.prepare(str(installed / "packs" / "file" / VERSION), [])


def test_the_python_side_refuses_a_pack_built_for_another_platform(installed, monkeypatch):
    pack = installed / "packs" / "file" / VERSION
    manifest = json.loads((pack / "pack.json").read_text())
    (pack / "pack.json").write_text(json.dumps(dict(manifest, variant="macos-arm64")))
    monkeypatch.setenv(packs.RUNTIME_HOME_ENV, str(installed / "runtimes" / VERSION))
    with pytest.raises(packs.PackError, match="is built for macos-arm64"):
        packs.prepare(str(pack), [])


def test_runtime_home_for_names_the_missing_line(installed):
    pack = packs.load_pack(installed / "packs" / "jdbc" / VERSION)
    assert packs.runtime_home_for(pack) == installed / "runtimes" / VERSION
    (installed / "runtimes" / VERSION / "runtime.json").unlink()
    with pytest.raises(packs.PackError, match=f"runtime on line {LINE}, and none is installed"):
        packs.runtime_home_for(pack)


def test_the_pack_records_the_runtime_jar_names_it_was_built_against(installed):
    runtime_jars = sorted(j.name for j in (installed / "runtimes" / VERSION / "jars").glob("*.jar"))
    assert runtime_jars
    for adapter in OWN:
        manifest = json.loads((installed / "packs" / adapter / VERSION / "pack.json").read_text())
        assert manifest["runtime_jars"] == runtime_jars


def test_a_runtime_holding_every_recorded_jar_starts(installed):
    for adapter in OWN:
        assert _plan(installed / "packs" / adapter / VERSION)["runtime_home"] == str(
            installed / "runtimes" / VERSION)


def test_a_runtime_missing_a_recorded_jar_is_refused_at_start_naming_it(installed):
    gone = COMMON[0]
    (installed / "runtimes" / VERSION / "jars" / gone).unlink()
    done = _start(installed / "packs" / "file" / VERSION)
    assert done.returncode == 2
    assert (f"was built against runtime jar {gone}, which the newest runtime on line {LINE} "
            f"({VERSION}") in done.stderr
    assert done.stdout == ""
    pack = packs.load_pack(installed / "packs" / "file" / VERSION)
    with pytest.raises(packs.PackError, match=f"runtime jar {gone}"):
        packs.runtime_home_for(pack)


def test_the_newest_runtime_missing_a_jar_starts_the_older_one_that_has_every_jar(installed, tmp_path):
    newer = _stage_runtime(installed, tmp_path / "build", "0.130.1")
    (newer / "jars" / COMMON[0]).unlink()
    for adapter in OWN:
        plan = _plan(installed / "packs" / adapter / VERSION)
        assert plan["runtime_home"] == str(installed / "runtimes" / VERSION)
    pack = packs.load_pack(installed / "packs" / "file" / VERSION)
    assert packs.runtime_home_for(pack) == installed / "runtimes" / VERSION


def test_the_python_side_refuses_a_runtime_missing_a_recorded_jar(installed, monkeypatch):
    gone = COMMON[0]
    (installed / "runtimes" / VERSION / "jars" / gone).unlink()
    monkeypatch.setenv(packs.RUNTIME_HOME_ENV, str(installed / "runtimes" / VERSION))
    with pytest.raises(packs.PackError, match=f"runtime jar {gone}, which runtime {VERSION}"):
        packs.prepare(str(installed / "packs" / "file" / VERSION), [])


def test_a_pack_manifest_without_runtime_jars_is_refused(installed):
    pack = installed / "packs" / "file" / VERSION
    manifest = json.loads((pack / "pack.json").read_text())
    del manifest["runtime_jars"]
    (pack / "pack.json").write_text(json.dumps(manifest))
    with pytest.raises(packs.PackError, match="no 'runtime_jars'"):
        packs.load_pack(pack)


def test_a_pack_manifest_without_a_runtime_line_is_refused(installed):
    pack = installed / "packs" / "file" / VERSION
    manifest = json.loads((pack / "pack.json").read_text())
    del manifest["runtime_line"]
    (pack / "pack.json").write_text(json.dumps(dict(manifest, runtime=VERSION)))
    with pytest.raises(packs.PackError, match="names no runtime line 'runtime_line'"):
        packs.load_pack(pack)


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


def test_pack_jars_refuses_a_pack_jar_the_runtime_also_holds(tmp_path):
    runtime = _jar_dir(tmp_path / "runtime", COMMON)
    adapter = _jar_dir(tmp_path / "adapter", COMMON + OWN["file"])
    out = _jar_dir(tmp_path / "out", ["avatica-core-1.26.0.jar"])
    with pytest.raises(SystemExit, match=r"holds a jar the runtime also holds: avatica-core-1.26.0.jar "
                                         r"\(runtime has avatica-core-1.26.0.jar\)"):
        runtime_packs.pack_jars(runtime, adapter, out)


def test_pack_jars_refuses_another_version_of_a_runtime_jar_already_in_the_pack(tmp_path):
    runtime = _jar_dir(tmp_path / "runtime", COMMON)
    adapter = _jar_dir(tmp_path / "adapter", COMMON + OWN["file"])
    out = _jar_dir(tmp_path / "out", ["jackson-core-2.19.0.jar"])
    with pytest.raises(SystemExit, match=r"jackson-core-2.19.0.jar \(runtime has jackson-core-2.18.4.jar\)"):
        runtime_packs.pack_jars(runtime, adapter, out)


def test_pack_jars_keeps_only_the_adapters_own_jars(tmp_path):
    runtime = _jar_dir(tmp_path / "runtime", COMMON)
    adapter = _jar_dir(tmp_path / "adapter", COMMON + OWN["file"])
    assert runtime_packs.pack_jars(runtime, adapter, tmp_path / "out") == sorted(OWN["file"])
    assert sorted(p.name for p in (tmp_path / "out").iterdir()) == sorted(OWN["file"])


def test_a_pack_holding_a_runtime_part_is_refused(tmp_path):
    pack = tmp_path / "pack"
    (pack / "model").mkdir(parents=True)
    (pack / "model" / "model.json").write_text("{}")
    (pack / "jre").mkdir()
    with pytest.raises(SystemExit, match="holds jre, which belongs to the runtime"):
        runtime_packs.write_pack(pack, "file", VERSION, LINE, VARIANT, ["a.jar"], [])


@pytest.mark.parametrize("line", [0, -1, "1", True])
def test_a_line_that_is_not_a_positive_integer_is_refused(tmp_path, line):
    pack = tmp_path / "pack"
    (pack / "model").mkdir(parents=True)
    (pack / "model" / "model.json").write_text("{}")
    with pytest.raises(SystemExit, match="is not a positive integer"):
        runtime_packs.write_pack(pack, "file", VERSION, line, VARIANT, ["a.jar"], [])


def test_a_runtime_version_that_is_not_major_minor_patch_is_refused(tmp_path):
    home = tmp_path / "runtimes" / "0.1.0-alpha"
    for d in ("jre", "cpython", "jars"):
        (home / d).mkdir(parents=True)
    (home / "pgwire-launcher.jar").write_bytes(b"")
    with pytest.raises(SystemExit, match="is not a release version"):
        runtime_packs.write_runtime(home, "0.1.0-alpha", VARIANT, LINE)


def test_windows_launchers_are_batch_files_with_the_pack_values_filled(tmp_path):
    pack = tmp_path / "pack"
    (pack / "model").mkdir(parents=True)
    (pack / "model" / "model.json").write_text("{}")
    runtime_packs.write_pack(pack, "govdata", VERSION, LINE, "windows-x86_64",
                             ["a.jar", "b.jar"], ["--auth", "askamerica-key"])
    bat = (pack / "bin" / "pgwire-govdata.bat").read_bytes().decode("ascii")
    assert "\r\n" in bat and "@" + "LINE@" not in bat
    assert f'findstr /L /C:"\\"line\\": {LINE}," ' in bat
    assert f"requires a pgwire-calcite runtime on line {LINE}, and none is installed" in bat
    assert "PGWIRE_CALCITE_RUNTIMES" in bat
    assert 'set "JARS=a.jar b.jar"' in bat


# --- package-manager formulas ------------------------------------------------------------


def _sums(path: pathlib.Path, version: str, adapters) -> pathlib.Path:
    path.mkdir()
    variants = [p["variant"] for p in json.loads((PACKAGING / "platforms.json").read_text())]
    parts = ["pgwire-calcite-runtime"] + [f"pgwire-{a}-pack" for a in adapters]
    for part in parts:
        for variant in variants:
            name = f"{part}-{version}-{variant}.tar.gz"
            digest = hashlib.sha256(name.encode()).hexdigest()
            (path / f"{name}.sha256").write_text(f"{digest}  {name}\n")
    return path


def _digest(version: str, part: str, variant: str) -> str:
    return hashlib.sha256(f"{part}-{version}-{variant}.tar.gz".encode()).hexdigest()


def test_formulas_are_one_runtime_formula_plus_one_per_pack_depending_on_it(tmp_path):
    adapters = [a["name"] for a in json.loads((PACKAGING / "adapters.json").read_text())]
    version = "0.130.0"
    sums = _sums(tmp_path / "sums", version, adapters)
    written = runtime_packs.write_formulas(tmp_path / "out", version, sums, [])
    brew, scoop = tmp_path / "out" / "homebrew", tmp_path / "out" / "scoop"
    assert sorted(p.name for p in brew.iterdir()) == sorted(
        ["pgwire-calcite-runtime.rb"] + [f"pgwire-{a}.rb" for a in adapters])
    assert sorted(p.name for p in scoop.iterdir()) == sorted(
        ["pgwire-calcite-runtime.json"] + [f"pgwire-{a}.json" for a in adapters])
    assert len(written) == 2 * (1 + len(adapters))

    runtime_rb = (brew / "pgwire-calcite-runtime.rb").read_text()
    assert "class PgwireCalciteRuntime < Formula" in runtime_rb
    assert "depends_on" not in runtime_rb
    assert f"/engine-v{version}/pgwire-calcite-runtime-{version}-linux-aarch64.tar.gz" in runtime_rb
    assert _digest(version, "pgwire-calcite-runtime", "macos-arm64") in runtime_rb

    for adapter in adapters:
        rb = (brew / f"pgwire-{adapter}.rb").read_text()
        assert f"class {''.join(w.capitalize() for w in ('pgwire-' + adapter).split('-'))} < Formula" in rb
        assert 'depends_on "pgwire-calcite-runtime"' in rb
        assert "pgwire-calcite-runtime-" not in rb, "a pack formula carries no runtime tarball"
        assert _digest(version, f"pgwire-{adapter}-pack", "linux-x86_64") in rb
        assert 'PGWIRE_CALCITE_RUNTIMES="#{Formula["pgwire-calcite-runtime"].opt_libexec}/runtimes"' in rb
        assert f'#{{libexec}}/packs/{adapter}/#{{version}}/bin/pgwire-{adapter}' in rb

        manifest = json.loads((scoop / f"pgwire-{adapter}.json").read_text())
        assert manifest["depends"] == "pgwire-calcite-runtime"
        assert manifest["architecture"]["64bit"]["url"].endswith(
            f"pgwire-{adapter}-pack-{version}-windows-x86_64.tar.gz")
        assert manifest["architecture"]["64bit"]["hash"] == _digest(
            version, f"pgwire-{adapter}-pack", "windows-x86_64")
        assert manifest["bin"] == f"pgwire-{adapter}.bat"
        script = "\n".join(manifest["installer"]["script"])
        assert "pgwire-calcite-runtime\\current\\runtimes" in script
        assert f"packs\\{adapter}\\' + $version + '\\bin\\pgwire-{adapter}.bat" in script

    runtime_json = json.loads((scoop / "pgwire-calcite-runtime.json").read_text())
    assert "depends" not in runtime_json and "bin" not in runtime_json
    assert runtime_json["architecture"]["64bit"]["hash"] == _digest(
        version, "pgwire-calcite-runtime", "windows-x86_64")


def test_formulas_leave_out_what_is_not_released_at_the_version(tmp_path):
    version = "0.110.0"  # before jira (0.119.0) and linux-aarch64 (0.128.0)
    sums = _sums(tmp_path / "sums", version, ["file", "jira"])
    runtime_packs.write_formulas(tmp_path / "out", version, sums, ["file"])
    rb = (tmp_path / "out" / "homebrew" / "pgwire-file.rb").read_text()
    assert "linux-aarch64" not in rb and "linux-x86_64" in rb
    with pytest.raises(SystemExit, match="no adapter released at 0.110.0 is named jira"):
        runtime_packs.write_formulas(tmp_path / "out2", version, sums, ["jira"])


def test_formulas_refuse_a_tarball_without_its_sha256(tmp_path):
    version = "0.130.0"
    sums = _sums(tmp_path / "sums", version, ["file"])
    (sums / f"pgwire-file-pack-{version}-macos-arm64.tar.gz.sha256").unlink()
    with pytest.raises(SystemExit, match=f"pgwire-file-pack-{version}-macos-arm64.tar.gz.sha256 is missing"):
        runtime_packs.write_formulas(tmp_path / "out", version, sums, ["file"])
