# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Two adapter packs started, for real, from one shared runtime (packs.py's layout).

The two adapters are the file adapter (the CI classpath, calcite-classpath.txt) and Calcite's JDBC
adapter over HSQLDB's SCOTT database (jdbc-pack-classpath.txt; both written by
.github/workflows/pgwire-calcite-ci.yml). The runtime takes the jars both hold; each pack takes
the rest. Each pack is started through its own launcher (bin/pgwire-<adapter>), which finds the
runtime, which starts the Java launcher, which starts the runtime's CPython; DuckDB then ATTACHes
each server (TYPE postgres) and reads one table. A Python-only runtime update follows, after which
both packs, untouched, start again from the updated runtime.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import pathlib
import shutil
import signal
import socket
import subprocess
import sys
import sysconfig
import time

import pytest

duckdb = pytest.importorskip("duckdb", reason="duckdb required for the ATTACH e2e")

from pgwire_calcite.classpath import ClasspathError, resolve_classpath

PROJECT = pathlib.Path(__file__).resolve().parents[2]
PACKAGING = PROJECT / "packaging"
FIXTURES = PROJECT / "tests" / "fixtures"
JDBC_CLASSPATH = PROJECT / "jdbc-pack-classpath.txt"
VERSION = "0.0.0"
LINE = 1
VARIANT = "test-variant"
START_SECONDS = 240


def _load_runtime_packs():
    spec = importlib.util.spec_from_file_location("runtime_packs", PACKAGING / "runtime_packs.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


runtime_packs = _load_runtime_packs()


def _dedupe(directory: pathlib.Path) -> None:
    subprocess.run([sys.executable, str(PROJECT / "scripts" / "dedupe_jars.py"), str(directory)],
                   check=True, capture_output=True)


def _adapter_jars(dest: pathlib.Path, entries) -> pathlib.Path:
    dest.mkdir(parents=True)
    for e in entries:
        p = pathlib.Path(e)
        if p.suffix == ".jar" and p.is_file() and not (dest / p.name).exists():
            shutil.copy2(p, dest / p.name)
    _dedupe(dest)
    return dest


def _java_home() -> pathlib.Path:
    home = os.environ.get("JAVA_HOME")
    assert home, "JAVA_HOME names the JDK the runtime's JRE stands in for"
    return pathlib.Path(home)


def _launcher_jar(dest: pathlib.Path, work: pathlib.Path) -> pathlib.Path:
    """packaging/build-launcher.sh, built into the runtime instead of the checkout."""
    classes = work / "launcher-classes"
    jdk = _java_home() / "bin"
    subprocess.run([str(jdk / "javac"), "--release", "17", "-d", str(classes),
                    str(PACKAGING / "launcher" / "pgwire" / "launch" / "Launcher.java")], check=True)
    subprocess.run([str(jdk / "jar"), "--create", "--file", str(dest), "--main-class",
                    "pgwire.launch.Launcher", "-C", str(classes), "."], check=True)
    return dest


def _runtime_cpython(dest: pathlib.Path) -> pathlib.Path:
    """A virtualenv of its own (so the update below changes only the runtime) that reads the
    packages of the interpreter running this test: pgwire_calcite and its dependencies."""
    subprocess.run([getattr(sys, "_base_executable", sys.executable), "-m", "venv", "--without-pip",
                    str(dest)], check=True)
    site = next(dest.glob("lib/python*/site-packages"))
    (site / "test-packages.pth").write_text(
        f"import site; site.addsitedir({sysconfig.get_paths()['purelib']!r})\n")
    return site


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _tree_digest(path: pathlib.Path) -> str:
    h = hashlib.sha256()
    for p in sorted(path.rglob("*")):
        h.update(str(p.relative_to(path)).encode())
        if p.is_file():
            h.update(p.read_bytes())
    return h.hexdigest()


@pytest.fixture(scope="module")
def installed(tmp_path_factory):
    try:
        file_entries = resolve_classpath()
    except ClasspathError as exc:
        pytest.skip(f"Calcite classpath unavailable: {exc}")
    assert JDBC_CLASSPATH.is_file(), (
        f"{JDBC_CLASSPATH} is missing; pgwire-calcite-ci.yml writes it beside calcite-classpath.txt")
    jdbc_entries = JDBC_CLASSPATH.read_text().strip().split(os.pathsep)

    tmp = tmp_path_factory.mktemp("runtime-packs")
    build = tmp / "build"
    file_jars = _adapter_jars(build / "file", file_entries)
    jdbc_jars = _adapter_jars(build / "jdbc", jdbc_entries)

    root = tmp / "root"
    runtime = root / "runtimes" / VERSION
    runtime.mkdir(parents=True)
    (runtime / "jre").symlink_to(_java_home())
    site = _runtime_cpython(runtime / "cpython")
    _launcher_jar(runtime / "pgwire-launcher.jar", tmp)
    runtime_packs.common_jars(runtime / "jars", [file_jars, jdbc_jars])
    runtime_packs.write_runtime(runtime, VERSION, VARIANT, LINE)

    file_pack = root / "packs" / "file" / VERSION
    runtime_packs.pack_jars(runtime / "jars", file_jars, file_pack / "jars")
    (file_pack / "model").mkdir(parents=True)
    (file_pack / "model" / "model.json").write_text(json.dumps({
        "version": "1.0", "defaultSchema": "SALES",
        "schemas": [{"name": "SALES", "type": "custom",
                     "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
                     "operand": {"directory": str(FIXTURES / "filedata"), "ephemeralCache": True,
                                 "baseDirectory": str(tmp / "file-base"),
                                 "executionEngine": "linq4j"}}]}))
    runtime_packs.write_pack(file_pack, "file", VERSION, LINE, VARIANT, ["--database", "file"])

    jdbc_pack = root / "packs" / "jdbc" / VERSION
    runtime_packs.pack_jars(runtime / "jars", jdbc_jars, jdbc_pack / "jars")
    (jdbc_pack / "model").mkdir(parents=True)
    (jdbc_pack / "model" / "model.json").write_text(json.dumps({
        "version": "1.0", "defaultSchema": "SCOTT",
        "schemas": [{"name": "SCOTT", "type": "jdbc", "jdbcUrl": "jdbc:hsqldb:res:scott",
                     "jdbcUser": "SCOTT", "jdbcPassword": "TIGER", "jdbcSchema": "SCOTT"}]}))
    runtime_packs.write_pack(jdbc_pack, "jdbc", VERSION, LINE, VARIANT, ["--database", "jdbc"])
    return {"root": root, "runtime": runtime, "site": site, "tmp": tmp,
            "packs": {"file": file_pack, "jdbc": jdbc_pack}}


def _serve_and_read(pack: pathlib.Path, adapter: str, sql: str, tmp: pathlib.Path, env=None):
    port = _free_port()
    log = (tmp / f"{adapter}-{port}.log").open("w")
    run_env = dict(os.environ, PGWIRE_CALCITE_STATE_DIR=str(tmp / f"state-{adapter}"), **(env or {}))
    for leaked in ("PGWIRE_CALCITE_CLASSPATH", "PGWIRE_CALCITE_CLASSPATH_FILE",
                   "PGWIRE_CALCITE_CLASSPATH_DIRS", "PGWIRE_CALCITE_RUNTIME_HOME"):
        run_env.pop(leaked, None)
    proc = subprocess.Popen(
        [str(pack / "bin" / f"pgwire-{adapter}"), "--host", "127.0.0.1", "--port", str(port),
         "--auth", "none"], stdout=log, stderr=subprocess.STDOUT, env=run_env,
        cwd=str(tmp), start_new_session=True)
    try:
        deadline = time.monotonic() + START_SECONDS
        con = duckdb.connect()
        con.execute("INSTALL postgres")
        con.execute("LOAD postgres")
        while True:
            assert proc.poll() is None, f"pgwire-{adapter} exited: {(tmp / log.name).read_text()}"
            try:
                con.execute(f"ATTACH 'host=127.0.0.1 port={port} user=tester dbname={adapter}' "
                            "AS pg (TYPE postgres, READ_ONLY)")
                break
            except duckdb.Error:
                assert time.monotonic() < deadline, (
                    f"pgwire-{adapter} did not accept a connection in {START_SECONDS}s: "
                    f"{pathlib.Path(log.name).read_text()}")
                time.sleep(1)
        rows = con.execute(sql).fetchall()
        con.close()
        return rows
    finally:
        os.killpg(proc.pid, signal.SIGTERM)
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait(timeout=10)
        log.close()


_FILE_SQL = 'SELECT "DNAME" FROM pg."SALES"."DEPTS" ORDER BY "DEPTNO"'
_JDBC_SQL = 'SELECT count(*) FROM pg."SCOTT"."EMP"'
_DEPTS = [("ACCOUNTING",), ("RESEARCH",), ("SALES",), ("OPERATIONS",)]


def test_two_packs_share_one_runtime_directory(installed):
    runtime = installed["runtime"]
    runtime_jars = {p.name for p in (runtime / "jars").glob("*.jar")}
    assert len(runtime_jars) > 50, "the runtime holds the jars common to both adapters"
    assert [p.name for p in (installed["root"] / "runtimes").iterdir()] == [VERSION]
    for adapter, pack in installed["packs"].items():
        own = {p.name for p in (pack / "jars").glob("*.jar")}
        assert own, f"the {adapter} pack holds jars of its own"
        assert not own & runtime_jars, f"the {adapter} pack repeats runtime jars: {own & runtime_jars}"
        for shared in ("jre", "cpython", "python", "pgwire-launcher.jar"):
            assert not (pack / shared).exists(), f"the {adapter} pack holds {shared}"


def test_each_pack_starts_from_the_runtime_and_serves_its_tables(installed):
    tmp = installed["tmp"]
    assert _serve_and_read(installed["packs"]["file"], "file", _FILE_SQL, tmp) == _DEPTS
    assert _serve_and_read(installed["packs"]["jdbc"], "jdbc", _JDBC_SQL, tmp) == [(14,)]


def test_a_python_only_runtime_update_leaves_both_packs_untouched_and_both_start(installed):
    tmp = installed["tmp"]
    packs_before = {a: _tree_digest(p) for a, p in installed["packs"].items()}
    jars_before = _tree_digest(installed["runtime"] / "jars")

    # The update: new Python code in the runtime's CPython, run at every interpreter start (it
    # records each start), and nothing else changed.
    marker = tmp / "runtime-update-starts.txt"
    (installed["site"] / "zz-runtime-update.pth").write_text(
        "import os, pathlib; p = os.environ.get('PGWIRE_RUNTIME_UPDATE_MARKER'); "
        "p and pathlib.Path(p).open('a').write('build-2\\n')\n")

    env = {"PGWIRE_RUNTIME_UPDATE_MARKER": str(marker)}
    assert _serve_and_read(installed["packs"]["file"], "file", _FILE_SQL, tmp, env) == _DEPTS
    assert _serve_and_read(installed["packs"]["jdbc"], "jdbc", _JDBC_SQL, tmp, env) == [(14,)]

    assert marker.read_text().count("build-2") >= 2, "both servers ran the updated runtime's Python"
    assert {a: _tree_digest(p) for a, p in installed["packs"].items()} == packs_before
    assert _tree_digest(installed["runtime"] / "jars") == jars_before


def test_a_pack_with_no_runtime_on_its_line_fails_at_start_naming_it(installed):
    pack = installed["packs"]["jdbc"]
    manifest = json.loads((pack / "pack.json").read_text())
    target = installed["root"] / "packs" / "jdbc" / "9.9.9"
    shutil.copytree(pack, target, symlinks=True)
    try:
        runtime_packs.write_pack(target, "jdbc", "9.9.9", LINE + 1, VARIANT, manifest["args"])
        done = subprocess.run([str(target / "bin" / "pgwire-jdbc"), "--port", str(_free_port())],
                              check=False, capture_output=True, text=True, timeout=60)
        assert done.returncode == 2
        assert f"requires a pgwire-calcite runtime on line {LINE + 1}, and none is installed" in done.stderr
    finally:
        shutil.rmtree(target)
