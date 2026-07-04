# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Phase 5 topology tests: Calcite child + Arrow-IPC socket bridge (PGW-033/037).

Runs the Calcite child server against the shared JVM backend and drives it through
BridgeBackend: query execution (incl. a large streamed result over the socket),
readiness gating, PG-only reject at the bridge, lifecycle decoupling (reconnect
after the child is recycled), and the full pgwire->bridge->child path.
"""

from __future__ import annotations

import time

import pytest

from pgwire_calcite import launcher
from pgwire_calcite.dialect import UnsupportedConstruct
from pgwire_calcite.sidecar import BridgeBackend, serve_calcite_child

from test_phase0_wire import MiniPgClient, _free_port


@pytest.fixture()
def child(calcite_backend):
    port = _free_port()
    srv = serve_calcite_child(calcite_backend, host="127.0.0.1", port=port)
    time.sleep(0.1)
    yield port, srv
    srv.shutdown()
    srv.server_close()


def test_bridge_executes_query(child):
    port, _ = child
    b = BridgeBackend(port=port)
    r = b.execute_sql("SELECT count(*) AS n FROM EMPS", "u")
    assert list(r.rows) == [(10,)]
    assert r.column_names == ["N"]


def test_bridge_streams_large_result_over_socket(child):
    port, _ = child
    b = BridgeBackend(port=port)
    r = b.execute_sql(
        "SELECT count(*) AS n FROM (SELECT e1.EMPNO FROM EMPS e1, EMPS e2, EMPS e3, EMPS e4) t", "u"
    )
    assert list(r.rows) == [(10000,)]


def test_bridge_join_aggregate(child):
    port, _ = child
    b = BridgeBackend(port=port)
    r = b.execute_sql(
        "SELECT d.DNAME, count(*) AS c FROM EMPS e JOIN DEPTS d ON e.DEPTNO=d.DEPTNO "
        "GROUP BY d.DNAME ORDER BY d.DNAME",
        "u",
    )
    assert list(r.rows) == [("ACCOUNTING", 2), ("RESEARCH", 3), ("SALES", 5)]


def test_bridge_ready(child):
    port, _ = child
    assert BridgeBackend(port=port).ready() is True
    # a port with nothing listening -> not ready
    assert BridgeBackend(port=_free_port()).ready() is False


def test_bridge_rejects_pg_only_before_child(child):
    port, _ = child
    b = BridgeBackend(port=port)
    with pytest.raises(UnsupportedConstruct):
        b.execute_sql("SELECT DISTINCT ON (DEPTNO) DEPTNO FROM EMPS", "u")


def test_bridge_error_is_surfaced(child):
    port, _ = child
    b = BridgeBackend(port=port)
    with pytest.raises(RuntimeError) as exc:
        list(b.execute_sql("SELECT * FROM no_such_table_xyz", "u").rows)
    assert "calcite" in str(exc.value).lower()


def test_bridge_reconnects_after_calcite_recycle(calcite_backend):
    """PGW-037: recycling the Calcite child (server restart) fails only in-flight;
    the next query reconnects to the fresh child."""
    port = _free_port()
    srv = serve_calcite_child(calcite_backend, host="127.0.0.1", port=port)
    time.sleep(0.1)
    b = BridgeBackend(port=port, connect_retries=5, connect_backoff=0.1)
    assert list(b.execute_sql("SELECT 1 AS n", "u").rows) == [(1,)]
    # recycle: stop the child...
    srv.shutdown()
    srv.server_close()
    assert b.ready() is False
    # ...and bring a fresh one up on the same port; idle bridge reconnects.
    srv2 = serve_calcite_child(calcite_backend, host="127.0.0.1", port=port)
    time.sleep(0.1)
    try:
        assert list(b.execute_sql("SELECT 2 AS n", "u").rows) == [(2,)]
    finally:
        srv2.shutdown()
        srv2.server_close()


def test_real_calcite_child_subprocess():
    """Genuine two-process topology: spawn the Calcite child (its OWN JVM) as a
    subprocess and query it over the bridge (PGW-033: Calcite not in our process)."""
    import pathlib
    import subprocess
    import sys

    from pgwire_calcite.classpath import ClasspathError, resolve_classpath

    try:
        resolve_classpath()
    except ClasspathError as exc:
        pytest.skip(f"Calcite classpath unavailable: {exc}")

    root = pathlib.Path(__file__).resolve().parents[2]
    model = str(root / "tests" / "fixtures" / "file-model.json")
    port = _free_port()
    proc = subprocess.Popen(
        [sys.executable, "-m", "pgwire_calcite.calcite_child",
         "--model", model, "--port", str(port), "--xmx", "1g"],
        cwd=str(root), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
    )
    try:
        # wait for the readiness marker (JVM boot)
        ready = False
        deadline = time.time() + 60
        while time.time() < deadline:
            line = proc.stdout.readline()
            if not line and proc.poll() is not None:
                break
            if "CALCITE_CHILD_READY" in line:
                ready = True
                break
        assert ready, "child did not report ready"
        b = BridgeBackend(port=port, connect_retries=5)
        assert list(b.execute_sql("SELECT count(*) AS n FROM EMPS", "u").rows) == [(10,)]
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def test_catalog_over_bridge(child):
    """PGW-012 over the sidecar: the Calcite child ships its catalog model to the
    pgwire process, so discovery works in the bridge topology."""
    port, _ = child
    backend = BridgeBackend(port=port)
    # direct fetch of the serialized catalog
    ctx, column_types = backend.fetch_catalog()
    names = {tm.table_name for tm in ctx.tables.values()}
    assert "emps" in names and "depts" in names
    assert any(column_types.values())  # columns came across too

    # end-to-end: serve pgwire over the bridge; launcher populates the catalog
    wport = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=wport, auth="none", backend=backend)
    import pgwire_calcite.server as server_mod

    assert server_mod.state.catalog_enabled, "catalog should be populated over the bridge"
    time.sleep(0.1)
    try:
        c = MiniPgClient("127.0.0.1", wport)
        try:
            r = c.query(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema NOT IN ('pg_catalog','information_schema') ORDER BY table_name"
            )
            assert r["error"] is None, r["error"]
            assert [row[0] for row in r["rows"]] == ["depts", "emps"]
        finally:
            c.close()
    finally:
        srv.shutdown()


def test_pgwire_over_bridge_end_to_end(child):
    port, _ = child
    backend = BridgeBackend(port=port)
    wport = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=wport, auth="none", backend=backend)
    time.sleep(0.1)
    try:
        c = MiniPgClient("127.0.0.1", wport)
        try:
            r = c.query("SELECT DNAME FROM DEPTS ORDER BY DNAME")
            assert r["error"] is None, r["error"]
            assert [row[0] for row in r["rows"]] == [
                "ACCOUNTING",
                "OPERATIONS",
                "RESEARCH",
                "SALES",
            ]
        finally:
            c.close()
    finally:
        srv.shutdown()
