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
after the child is recycled), the full pgwire->bridge->child path, and
cancellation/statement_timeout across the bridge (PGW-050/051).
"""

from __future__ import annotations

import threading
import time

import psycopg
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
    assert list(r.iter_rows()) == [(10,)]
    assert r.column_names == ["N"]


def test_bridge_streams_large_result_over_socket(child):
    port, _ = child
    b = BridgeBackend(port=port)
    r = b.execute_sql(
        "SELECT count(*) AS n FROM (SELECT e1.EMPNO FROM EMPS e1, EMPS e2, EMPS e3, EMPS e4) t", "u"
    )
    assert list(r.iter_rows()) == [(10000,)]


def test_bridge_join_aggregate(child):
    port, _ = child
    b = BridgeBackend(port=port)
    r = b.execute_sql(
        "SELECT d.DNAME, count(*) AS c FROM EMPS e JOIN DEPTS d ON e.DEPTNO=d.DEPTNO "
        "GROUP BY d.DNAME ORDER BY d.DNAME",
        "u",
    )
    assert list(r.iter_rows()) == [("ACCOUNTING", 2), ("RESEARCH", 3), ("SALES", 5)]


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
        list(b.execute_sql("SELECT * FROM no_such_table_xyz", "u").iter_rows())
    assert "calcite" in str(exc.value).lower()


def test_bridge_reconnects_after_calcite_recycle(calcite_backend):
    """PGW-037: recycling the Calcite child (server restart) fails only in-flight;
    the next query reconnects to the fresh child."""
    port = _free_port()
    srv = serve_calcite_child(calcite_backend, host="127.0.0.1", port=port)
    time.sleep(0.1)
    b = BridgeBackend(port=port, connect_retries=5, connect_backoff=0.1)
    assert list(b.execute_sql("SELECT 1 AS n", "u").iter_rows()) == [(1,)]
    # recycle: stop the child...
    srv.shutdown()
    srv.server_close()
    assert b.ready() is False
    # ...and bring a fresh one up on the same port; idle bridge reconnects.
    srv2 = serve_calcite_child(calcite_backend, host="127.0.0.1", port=port)
    time.sleep(0.1)
    try:
        assert list(b.execute_sql("SELECT 2 AS n", "u").iter_rows()) == [(2,)]
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
        assert list(b.execute_sql("SELECT count(*) AS n FROM EMPS", "u").iter_rows()) == [(10,)]
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


# --- cancellation and statement_timeout over the bridge (PGW-050/051) --------

#: A 7-way self cross join over the 10-row EMPS fixture: 10^7 rows through linq4j,
#: measured at ~156s uncancelled. Any assertion below that finishes in seconds
#: proves the child actually stopped executing.
SLOW_SQL = "SELECT count(*) AS n FROM EMPS a, EMPS b, EMPS c, EMPS d, EMPS e, EMPS f, EMPS g"


def _wait_for_in_flight(timeout_s: float = 30.0) -> set:
    """Block until the Calcite child has registered a statement."""
    from pgwire_calcite.calcite_backend import IN_FLIGHT

    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        active = IN_FLIGHT.active_sessions()
        if active:
            return active
        time.sleep(0.01)
    raise AssertionError("no statement reached the child's in-flight registry")


def test_bridge_statement_timeout_stops_the_child(child):
    """statement_timeout is enforced inside the child and comes back as 57014,
    not as a socket timeout with the child still executing."""
    from pgwire_calcite.backend import CANCELED_BY_TIMEOUT, QueryCanceled

    port, _ = child
    b = BridgeBackend(port=port)
    started = time.monotonic()
    with pytest.raises(QueryCanceled) as excinfo:
        list(b.execute_sql(SLOW_SQL, "u", session_key="sess-timeout", timeout_ms=500).iter_rows())
    assert excinfo.value.sqlstate == "57014"
    assert str(excinfo.value) == CANCELED_BY_TIMEOUT
    assert time.monotonic() - started < 60, "the child kept executing past the timeout"


def test_bridge_cancel_session_aborts_an_in_flight_query(child):
    """A cancel crosses the bridge on its own connection while the query's
    connection is still streaming."""
    from pgwire_calcite.backend import CANCELED_BY_USER, QueryCanceled

    port, _ = child
    b = BridgeBackend(port=port)
    outcome = {}

    def _run():
        try:
            list(b.execute_sql(SLOW_SQL, "u", session_key="sess-cancel").iter_rows())
            outcome["error"] = None
        except BaseException as exc:  # recorded, asserted on the main thread
            outcome["error"] = exc

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    _wait_for_in_flight()
    assert b.cancel_session("sess-cancel", CANCELED_BY_USER) is True
    t.join(timeout=60)
    assert not t.is_alive(), "the child kept executing after the cancel"
    assert isinstance(outcome["error"], QueryCanceled)
    assert outcome["error"].sqlstate == "57014"


def test_bridge_cancel_of_an_idle_session_reports_nothing_in_flight(child):
    from pgwire_calcite.backend import CANCELED_BY_USER

    port, _ = child
    assert BridgeBackend(port=port).cancel_session("no-such-session", CANCELED_BY_USER) is False


def test_cancel_request_over_the_wire_reaches_the_child(child):
    """End to end: psycopg's cancel on a pgwire connection served by the bridge
    backend aborts the statement running in the Calcite child, and the session
    stays usable afterwards."""
    port, _ = child
    backend = BridgeBackend(port=port)
    wport = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=wport, auth="none", backend=backend)
    time.sleep(0.1)
    try:
        conn = psycopg.connect(
            f"host=127.0.0.1 port={wport} user=tester dbname=postgres", autocommit=True
        )
        try:
            failure = {}

            def _run():
                try:
                    with conn.cursor() as cur:
                        cur.execute(SLOW_SQL)
                        cur.fetchall()
                    failure["error"] = None
                except BaseException as exc:
                    failure["error"] = exc

            t = threading.Thread(target=_run, daemon=True)
            t.start()
            _wait_for_in_flight()
            # cancel_safe, not the older cancel(): psycopg's binary pq makes
            # PQcancel a blocking C call that holds the GIL, so this in-process
            # server thread could never answer the CancelRequest.
            conn.cancel_safe(timeout=60)
            t.join(timeout=120)
            assert not t.is_alive(), "the child kept executing after the CancelRequest"
            assert failure["error"] is not None
            assert getattr(failure["error"], "sqlstate", None) == "57014", failure["error"]
            # PG keeps a cancelled backend serving
            with conn.cursor() as cur:
                cur.execute("SELECT 1 AS n")
                assert cur.fetchall() == [(1,)]
        finally:
            conn.close()
    finally:
        srv.shutdown()
