# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cancellation, statement_timeout and session reset (PGW-050/051/052).

Exercised through a real psycopg3 client against the wire server so the wire
contract is what is asserted, not the internals: a CancelRequest on a second
connection aborts the first connection's running query with SQLSTATE 57014 and
leaves it usable; SET statement_timeout is honored per session and reported by
SHOW; DISCARD ALL puts the session back to its startup settings.

Skips only if the Calcite classpath cannot be resolved (conftest's fixture).
"""

from __future__ import annotations

import threading
import time

import psycopg
import pytest

from pgwire_calcite import launcher
from pgwire_calcite.backend import CANCELED_BY_TIMEOUT, CANCELED_BY_USER
from pgwire_calcite.calcite_backend import IN_FLIGHT, InFlightRegistry
from pgwire_calcite.server import _format_statement_timeout, _parse_statement_timeout

from test_phase0_wire import _free_port

#: A 7-way self cross join over the 10-row EMPS fixture: 10^7 rows through
#: linq4j. Measured at ~156s uncancelled, so every assertion below that finishes
#: in seconds proves the cancel/timeout actually stopped the engine.
SLOW_SQL = (
    "SELECT count(*) AS n FROM EMPS a, EMPS b, EMPS c, EMPS d, EMPS e, EMPS f, EMPS g"
)
#: 10^5 rows, measured at ~1.7s: long enough that a 200ms timeout must interrupt
#: it, short enough that "no timeout" can be proved by letting it finish.
MEDIUM_SQL = "SELECT count(*) AS n FROM EMPS a, EMPS b, EMPS c, EMPS d, EMPS e"
MEDIUM_ROWS = 100000


@pytest.fixture(scope="module")
def server(calcite_backend):
    port = _free_port()
    srv = launcher.serve(
        host="127.0.0.1", port=port, auth="none", backend=calcite_backend, statement_timeout_ms=0
    )
    time.sleep(0.2)
    yield port
    srv.shutdown()
    srv.server_close()


def _connect(port: int) -> psycopg.Connection:
    return psycopg.connect(
        f"host=127.0.0.1 port={port} user=tester dbname=postgres", autocommit=True
    )


def _wait_for_in_flight(timeout_s: float = 30.0) -> set:
    """Block until the server has a statement registered, so cancel can't race."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        active = IN_FLIGHT.active_sessions()
        if active:
            return active
        time.sleep(0.01)
    raise AssertionError("no statement reached the in-flight registry")


# --- pure units: no JVM, no socket ------------------------------------------


@pytest.mark.parametrize(
    "value,expected_ms",
    [
        ("0", 0),
        ("100", 100),
        ("'100ms'", 100),
        ("'2s'", 2000),
        ("'1min'", 60000),
        ("'1h'", 3600000),
        ("'2000us'", 2),
    ],
)
def test_statement_timeout_values_parse_with_pg_units(value, expected_ms):
    assert _parse_statement_timeout(value) == expected_ms


def test_statement_timeout_rejects_a_value_it_cannot_mean():
    from pgwire_calcite.backend import InvalidParameterValue

    with pytest.raises(InvalidParameterValue) as excinfo:
        _parse_statement_timeout("'later'")
    assert excinfo.value.sqlstate == "22023"


@pytest.mark.parametrize(
    "ms,shown", [(0, "0"), (100, "100ms"), (2000, "2s"), (60000, "1min"), (1500, "1500ms")]
)
def test_statement_timeout_is_shown_the_way_postgres_shows_it(ms, shown):
    assert _format_statement_timeout(ms) == shown


class _FakeStatement:
    def __init__(self):
        self.cancelled = 0

    def cancel(self):
        self.cancelled += 1


def test_registry_cancels_once_and_records_the_reason(monkeypatch):
    monkeypatch.setattr(
        "pgwire_calcite.calcite_backend._attach_current_thread_to_jvm", lambda: None
    )
    registry = InFlightRegistry()
    stmt = _FakeStatement()
    handle = registry.begin("sess-1", stmt)

    assert registry.active_sessions() == {"sess-1"}
    assert registry.cancel("sess-1", CANCELED_BY_USER) is True
    assert registry.cancel("sess-1", CANCELED_BY_TIMEOUT) is False  # already cancelled
    assert stmt.cancelled == 1
    assert handle.reason == CANCELED_BY_USER

    registry.end("sess-1", handle)
    assert registry.active_sessions() == set()
    assert registry.cancel("sess-1", CANCELED_BY_USER) is False  # nothing in flight


# --- through the wire --------------------------------------------------------


def test_backend_key_data_is_delivered_and_per_session(server):
    with _connect(server) as one, _connect(server) as two:
        assert one.info.backend_pid != 0
        assert two.info.backend_pid != 0
        assert one.info.backend_pid != two.info.backend_pid


def test_cancel_request_aborts_the_query_and_the_session_survives(server):
    conn = _connect(server)
    try:
        failure: dict = {}

        def run_slow():
            try:
                conn.execute(SLOW_SQL).fetchone()
                failure["result"] = "completed"
            except Exception as exc:  # noqa: BLE001 - recorded and asserted below
                failure["exc"] = exc

        worker = threading.Thread(target=run_slow)
        started = time.monotonic()
        worker.start()
        _wait_for_in_flight()
        # cancel_safe (not the older cancel()) because this server runs in the
        # test process: psycopg's binary pq implementation makes PQcancel a
        # blocking C call that holds the GIL, so the in-process server thread
        # could never answer the CancelRequest. cancel_safe does the same
        # protocol exchange over non-blocking I/O.
        conn.cancel_safe(timeout=60)
        worker.join(120)

        assert not worker.is_alive(), "cancelled query never returned"
        assert "result" not in failure, "query ran to completion instead of being cancelled"
        exc = failure["exc"]
        assert isinstance(exc, psycopg.errors.QueryCanceled), repr(exc)
        assert exc.sqlstate == "57014"
        assert CANCELED_BY_USER in str(exc)
        assert time.monotonic() - started < 120

        # PG does not disconnect a cancelled backend: the session keeps serving.
        assert conn.execute("SELECT 1").fetchone() == (1,)
    finally:
        conn.close()


def test_cancel_request_with_a_wrong_secret_key_is_ignored(server):
    with _connect(server) as conn:
        import socket
        import struct

        sock = socket.create_connection(("127.0.0.1", server))
        try:
            # backend_pid is signed here and unsigned on the wire; "i" round-trips
            # the same 32 bits either way.
            sock.sendall(struct.pack("!iiiI", 16, 80877102, conn.info.backend_pid, 0))
        finally:
            sock.close()
        assert conn.execute("SELECT 1").fetchone() == (1,)


def test_statement_timeout_cancels_the_query_for_this_session_only(server):
    with _connect(server) as timed, _connect(server) as untimed:
        timed.execute("SET statement_timeout = 100")
        assert timed.execute("SHOW statement_timeout").fetchone() == ("100ms",)
        # The setting is per session, not per server.
        assert untimed.execute("SHOW statement_timeout").fetchone() == ("0",)

        started = time.monotonic()
        with pytest.raises(psycopg.errors.QueryCanceled) as excinfo:
            timed.execute(SLOW_SQL).fetchone()
        elapsed = time.monotonic() - started

        assert excinfo.value.sqlstate == "57014"
        assert CANCELED_BY_TIMEOUT in str(excinfo.value)
        assert elapsed < 120, f"timeout did not stop the engine ({elapsed:.1f}s)"
        assert timed.execute("SELECT 1").fetchone() == (1,)


def test_statement_timeout_zero_means_no_timeout(server):
    """Same query, same session: cancelled under a timeout, completes under 0."""
    with _connect(server) as conn:
        conn.execute("SET statement_timeout = 200")
        with pytest.raises(psycopg.errors.QueryCanceled):
            conn.execute(MEDIUM_SQL).fetchone()

        conn.execute("SET statement_timeout = 0")
        assert conn.execute("SHOW statement_timeout").fetchone() == ("0",)
        assert conn.execute(MEDIUM_SQL).fetchone() == (MEDIUM_ROWS,)


def test_statement_timeout_is_readable_through_current_setting(server):
    with _connect(server) as conn:
        conn.execute("SET statement_timeout = '2s'")
        assert conn.execute("SELECT current_setting('statement_timeout')").fetchone() == ("2s",)


def test_reset_returns_a_setting_to_the_server_default(server):
    with _connect(server) as conn:
        conn.execute("SET statement_timeout = 750")
        conn.execute("RESET statement_timeout")
        assert conn.execute("SHOW statement_timeout").fetchone() == ("0",)


def test_discard_all_clears_session_settings(server):
    with _connect(server) as conn:
        conn.execute("SET statement_timeout = 250")
        conn.execute("SET application_name = 'phase9'")
        assert conn.execute("SHOW statement_timeout").fetchone() == ("250ms",)
        assert conn.execute("SHOW application_name").fetchone() == ("phase9",)

        conn.execute("DISCARD ALL")

        assert conn.execute("SHOW statement_timeout").fetchone() == ("0",)
        assert conn.execute("SHOW application_name").fetchone() == ("",)


def test_discard_all_survives_and_still_runs_queries(server):
    with _connect(server) as conn:
        conn.execute("DISCARD ALL")
        assert conn.execute("SELECT count(*) AS n FROM EMPS").fetchone() == (10,)


# --- session-command bookkeeping the wire cannot observe directly ------------


class _FakeCtx:
    def __init__(self):
        self.stmts = {"s1": ("SELECT 1", [])}
        self.portals = {"p1": ("s1", [], [])}
        self.result_cache = {"p1": object()}


def test_discard_all_closes_prepared_statements_and_portals():
    from pgwire_calcite.server import CalciteSession

    session = CalciteSession()
    ctx = _FakeCtx()
    session.bind_context(ctx)
    session.apply_session_command("SET statement_timeout = 900")

    session.apply_session_command("DISCARD ALL")

    assert ctx.stmts == {}
    assert ctx.portals == {}
    assert ctx.result_cache == {}
    assert session.statement_timeout_ms == 0
    assert session.settings["statement_timeout"] == "0"


def test_deallocate_all_closes_prepared_statements_but_keeps_settings():
    from pgwire_calcite.server import CalciteSession

    session = CalciteSession()
    ctx = _FakeCtx()
    session.bind_context(ctx)
    session.apply_session_command("SET statement_timeout = 900")

    session.apply_session_command("DEALLOCATE ALL")

    assert ctx.stmts == {}
    assert ctx.portals == {}
    assert session.statement_timeout_ms == 900


def test_deallocate_one_closes_only_that_statement():
    from pgwire_calcite.server import CalciteSession

    session = CalciteSession()
    ctx = _FakeCtx()
    ctx.stmts["s2"] = ("SELECT 2", [])
    session.bind_context(ctx)

    session.apply_session_command("DEALLOCATE s1")

    assert set(ctx.stmts) == {"s2"}


def test_server_default_statement_timeout_is_configurable():
    from pgwire_calcite.launcher import build_state

    assert build_state(statement_timeout_ms=1500).statement_timeout_ms == 1500
    assert build_state().statement_timeout_ms == 0  # PG's default: no timeout


def test_a_new_session_starts_at_the_server_default(monkeypatch):
    import pgwire_calcite.server as server_mod
    from pgwire_calcite.launcher import build_state
    from pgwire_calcite.server import CalciteSession

    monkeypatch.setattr(server_mod, "state", build_state(statement_timeout_ms=1500))
    session = CalciteSession()

    assert session.statement_timeout_ms == 1500
    assert session.settings["statement_timeout"] == "1500ms"
