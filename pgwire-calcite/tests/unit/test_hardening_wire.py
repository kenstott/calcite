# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Wire-level robustness: what one misbehaving client can do to the server and to the
other clients sharing it.

No JVM: the backends here are test doubles. ``LaneBackend`` takes the engine the way the
real backends do (``CancelScope`` over one shared lock, a streamed result that keeps the
lock until it is closed), so the contention behaviour under test is the production code's.
"""

from __future__ import annotations

import socket
import struct
import threading
import time
from typing import Iterator, List, Optional

import pytest

from pgwire_calcite import launcher
from pgwire_calcite.arrow_bridge import _ClosingIterator
from pgwire_calcite.backend import (
    CANCELED_BY_USER,
    QueryCanceled,
    StubBackend,
)
from pgwire_calcite.calcite_backend import IN_FLIGHT, CancelScope, InFlightStatement
from pgwire_calcite.server import CalciteHandler
from pgwire_calcite.types import QueryResult

from test_phase0_wire import MiniPgClient, _free_port


# --- helpers ------------------------------------------------------------------


def error_fields(payload: bytes) -> dict:
    """An ErrorResponse body as {field code: value}."""
    fields = {}
    for part in payload.split(b"\x00"):
        if part:
            fields[chr(part[0])] = part[1:].decode("utf-8", errors="replace")
    return fields


class ExtClient(MiniPgClient):
    """MiniPgClient plus the extended protocol, one message at a time."""

    def send(self, msg_type: bytes, body: bytes = b"") -> None:
        self._send(msg_type, body)

    def parse(self, sql: str, name: str = "") -> None:
        self._send(b"P", name.encode() + b"\x00" + sql.encode() + b"\x00" + struct.pack("!h", 0))

    def bind(self, portal: str = "", stmt: str = "") -> None:
        self._send(
            b"B", portal.encode() + b"\x00" + stmt.encode() + b"\x00" + struct.pack("!hhh", 0, 0, 0)
        )

    def execute(self, portal: str = "", limit: int = 0) -> None:
        self._send(b"E", portal.encode() + b"\x00" + struct.pack("!i", limit))

    def close_target(self, kind: bytes, name: str = "") -> None:
        self._send(b"C", kind + name.encode() + b"\x00")

    def sync(self) -> List[tuple]:
        """Send Sync and return every message up to and including ReadyForQuery."""
        self._send(b"S", b"")
        out = []
        while True:
            mtype, payload = self._recv_msg()
            out.append((mtype, payload))
            if mtype == "Z":
                return out

    def extended(self, sql: str, limit: int = 0) -> List[tuple]:
        self.parse(sql)
        self.bind()
        self.execute(limit=limit)
        return self.sync()

    def is_closed_by_server(self, timeout_s: float = 5.0) -> bool:
        """True once the server has closed this connection."""
        self.sock.settimeout(timeout_s)
        try:
            while True:
                if self.sock.recv(65536) == b"":
                    return True
        except socket.timeout:
            return False
        except OSError:
            return True


def types_of(messages: List[tuple]) -> str:
    return "".join(m[0] for m in messages)


def first_error(messages: List[tuple]) -> dict:
    for mtype, payload in messages:
        if mtype == "E":
            return error_fields(payload)
    raise AssertionError(f"no ErrorResponse among {types_of(messages)!r}")


class _FakeStatement:
    def __init__(self) -> None:
        self.cancelled = threading.Event()

    def setQueryTimeout(self, seconds) -> None:  # noqa: N802 - JDBC name
        del seconds

    def cancel(self) -> None:
        self.cancelled.set()

    def close(self) -> None:
        pass


class LaneBackend:
    """A backend with one shared lane, taken exactly as CalciteBackend takes it.

    Every statement returns ``nbatches`` batches of ``batch_rows`` rows of ``width``
    bytes (one whose SQL names "big" returns far more than a socket buffer holds); the stream holds the lane lock until it is closed, and tells its CancelScope
    when it is inside the "engine" (producing a batch) and when it is not.
    """

    def __init__(self, nbatches: int = 4, batch_rows: int = 2, width: int = 4) -> None:
        self.lock = threading.RLock()
        self.nbatches = nbatches
        self.batch_rows = batch_rows
        self.width = width
        self.released = 0

    def ready(self) -> bool:
        return True

    @property
    def extensions(self) -> frozenset:
        return frozenset()

    def cancel_session(self, session_key, reason) -> bool:
        return IN_FLIGHT.cancel(session_key, reason)

    def discard_session(self, session_key) -> None:
        IN_FLIGHT.discard(session_key)

    def lock_is_free(self) -> bool:
        got = []

        def _try() -> None:
            if self.lock.acquire(blocking=False):
                self.lock.release()
                got.append(True)

        t = threading.Thread(target=_try)
        t.start()
        t.join()
        return bool(got)

    def execute_sql(
        self, sql, role_id, params=None, stream=False, session_key=None, timeout_ms=0,
        lane="user", client_gone=None,
    ) -> QueryResult:
        del role_id, params, stream, lane
        # A statement that names "big" gets a result no socket buffer can hold.
        nbatches, batch_rows, width = (
            (100000, 64, 4096) if "big" in sql else (self.nbatches, self.batch_rows, self.width)
        )
        scope = CancelScope(session_key, timeout_ms, client_gone)
        scope.acquire(self.lock)
        scope.arm(_FakeStatement())
        released = []

        def _release() -> None:
            if released:
                return
            released.append(True)
            scope.disarm()
            self.released += 1
            self.lock.release()

        def _gen() -> Iterator[list]:
            try:
                for b in range(nbatches):
                    scope.enter_engine()
                    scope.raise_if_canceled()
                    batch = [(b * batch_rows + i, "x" * width) for i in range(batch_rows)]
                    scope.leave_engine()
                    yield batch
            finally:
                _release()

        scope.leave_engine()
        return QueryResult(
            column_names=["id", "pad"],
            column_types=["INTEGER", "VARCHAR"],
            row_batches=_ClosingIterator(_gen(), _release),
        )


@pytest.fixture
def wire():
    """Start a pgwire-calcite server on a free port against a supplied backend."""
    servers = []

    def _start(backend, **kwargs):
        port = _free_port()
        srv = launcher.serve(host="127.0.0.1", port=port, auth="none", backend=backend, **kwargs)
        servers.append(srv)
        time.sleep(0.1)
        return srv, port

    yield _start
    for srv in servers:
        srv.shutdown()
        srv.terminate_sessions("test teardown")  # so server_close() has no thread to wait on
        srv.server_close()


def _wait_until(predicate, timeout_s: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


# --- connection bookkeeping -----------------------------------------------------


def test_a_client_that_vanishes_mid_result_is_not_left_in_the_server_table(wire):
    """A client killed while its result streams breaks the pipe on the server's error
    path; that connection's context must still leave the server's table."""
    backend = LaneBackend()
    srv, port = wire(backend)
    client = ExtClient("127.0.0.1", port)
    client.send(b"Q", b"SELECT big\x00")
    client._recv_msg()  # RowDescription: the result is streaming
    # Reset instead of an orderly close, as a killed process does.
    client.sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    client.sock.close()

    assert _wait_until(lambda: backend.released == 1), "the engine lane was never released"
    assert _wait_until(lambda: len(srv.ctxts) == 0), f"leaked contexts: {len(srv.ctxts)}"
    assert backend.lock_is_free()


def test_an_authenticated_password_session_may_sit_idle_past_the_startup_timeout(monkeypatch):
    """The startup timeout bounds the handshake only, on every authentication path."""
    monkeypatch.setattr(CalciteHandler, "_STARTUP_TIMEOUT_SECONDS", 0.3)
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="simple", users={"alice": "s3cret"})
    try:
        client = MiniPgClient("127.0.0.1", port, user="alice", password="s3cret")
        time.sleep(1.0)
        res = client.query("SELECT 1")
        assert res["error"] is None
        assert res["rows"] == [["1"]]
        client.close()
    finally:
        srv.shutdown()
        srv.server_close()


# --- errors reach the client as errors, and the connection survives -------------


def test_an_engine_error_carries_a_severity_and_a_sqlstate(wire):
    _, port = wire(StubBackend())
    client = ExtClient("127.0.0.1", port)
    client.send(b"Q", b"SELECT nope FROM nowhere\x00")
    messages = []
    while True:
        m = client._recv_msg()
        messages.append(m)
        if m[0] == "Z":
            break
    fields = first_error(messages)
    assert fields["S"] == "ERROR"
    assert fields["C"] == "XX000"
    assert "StubBackend" in fields["M"]
    client.close()


def test_a_failed_simple_query_does_not_swallow_the_next_extended_query(wire):
    """An error in a simple Query used to leave the connection's error flag set, and the
    next extended batch's Execute was skipped without a word: the client read no rows."""
    _, port = wire(StubBackend())
    client = ExtClient("127.0.0.1", port)
    assert client.query("SELECT nope FROM nowhere")["error"] is not None

    messages = client.extended("SELECT 1")

    assert types_of(messages) == "12DCZ", types_of(messages)
    client.close()


def test_closing_a_statement_that_does_not_exist_is_not_an_error(wire):
    _, port = wire(StubBackend())
    client = ExtClient("127.0.0.1", port)
    client.close_target(b"S", "never_prepared")
    client.close_target(b"P", "never_bound")
    messages = client.sync()
    assert types_of(messages) == "33Z", types_of(messages)
    assert client.query("SELECT 1")["rows"] == [["1"]]
    client.close()


def test_binding_an_unknown_statement_is_an_error_not_a_dropped_connection(wire):
    _, port = wire(StubBackend())
    client = ExtClient("127.0.0.1", port)
    client.bind(stmt="never_prepared")
    client.execute()
    messages = client.sync()
    assert types_of(messages) == "EZ", types_of(messages)
    assert first_error(messages)["C"] == "26000"
    assert client.query("SELECT 1")["rows"] == [["1"]]
    client.close()


def test_executing_an_unknown_portal_is_an_error_with_its_sqlstate(wire):
    _, port = wire(StubBackend())
    client = ExtClient("127.0.0.1", port)
    client.execute(portal="never_bound")
    messages = client.sync()
    assert types_of(messages) == "EZ", types_of(messages)
    assert first_error(messages)["C"] == "34000"
    client.close()


def test_a_malformed_bind_is_a_protocol_violation_not_a_dropped_connection(wire):
    _, port = wire(StubBackend())
    client = ExtClient("127.0.0.1", port)
    client.parse("SELECT 1")
    client.send(b"B", b"\x00\x00\x00")  # portal, statement, then a truncated format count
    messages = client.sync()
    assert types_of(messages) == "1EZ", types_of(messages)
    assert first_error(messages)["C"] == "08P01"
    assert client.query("SELECT 1")["rows"] == [["1"]]
    client.close()


def test_after_an_error_messages_are_discarded_until_sync(wire):
    """PostgreSQL's rule: nothing after the failed message is acted on before Sync."""
    _, port = wire(StubBackend())
    client = ExtClient("127.0.0.1", port)
    client.bind(stmt="never_prepared")  # fails
    client.parse("SELECT 1", name="s1")  # discarded
    client.bind(stmt="s1")  # discarded
    client.execute()  # discarded
    messages = client.sync()
    assert types_of(messages) == "EZ", types_of(messages)
    # The discarded Parse did not create the statement.
    client.bind(stmt="s1")
    assert first_error(client.sync())["C"] == "26000"
    client.close()


class _FailsMidStream(LaneBackend):
    """Yields one batch, then is cancelled the way statement_timeout cancels."""

    def execute_sql(self, *args, **kwargs) -> QueryResult:
        result = super().execute_sql(*args, **kwargs)
        inner = result.row_batches

        def _gen() -> Iterator[list]:
            try:
                yield next(inner)
                raise QueryCanceled(CANCELED_BY_USER)
            finally:
                inner.close()

        return QueryResult(
            column_names=result.column_names,
            column_types=result.column_types,
            row_batches=_ClosingIterator(_gen(), inner.close),
        )


def test_a_cancel_that_lands_mid_result_keeps_an_extended_protocol_session(wire):
    backend = _FailsMidStream()
    _, port = wire(backend)
    client = ExtClient("127.0.0.1", port)

    messages = client.extended("SELECT anything")

    assert types_of(messages).endswith("EZ"), types_of(messages)
    assert first_error(messages)["C"] == "57014"
    assert backend.lock_is_free()
    # The session survives and runs the next statement.
    assert types_of(client.extended("SELECT anything")).endswith("EZ")
    client.close()


def test_closing_a_suspended_portal_releases_the_engine_at_once(wire):
    """A cursor closed after a partial fetch must not keep the engine until the
    session's next statement."""
    backend = LaneBackend(nbatches=4, batch_rows=2)
    _, port = wire(backend)
    client = ExtClient("127.0.0.1", port)
    messages = client.extended("SELECT rows", limit=1)
    assert "s" in types_of(messages), types_of(messages)  # PortalSuspended
    assert not backend.lock_is_free()

    client.close_target(b"P", "")
    assert types_of(client.sync()) == "3Z"

    assert backend.lock_is_free()
    assert backend.released == 1
    client.close()


# --- several clients, one engine ------------------------------------------------


def test_a_cancel_request_reaches_a_statement_still_queued_for_the_engine():
    """Unit: the wait for the lane is cancellable, not only the statement once it runs."""
    lock = threading.Lock()
    lock.acquire()
    outcome: dict = {}

    def _queued() -> None:
        try:
            CancelScope("queued-session", 0).acquire(lock)
            outcome["acquired"] = True
        except QueryCanceled as exc:
            outcome["exc"] = exc

    worker = threading.Thread(target=_queued)
    worker.start()
    try:
        assert _wait_until(lambda: "queued-session" in IN_FLIGHT.queued_sessions())
        assert IN_FLIGHT.cancel("queued-session", CANCELED_BY_USER) is True
        worker.join(5)
        assert not worker.is_alive()
        assert "acquired" not in outcome
        assert CANCELED_BY_USER in str(outcome["exc"])
        assert IN_FLIGHT.queued_sessions() == set()
    finally:
        lock.release()
        worker.join(5)


def test_cancel_request_on_the_wire_aborts_a_queued_statement(wire, monkeypatch):
    """Client A holds the engine; client B's statement queues behind it; B's
    CancelRequest must end B's wait with 57014 while A is untouched."""
    monkeypatch.setattr(CancelScope, "idle_holder_grace_ms", 0)
    backend = LaneBackend(nbatches=4, batch_rows=2)
    srv, port = wire(backend)
    a = ExtClient("127.0.0.1", port)
    assert "s" in types_of(a.extended("SELECT rows", limit=1))  # A keeps the engine

    b = ExtClient("127.0.0.1", port)
    result: dict = {}
    worker = threading.Thread(target=lambda: result.update(b.query("SELECT rows")))
    worker.start()
    assert _wait_until(lambda: len(IN_FLIGHT.queued_sessions()) == 1)

    # B's BackendKeyData, as the server recorded it.
    (b_key,) = IN_FLIGHT.queued_sessions()
    ctx = next(c for c in srv.ctxts.values() if str(c.session.id) == b_key)
    cancel = socket.create_connection(("127.0.0.1", port))
    cancel.sendall(struct.pack("!IIII", 16, 80877102, ctx.process_id, ctx.secret_key))
    cancel.close()

    worker.join(5)
    assert not worker.is_alive(), "the queued statement ignored the cancel request"
    fields = error_fields(result["error"].encode())
    assert fields["C"] == "57014"
    assert CANCELED_BY_USER in fields["M"]
    # A still owns its portal and can finish it.
    a.execute(limit=0)
    assert types_of(a.sync()).endswith("CZ")
    a.close()
    b.close()


def test_an_idle_cursor_does_not_starve_the_other_clients(wire, monkeypatch):
    """Client A fetches one row of a cursor and goes quiet, holding the engine. Client
    B must get its answer once the grace passes -- not 'server is busy' -- and A, which
    was holding the engine for nothing, is disconnected."""
    monkeypatch.setattr(CancelScope, "idle_holder_grace_ms", 300)
    monkeypatch.setattr(CancelScope, "max_queue_wait_ms", 5000)
    backend = LaneBackend(nbatches=4, batch_rows=2)
    _, port = wire(backend)
    a = ExtClient("127.0.0.1", port)
    assert "s" in types_of(a.extended("SELECT rows", limit=1))

    b = MiniPgClient("127.0.0.1", port)
    started = time.monotonic()
    res = b.query("SELECT rows")

    assert res["error"] is None, res["error"]
    assert len(res["rows"]) == 8
    assert time.monotonic() - started < 4
    assert a.is_closed_by_server()
    b.close()


def test_a_client_that_stops_reading_does_not_starve_the_other_clients(wire, monkeypatch):
    """Client A asks for a large result and never reads it: the server blocks writing
    to A while A's statement holds the engine. B must still be served."""
    monkeypatch.setattr(CancelScope, "idle_holder_grace_ms", 300)
    monkeypatch.setattr(CancelScope, "max_queue_wait_ms", 8000)
    backend = LaneBackend(nbatches=2, batch_rows=2)
    _, port = wire(backend)
    a = ExtClient("127.0.0.1", port)
    a.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
    a.send(b"Q", b"SELECT big\x00")  # and never read
    assert _wait_until(lambda: not backend.lock_is_free())

    b = MiniPgClient("127.0.0.1", port)
    started = time.monotonic()
    res = b.query("SELECT small")

    assert res["error"] is None, res["error"]
    assert len(res["rows"]) == 4
    assert time.monotonic() - started < 6
    b.close()
    a.sock.close()


def test_a_reader_that_keeps_reading_is_never_evicted(wire, monkeypatch):
    """The eviction is for a holder that is not using the engine; a client steadily
    consuming a long result is using it, however long another statement has waited."""
    monkeypatch.setattr(CancelScope, "idle_holder_grace_ms", 600)
    monkeypatch.setattr(CancelScope, "max_queue_wait_ms", 8000)

    class _Slow(LaneBackend):
        def execute_sql(self, *args, **kwargs):
            result = super().execute_sql(*args, **kwargs)
            inner = result.row_batches

            def _gen():
                try:
                    for batch in inner:
                        time.sleep(0.1)  # consumer-side pacing, well under the grace
                        yield batch
                finally:
                    inner.close()

            return QueryResult(
                column_names=result.column_names,
                column_types=result.column_types,
                row_batches=_ClosingIterator(_gen(), inner.close),
            )

    backend = _Slow(nbatches=12, batch_rows=2)
    _, port = wire(backend)
    a = MiniPgClient("127.0.0.1", port)
    a_result: dict = {}
    worker = threading.Thread(target=lambda: a_result.update(a.query("SELECT rows")))
    worker.start()
    assert _wait_until(lambda: not backend.lock_is_free())

    b = MiniPgClient("127.0.0.1", port)
    res = b.query("SELECT rows")
    worker.join(10)

    assert a_result["error"] is None, a_result["error"]
    assert len(a_result["rows"]) == 24
    assert res["error"] is None
    a.close()
    b.close()


@pytest.fixture
def wedge_exits(monkeypatch):
    """Short cancel grace; records exit_wedged calls instead of exiting pytest."""
    monkeypatch.setattr(
        "pgwire_calcite.calcite_backend._attach_current_thread_to_jvm", lambda: None
    )
    monkeypatch.setattr(InFlightStatement, "cancel_grace_ms", 300)
    calls: list = []
    monkeypatch.setattr(
        "pgwire_calcite.calcite_backend.exit_wedged", lambda reason, grace: calls.append(reason)
    )
    return calls


def test_statement_timeout_on_an_idle_cursor_does_not_exit_the_server(wire, wedge_exits, monkeypatch):
    """statement_timeout fires on a cursor whose client fetched one row and went quiet.
    The engine returned long ago; the statement is open only because the client holds
    it. That used to be declared a wedge and the whole server exited (status 3), taking
    every other client's session with it. The idle client is dropped instead."""
    monkeypatch.setattr(CancelScope, "idle_holder_grace_ms", 0)  # isolate the timeout path
    backend = LaneBackend(nbatches=4, batch_rows=2)
    _, port = wire(backend, statement_timeout_ms=200)
    a = ExtClient("127.0.0.1", port)
    assert "s" in types_of(a.extended("SELECT rows", limit=1))

    assert a.is_closed_by_server(timeout_s=5)
    assert _wait_until(backend.lock_is_free)
    time.sleep(0.8)  # past a second grace period: still no wedge declared
    assert wedge_exits == []

    b = MiniPgClient("127.0.0.1", port)
    assert b.query("SELECT rows")["error"] is None
    b.close()


def test_a_statement_stuck_inside_the_engine_is_still_declared_a_wedge(wedge_exits):
    """The wedge exit is unchanged for what it was built for: a cancelled statement
    whose thread never comes back from the engine."""
    evicted: list = []
    handle = InFlightStatement(_FakeStatement(), evict=evicted.append)

    handle.cancel(CANCELED_BY_USER)

    assert _wait_until(lambda: wedge_exits == [CANCELED_BY_USER], timeout_s=5)
    assert evicted == []  # dropping the client cannot free a thread stuck in the engine


def test_many_clients_share_the_lane_without_losing_or_mixing_results(wire):
    backend = LaneBackend(nbatches=3, batch_rows=5)
    srv, port = wire(backend)
    errors: list = []

    def _client(n: int) -> None:
        try:
            c = ExtClient("127.0.0.1", port, user=f"u{n}")
            for i in range(10):
                if i % 2:
                    res = c.query("SELECT rows")
                    assert res["error"] is None and len(res["rows"]) == 15
                else:
                    assert types_of(c.extended("SELECT rows")).count("D") == 15
            c.close()
        except Exception as exc:  # noqa: BLE001 - collected and asserted below
            errors.append(repr(exc))

    threads = [threading.Thread(target=_client, args=(n,)) for n in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(60)

    assert errors == []
    assert backend.released == 12 * 10
    assert backend.lock_is_free()
    assert IN_FLIGHT.queued_sessions() == set()
    assert _wait_until(lambda: len(srv.ctxts) == 0)
