# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The hardening of test_hardening_wire.py, against the real Calcite engine.

Several clients at once (AskAmerica runs one MCP process per conversation, each with its
own connection to one shared server) and one client at a time (Provisa), over the embedded
JDBC connection, its lane lock and the Arrow stream -- no test double in the path.
"""

from __future__ import annotations

import asyncio
import socket
import struct
import threading
import time

import psycopg
import pytest

from pgwire_calcite import launcher
from pgwire_calcite.backend import LANE_USER
from pgwire_calcite.calcite_backend import IN_FLIGHT, CancelScope, InFlightStatement

from test_hardening_wire import ExtClient, _wait_until, first_error, types_of
from test_phase0_wire import MiniPgClient, _free_port

#: 10^4 rows: ten Arrow batches, so a result can be left half read.
ROWS_SQL = "SELECT a.EMPNO FROM EMPS a, EMPS b, EMPS c, EMPS d"
ROWS = 10000
#: 10^6 rows of text: far more than the socket buffers between server and client hold.
BIG_SQL = "SELECT a.ENAME, b.ENAME, c.ENAME FROM EMPS a, EMPS b, EMPS c, EMPS d, EMPS e, EMPS f"


@pytest.fixture
def server(calcite_backend):
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", backend=calcite_backend)
    time.sleep(0.2)
    yield srv, port
    srv.shutdown()
    srv.terminate_sessions("test teardown")
    srv.server_close()


def _lane_is_free(backend) -> bool:
    _, lock = backend.lane(LANE_USER)
    got = []

    def _try() -> None:
        if lock.acquire(blocking=False):
            lock.release()
            got.append(True)

    t = threading.Thread(target=_try)
    t.start()
    t.join()
    return bool(got)


def _connect(port: int) -> psycopg.Connection:
    return psycopg.connect(
        f"host=127.0.0.1 port={port} user=tester dbname=postgres", autocommit=True
    )


# --- several clients at once ---------------------------------------------------


def test_many_psycopg_connections_get_their_own_answers(server, calcite_backend):
    srv, port = server
    errors: list = []
    names = {7369: "SMITH", 7499: "ALLEN"}

    def _client(n: int) -> None:
        try:
            with _connect(port) as conn:
                for i in range(6):
                    empno = 7369 if (n + i) % 2 else 7499
                    row = conn.execute(
                        "SELECT ENAME FROM EMPS WHERE EMPNO = %s", (empno,)
                    ).fetchone()
                    assert row == (names[empno],), (n, i, row)
                    assert conn.execute("SELECT count(*) FROM EMPS").fetchone() == (10,)
        except Exception as exc:  # noqa: BLE001 - collected and asserted below
            errors.append(repr(exc))

    threads = [threading.Thread(target=_client, args=(n,)) for n in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(120)

    assert errors == []
    assert all(not t.is_alive() for t in threads)
    assert _lane_is_free(calcite_backend)
    assert IN_FLIGHT.active_sessions() == set()
    assert IN_FLIGHT.queued_sessions() == set()
    assert _wait_until(lambda: len(srv.ctxts) == 0)


def test_many_asyncpg_connections_bind_parameters_concurrently(server):
    """asyncpg binds in binary, so each session resolves parameter types through the
    engine; six sessions do it at once, each re-parsing (statement cache off)."""
    asyncpg = pytest.importorskip("asyncpg")
    _, port = server

    async def _one(n: int):
        conn = await asyncpg.connect(
            host="127.0.0.1", port=port, user=f"u{n}", database="postgres",
            statement_cache_size=0,
        )
        try:
            out = []
            for _ in range(4):
                by_name = await conn.fetch("SELECT EMPNO FROM EMPS WHERE ENAME = $1", "SMITH")
                by_dept = await conn.fetch(
                    "SELECT count(*) FROM EMPS WHERE DEPTNO = $1 AND ENAME <> $2", 20, "x"
                )
                out.append((by_name[0][0], by_dept[0][0]))
            return out
        finally:
            await conn.close()

    async def _run():
        return await asyncio.gather(*[_one(n) for n in range(6)])

    results = asyncio.run(_run())
    assert len(results) == 6
    first = results[0][0]
    assert first[0] == 7369 and first[1] > 0
    assert all(pair == first for client in results for pair in client)


def test_a_client_killed_mid_result_frees_the_engine_for_the_next_client(
    server, calcite_backend
):
    srv, port = server
    victim = ExtClient("127.0.0.1", port)
    victim.send(b"Q", BIG_SQL.encode() + b"\x00")
    victim._recv_msg()  # RowDescription: rows are streaming
    victim.sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    victim.sock.close()  # reset, as a killed client process does

    with _connect(port) as conn:
        started = time.monotonic()
        assert conn.execute("SELECT count(*) FROM EMPS").fetchone() == (10,)
        assert time.monotonic() - started < 20

    assert _wait_until(lambda: _lane_is_free(calcite_backend))
    assert _wait_until(lambda: len(srv.ctxts) == 0), f"leaked contexts: {len(srv.ctxts)}"
    assert IN_FLIGHT.active_sessions() == set()


def test_an_idle_cursor_is_dropped_for_a_waiting_client(server, calcite_backend, monkeypatch):
    monkeypatch.setattr(CancelScope, "idle_holder_grace_ms", 500)
    monkeypatch.setattr(CancelScope, "max_queue_wait_ms", 20000)
    _, port = server
    holder = ExtClient("127.0.0.1", port)
    messages = holder.extended(ROWS_SQL, limit=1)
    assert "s" in types_of(messages), types_of(messages)  # PortalSuspended
    assert not _lane_is_free(calcite_backend)

    with _connect(port) as conn:
        started = time.monotonic()
        assert conn.execute("SELECT count(*) FROM EMPS").fetchone() == (10,)
        assert time.monotonic() - started < 15

    assert holder.is_closed_by_server()


def test_a_client_that_never_reads_is_dropped_for_a_waiting_client(
    server, calcite_backend, monkeypatch
):
    monkeypatch.setattr(CancelScope, "idle_holder_grace_ms", 500)
    monkeypatch.setattr(CancelScope, "max_queue_wait_ms", 30000)
    _, port = server
    stalled = ExtClient("127.0.0.1", port)
    stalled.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
    stalled.send(b"Q", BIG_SQL.encode() + b"\x00")  # and never read the answer
    assert _wait_until(lambda: not _lane_is_free(calcite_backend), timeout_s=20)

    with _connect(port) as conn:
        started = time.monotonic()
        assert conn.execute("SELECT count(*) FROM EMPS").fetchone() == (10,)
        assert time.monotonic() - started < 25

    stalled.sock.close()
    assert _wait_until(lambda: _lane_is_free(calcite_backend))


def test_statement_timeout_on_an_idle_cursor_keeps_the_server(
    server, calcite_backend, monkeypatch
):
    """The real Arrow stream reports when it is outside the engine, so a timeout that
    fires on a cursor nobody is fetching from drops that client and not the server."""
    monkeypatch.setattr(CancelScope, "idle_holder_grace_ms", 0)
    monkeypatch.setattr(InFlightStatement, "cancel_grace_ms", 400)
    wedges: list = []
    monkeypatch.setattr(
        "pgwire_calcite.calcite_backend.exit_wedged", lambda reason, grace: wedges.append(reason)
    )
    _, port = server
    holder = ExtClient("127.0.0.1", port)
    assert holder.query("SET statement_timeout = 300")["error"] is None
    assert "s" in types_of(holder.extended(ROWS_SQL, limit=1))

    assert holder.is_closed_by_server(timeout_s=10)
    assert _wait_until(lambda: _lane_is_free(calcite_backend))
    time.sleep(1.0)
    assert wedges == []
    with _connect(port) as conn:
        assert conn.execute("SELECT count(*) FROM EMPS").fetchone() == (10,)


# --- one client at a time ------------------------------------------------------


def test_one_connection_survives_every_kind_of_failure_in_sequence(server, calcite_backend):
    """A single long-lived connection: errors, a cursor left half read and closed, an
    unknown statement -- and after each, the same connection keeps answering."""
    _, port = server
    client = ExtClient("127.0.0.1", port)

    assert client.query("SELECT count(*) FROM EMPS")["rows"] == [["10"]]
    bad = client.query("SELECT no_such_column FROM EMPS")
    assert bad["error"] is not None
    assert types_of(client.extended("SELECT count(*) FROM EMPS")) == "12DCZ"

    failed = client.extended("SELECT no_such_column FROM EMPS")
    assert first_error(failed)["C"] == "XX000"
    assert types_of(client.extended("SELECT count(*) FROM EMPS")) == "12DCZ"

    assert "s" in types_of(client.extended(ROWS_SQL, limit=5))
    client.close_target(b"P", "")
    assert types_of(client.sync()) == "3Z"
    assert _lane_is_free(calcite_backend)

    client.bind(stmt="never_prepared")
    assert first_error(client.sync())["C"] == "26000"

    full = client.query(ROWS_SQL)
    assert full["error"] is None and len(full["rows"]) == ROWS
    assert client.query("SELECT count(*) FROM EMPS")["rows"] == [["10"]]
    client.close()
    assert _wait_until(lambda: _lane_is_free(calcite_backend))


def test_a_minimal_client_reads_a_full_result_while_others_queue(server, calcite_backend):
    """A long result being read steadily is never mistaken for an idle holder."""
    _, port = server
    reader = MiniPgClient("127.0.0.1", port)
    result: dict = {}
    worker = threading.Thread(target=lambda: result.update(reader.query(ROWS_SQL)))
    worker.start()
    with _connect(port) as conn:
        assert conn.execute("SELECT count(*) FROM EMPS").fetchone() == (10,)
    worker.join(60)
    assert result["error"] is None
    assert len(result["rows"]) == ROWS
    reader.close()
