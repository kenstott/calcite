# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Session items as Provisa's pgwire answers them (REQ-2255 P5).

- Advisory locks are held per connection: exclusive or shared, session or transaction level,
  released by unlock, unlock_all, the transaction's end or the connection's end; a wait ends
  with the lock or the statement_timeout.
- SET TimeZone is validated, answered by SHOW and reported in ParameterStatus.
- A statement refused by name (a construct with no Calcite form, COPY FROM) is an error with
  its SQLSTATE, logged as a refusal without a traceback, and the connection carries on.
"""

# Requirements: REQ-2255

from __future__ import annotations

import logging
import time

import pytest

from pgwire_calcite import launcher

from test_phase0_wire import MiniPgClient, _free_port
from test_phase9_wire_fixes import EchoParamsBackend


@pytest.fixture()
def connect():
    """A factory of clients on one echo server; every client is closed at teardown, so no
    handler thread is left reading a socket nobody will write to."""
    import pgwire_calcite.server as server_mod

    p = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=p, auth="none", backend=EchoParamsBackend())
    # SHOW is answered by the catalog intercept, which an echo backend leaves off.
    server_mod.state.catalog_enabled = True
    time.sleep(0.1)
    clients: list = []

    def _connect() -> MiniPgClient:
        c = MiniPgClient("127.0.0.1", p)
        clients.append(c)
        return c

    yield _connect
    for c in clients:
        try:
            c.sock.close()
        except OSError:
            pass
    srv.shutdown()


def _scalar(c: MiniPgClient, sql: str):
    r = c.query(sql)
    assert r["error"] is None, r
    return r["rows"][0][0]


def _error(c: MiniPgClient, sql: str) -> str:
    r = c.query(sql)
    assert r["error"] is not None, r
    return r["error"]


# --------------------------------------------------------------------------- advisory locks


def test_an_exclusive_lock_excludes_another_connection_until_unlocked(connect):
    a, b = connect(), connect()
    assert _scalar(a, "SELECT pg_advisory_lock(42)") == ""
    assert _scalar(a, "SELECT pg_try_advisory_lock(42)") == "t"  # re-entrant for its holder
    assert _scalar(b, "SELECT pg_try_advisory_lock(42)") == "f"
    assert _scalar(a, "SELECT pg_advisory_unlock(42)") == "t"
    assert _scalar(b, "SELECT pg_try_advisory_lock(42)") == "f"  # a still holds it once
    assert _scalar(a, "SELECT pg_advisory_unlock(42)") == "t"
    assert _scalar(a, "SELECT pg_advisory_unlock(42)") == "f"  # holds none now
    assert _scalar(b, "SELECT pg_try_advisory_lock(42) AS got") == "t"


def test_a_two_key_lock_and_unlock_all(connect):
    a, b = connect(), connect()
    assert _scalar(a, "SELECT pg_try_advisory_lock(1, 2)") == "t"
    assert _scalar(b, "SELECT pg_try_advisory_lock(1, 2)") == "f"
    assert _scalar(b, "SELECT pg_try_advisory_lock(1)") == "t"  # a different key
    assert _scalar(a, "SELECT pg_advisory_unlock_all()") == ""
    assert _scalar(b, "SELECT pg_try_advisory_lock(1, 2)") == "t"


def test_shared_locks_share_and_exclude_an_exclusive_one(connect):
    a, b = connect(), connect()
    assert _scalar(a, "SELECT pg_advisory_lock_shared(7)") == ""
    assert _scalar(b, "SELECT pg_try_advisory_lock_shared(7)") == "t"
    assert _scalar(b, "SELECT pg_try_advisory_lock(7)") == "f"


def test_a_transaction_lock_ends_with_its_transaction(connect):
    a, b = connect(), connect()
    assert _scalar(a, "SELECT pg_try_advisory_xact_lock(9)") == "t"  # no transaction: gone
    assert _scalar(b, "SELECT pg_try_advisory_lock(9)") == "t"
    assert _scalar(b, "SELECT pg_advisory_unlock(9)") == "t"
    a.query("BEGIN")
    assert _scalar(a, "SELECT pg_advisory_xact_lock(10)") == ""
    assert _scalar(b, "SELECT pg_try_advisory_lock(10)") == "f"
    a.query("COMMIT")
    assert _scalar(b, "SELECT pg_try_advisory_lock(10)") == "t"


def test_a_connections_end_releases_its_locks(connect):
    a, b = connect(), connect()
    assert _scalar(a, "SELECT pg_advisory_lock(5)") == ""
    a.sock.close()
    deadline = time.monotonic() + 5
    while _scalar(b, "SELECT pg_try_advisory_lock(5)") != "t":
        assert time.monotonic() < deadline, "the closed connection's lock was never released"
        time.sleep(0.05)


def test_a_wait_ends_with_the_statement_timeout(connect):
    a, b = connect(), connect()
    assert _scalar(a, "SELECT pg_advisory_lock(77)") == ""
    b.query("SET statement_timeout = 200")
    started = time.monotonic()
    assert "57014" in _error(b, "SELECT pg_advisory_lock(77)")
    assert time.monotonic() - started < 5


# --------------------------------------------------------------------------- TimeZone


def _messages(c: MiniPgClient, sql: str) -> list:
    c._send(b"Q", sql.encode() + b"\x00")
    out = []
    while True:
        mtype, body = c._recv_msg()
        out.append((mtype, body))
        if mtype == "Z":
            return out


def test_set_timezone_is_reported_and_answered(connect):
    c = connect()
    msgs = _messages(c, "SET TimeZone = 'America/New_York'")
    status = [b for t, b in msgs if t == "S"]
    assert status == [b"TimeZone\x00America/New_York\x00"], msgs
    assert [t for t, _ in msgs].index("S") < [t for t, _ in msgs].index("C")
    assert _scalar(c, "SHOW TimeZone") == "America/New_York"
    msgs = _messages(c, "SET TIME ZONE 'UTC'")
    assert [b for t, b in msgs if t == "S"] == [b"TimeZone\x00UTC\x00"], msgs


def test_an_unknown_timezone_is_refused(connect):
    c = connect()
    assert "22023" in _error(c, "SET TimeZone = 'Not/AZone'")
    assert _scalar(c, "SHOW TimeZone") == "UTC"


# --------------------------------------------------------------------------- refusals


def test_a_construct_with_no_calcite_form_is_refused_without_a_traceback(calcite_backend, caplog):
    p = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=p, auth="none", backend=calcite_backend)
    time.sleep(0.1)
    try:
        c = MiniPgClient("127.0.0.1", p)
        with caplog.at_level(logging.INFO):
            error = _error(c, 'SELECT DISTINCT ON ("deptno") "deptno" FROM "SALES"."emps"')
        assert "0A000" in error and "DISTINCT ON" in error
        assert not [r for r in caplog.records if r.exc_info], [r.getMessage() for r in caplog.records]
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert c.query("SELECT 1")["error"] is None
        c.sock.close()
    finally:
        srv.shutdown()


def test_copy_from_stdin_is_refused_and_the_connection_carries_on(connect):
    c = connect()
    msgs = _messages(c, "COPY t FROM STDIN")
    assert "G" not in [t for t, _ in msgs]  # never a CopyInResponse
    errors = [b for t, b in msgs if t == "E"]
    assert len(errors) == 1 and b"C0A000" in errors[0], msgs
    assert c.query("SELECT 1")["error"] is None
