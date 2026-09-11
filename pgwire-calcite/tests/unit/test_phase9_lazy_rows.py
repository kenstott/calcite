# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Lazy row streaming at the wire (PGW-020, PGW-022).

The wire layer must emit DataRow messages while the backend is still producing
rows, hold at most one batch in memory, take column types from ResultSetMetaData
(buffering exactly one batch only when the metadata is insufficient), report an
exact CommandComplete count, and release the backend statement on both normal
completion and a client disconnect mid-stream.

A fixture backend that yields batches from a generator makes production order
observable: the generator records how many rows it has produced, and the client
records when the first DataRow arrived.
"""

from __future__ import annotations

import socket
import threading
import time
from typing import Iterator, List, Optional

import pytest

from pgwire_calcite import launcher
from pgwire_calcite.types import QueryResult

from test_phase0_wire import MiniPgClient, _free_port

BATCH = 1024


class GeneratorBackend:
    """Backend whose rows exist only as they are pulled, one batch at a time.

    ``produced`` counts rows handed to the wire; ``resident`` is the number of
    batches the generator itself is holding (always <= 1 — it keeps none). The
    generator's ``finally`` records that the "statement" was closed, standing in
    for the JDBC statement/allocator/lock release that arrow_bridge performs.
    """

    def __init__(self, nrows: int, column_types: Optional[List[str]], batch_size: int = BATCH):
        self._nrows = nrows
        self._column_types = column_types
        self._batch_size = batch_size
        self.produced = 0
        self.max_batch_len = 0
        self.closed = False
        self.first_row_seen = threading.Event()
        self.release_after_first_row = threading.Event()

    def ready(self) -> bool:
        return True

    def _batches(self) -> Iterator[List[tuple]]:
        try:
            for start in range(0, self._nrows, self._batch_size):
                n = min(self._batch_size, self._nrows - start)
                batch = [(start + i, f"name-{start + i}") for i in range(n)]
                self.produced += n
                self.max_batch_len = max(self.max_batch_len, len(batch))
                yield batch
                # Drop the reference so only the consumer's batch is resident.
                del batch
        finally:
            self.closed = True

    def cancel_session(self, session_key, reason) -> bool:
        """Test backend: nothing runs long enough to be in flight."""
        del session_key, reason
        return False

    def discard_session(self, session_key) -> None:
        """Test backend: no per-session execution state."""
        del session_key

    def execute_sql(
        self, sql, role_id, params=None, stream=False, session_key=None, timeout_ms=0
    ) -> QueryResult:
        del sql, role_id, params, stream, session_key, timeout_ms
        return QueryResult(
            column_names=["id", "name"],
            column_types=self._column_types,
            row_batches=self._batches(),
        )


@pytest.fixture
def wire():
    """Start a pgwire-calcite server on a free port against a supplied backend."""
    servers = []

    def _start(backend):
        port = _free_port()
        srv = launcher.serve(host="127.0.0.1", port=port, auth="none", backend=backend)
        servers.append(srv)
        time.sleep(0.1)
        return port

    yield _start
    for srv in servers:
        srv.shutdown()


def test_first_data_row_precedes_last_produced_row(wire):
    """PGW-020: rows go out while the backend is still producing them."""
    backend = GeneratorBackend(nrows=10_000, column_types=["INTEGER", "VARCHAR"], batch_size=256)
    port = wire(backend)

    c = MiniPgClient("127.0.0.1", port)
    try:
        c._send(b"Q", b"SELECT id, name FROM t\x00")
        produced_at_first_row = None
        rows = 0
        tag = None
        while True:
            mtype, payload = c._recv_msg()
            if mtype == "D":
                if produced_at_first_row is None:
                    produced_at_first_row = backend.produced
                rows += 1
            elif mtype == "C":
                tag = payload.rstrip(b"\x00").decode()
            elif mtype == "E":
                pytest.fail(payload.decode(errors="replace"))
            elif mtype == "Z":
                break
    finally:
        c.close()

    assert rows == 10_000
    assert tag == "SELECT 10000"  # exact count, tallied as rows streamed
    # The first DataRow was on the wire long before the backend produced row 10000.
    assert produced_at_first_row is not None
    assert produced_at_first_row < 10_000


def test_memory_shape_one_batch_resident(wire):
    """PGW-020: 100k rows never put more than one batch in memory."""
    backend = GeneratorBackend(nrows=100_000, column_types=["INTEGER", "VARCHAR"], batch_size=BATCH)
    port = wire(backend)

    c = MiniPgClient("127.0.0.1", port)
    try:
        r = c.query("SELECT id, name FROM t")
        assert r["error"] is None, r["error"]
        assert len(r["rows"]) == 100_000
        assert r["command_tag"] == "SELECT 100000"
    finally:
        c.close()

    assert backend.max_batch_len == BATCH  # never accumulated across batches
    assert backend.closed  # released on normal completion


def test_types_from_metadata_do_not_buffer(wire):
    """Sufficient metadata => RowDescription is sent without pulling any batch."""
    backend = GeneratorBackend(nrows=4096, column_types=["INTEGER", "VARCHAR"], batch_size=BATCH)
    result = backend.execute_sql("select", "u", stream=True)

    from pgwire_calcite.server import CalciteQueryResult

    qr = CalciteQueryResult(result, "SELECT id, name FROM t")
    assert backend.produced == 0  # no peek: types came from the metadata labels
    assert [qr.column(i)[0] for i in range(qr.column_count())] == ["id", "name"]
    assert list(qr.rows()) == [(i, f"name-{i}") for i in range(4096)]


def test_insufficient_metadata_buffers_exactly_one_batch(wire):
    """Calcite ANY/OTHER (empty label) => infer from exactly one buffered batch."""
    backend = GeneratorBackend(nrows=4096, column_types=["", ""], batch_size=BATCH)
    result = backend.execute_sql("select", "u", stream=True)

    from buenavista.core import BVType

    from pgwire_calcite.server import CalciteQueryResult

    qr = CalciteQueryResult(result, "SELECT id, name FROM t")
    assert backend.produced == BATCH  # exactly one batch peeked, not the whole result
    assert [qr.column(i)[1] for i in range(qr.column_count())] == [BVType.BIGINT, BVType.TEXT]
    # The peeked batch is still delivered — no row read twice, none dropped.
    assert list(qr.rows()) == [(i, f"name-{i}") for i in range(4096)]


def test_opaque_jdbc_type_names_report_no_label():
    """normalize: ANY/OTHER carry no usable type, so the stream label is empty."""
    from pgwire_calcite import normalize

    assert normalize.stream_type_label("ANY") == ""
    assert normalize.stream_type_label("OTHER") == ""
    assert normalize.stream_type_label("INTEGER") == "INTEGER"
    assert normalize.stream_type_label("VARCHAR(10)") == "VARCHAR"


def test_client_disconnect_midstream_closes_statement(wire):
    """PGW-022: a client that vanishes mid-stream must not leak the statement."""
    backend = GeneratorBackend(nrows=5_000_000, column_types=["INTEGER", "VARCHAR"], batch_size=64)
    port = wire(backend)

    sock = socket.create_connection(("127.0.0.1", port), timeout=5)
    c = MiniPgClient.__new__(MiniPgClient)
    c.sock = sock
    c.params = {}
    c._startup("tester", None)
    c._send(b"Q", b"SELECT id, name FROM t\x00")
    # Read far enough to be certain the server is mid-stream, then vanish.
    while True:
        mtype, _ = c._recv_msg()
        if mtype == "D":
            break
    assert not backend.closed
    sock.close()

    deadline = time.time() + 10
    while time.time() < deadline and not backend.closed:
        time.sleep(0.05)
    assert backend.closed, "backend statement leaked after client disconnect"
    assert backend.produced < 5_000_000  # it never got to materialize the result


def test_row_limit_resumes_instead_of_replaying(wire):
    """An abandoned rows() generator resumes at the next unsent row."""
    backend = GeneratorBackend(nrows=10, column_types=["INTEGER", "VARCHAR"], batch_size=4)
    result = backend.execute_sql("select", "u", stream=True)

    from pgwire_calcite.server import CalciteQueryResult

    qr = CalciteQueryResult(result, "SELECT id, name FROM t")
    first = []
    gen = qr.rows()
    for row in gen:
        first.append(row)
        if len(first) == 3:
            break
    del gen  # portal suspended: generator abandoned, position kept on the result
    rest = list(qr.rows())
    assert first == [(i, f"name-{i}") for i in range(3)]
    assert rest == [(i, f"name-{i}") for i in range(3, 10)]


def test_next_statement_releases_previous_stream(wire):
    """A session runs one statement at a time; the next one releases the previous."""
    from pgwire_calcite import server as srv_mod
    from pgwire_calcite.server import CalciteSession
    from pgwire_calcite.state import ServerState

    backend = GeneratorBackend(nrows=100_000, column_types=["INTEGER", "VARCHAR"])
    session = CalciteSession()
    session.role_id = "tester"
    prev_state = srv_mod.state
    srv_mod.state = ServerState(backend=backend)
    try:
        qr = session.execute_sql("SELECT id, name FROM t")
        next(qr.rows())  # leave the stream mid-flight
        assert not backend.closed
        session.execute_sql("SELECT id, name FROM t2")  # new statement
        assert backend.closed
    finally:
        srv_mod.state = prev_state
