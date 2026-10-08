# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Arrow result path: Calcite JDBC -> Arrow IPC -> pyarrow -> streamed rows (PGW-019/020/022).

The row->Arrow transpose is done ONCE in the JVM via arrow-jdbc's JdbcToArrow
(bundled in vendor/jars). Results cross to Python as Arrow IPC, batch by batch,
and are decoded with pyarrow — never materializing the whole result (PGW-020).
Column schema is read from ResultSetMetaData up front (so the wire can send
RowDescription before any row streams); rows then stream one Arrow batch at a
time. On early stop (client disconnect, LIMIT-few, portal suspension) the
generator's finally cancels the Calcite statement and releases JVM/Arrow buffers
(PGW-022) — no leaked running query.

Concurrency: a single JDBC Connection is not thread-safe, so the backend lock is
held for the whole stream. Real concurrency is a Phase 3/5 concern (JVM sidecar +
pooling); correctness first.
"""

from __future__ import annotations

import logging
from typing import Iterator, List, Optional, Tuple

from pgwire_calcite import normalize

log = logging.getLogger(__name__)

#: rows per Arrow batch — bounds peak memory and paging granularity
DEFAULT_BATCH_SIZE = 1024


class _ArrowClasses:
    """Lazily-resolved JVM classes (after the JVM is started)."""

    _cache = None

    @classmethod
    def get(cls):
        if cls._cache is None:
            import jpype

            cls._cache = {
                "RootAllocator": jpype.JClass("org.apache.arrow.memory.RootAllocator"),
                "JdbcToArrow": jpype.JClass("org.apache.arrow.adapter.jdbc.JdbcToArrow"),
                "JdbcToArrowUtils": jpype.JClass(
                    "org.apache.arrow.adapter.jdbc.JdbcToArrowUtils"
                ),
                "ConfigBuilder": jpype.JClass(
                    "org.apache.arrow.adapter.jdbc.JdbcToArrowConfigBuilder"
                ),
                "ArrowStreamWriter": jpype.JClass(
                    "org.apache.arrow.vector.ipc.ArrowStreamWriter"
                ),
                "ByteArrayOutputStream": jpype.JClass("java.io.ByteArrayOutputStream"),
                "Channels": jpype.JClass("java.nio.channels.Channels"),
            }
        return cls._cache


#: Arrow type ids whose JDBC consumer must be replaced (see _consumer_factory).
_BINARY_TYPE_IDS = frozenset({"Binary", "LargeBinary", "FixedSizeBinary"})

_FACTORY_CACHE = None


class _ClosingIterator:
    """An iterator whose ``close()`` runs its release whether or not iteration ever started.

    ``stream_ipc_batches`` takes the backend lock BEFORE it returns, and a generator's
    ``finally`` only runs once the generator has started: closing one that was never
    iterated is a no-op. A result that is closed without being read -- a Describe, whose
    column types came from metadata so no batch was peeked -- therefore kept the lock held
    for good. The lock is re-entrant, so the same connection never noticed; every other
    connection waited out the queue bound and failed 'server is busy'. ``release`` must be
    idempotent: exhausting the generator runs it too.
    """

    def __init__(self, gen: Iterator, release) -> None:
        self._gen = gen
        self._release = release

    def __iter__(self) -> "_ClosingIterator":
        return self

    def __next__(self):
        return next(self._gen)

    def close(self) -> None:
        try:
            self._gen.close()  # type: ignore[attr-defined]
        finally:
            self._release()


def _closer(source):
    """``source.close`` when it has one, else a no-op (a plain iterator owns nothing)."""
    close = getattr(source, "close", None)
    return close if close is not None else (lambda: None)


def _consumer_factory(C):
    """A JdbcConsumerFactory that reads binary columns with ``ResultSet.getBytes``.

    arrow-jdbc's stock BinaryConsumer reads binary columns through
    ``getBinaryStream``, which Calcite's Avatica cursor does not implement --
    a VARBINARY column made the whole stream fail with "cannot convert to
    InputStream (binary)". Every other Arrow type keeps the stock consumer, so
    only binary columns cross into Python here, once per row.
    """
    global _FACTORY_CACHE
    if _FACTORY_CACHE is not None:
        return _FACTORY_CACHE
    import jpype

    @jpype.JImplements("org.apache.arrow.adapter.jdbc.consumer.JdbcConsumer")
    class _BytesConsumer:
        """Consume one binary column into a VarBinary/FixedSizeBinary vector."""

        def __init__(self, column: int, vector):
            self._column = column
            self._vector = vector
            self._index = 0

        @jpype.JOverride
        def consume(self, rs):
            value = rs.getBytes(self._column)
            if bool(rs.wasNull()):
                self._vector.setNull(self._index)
            else:
                self._vector.setSafe(self._index, value)
            self._index += 1

        @jpype.JOverride
        def resetValueVector(self, vector):
            # A new batch root: write from the top of the new vector.
            self._vector = vector
            self._index = 0

        @jpype.JOverride
        def close(self):
            self._vector.close()

    @jpype.JImplements("org.apache.arrow.adapter.jdbc.JdbcToArrowConfig$JdbcConsumerFactory")
    class _Factory:
        @jpype.JOverride
        def apply(self, arrow_type, column_index, nullable, vector, config):
            if str(arrow_type.getTypeID()) in _BINARY_TYPE_IDS:
                return _BytesConsumer(int(column_index), vector)
            # Every column is read as nullable, whatever the adapter declares. ``nullable`` is
            # the adapter's claim about the column, and arrow-jdbc's consumer for a column
            # claimed NOT NULL does not check for null: one null value in such a column
            # (seen on cloudops) threw NullPointerException and ended the connection.
            return C["JdbcToArrowUtils"].getConsumer(
                arrow_type, column_index, True, vector, config
            )

    _FACTORY_CACHE = _Factory()
    return _FACTORY_CACHE


def _columns_from_metadata(rs) -> Tuple[List[str], List[str]]:
    """Read (column_names, duckdb_labels) from ResultSetMetaData without consuming rows."""
    md = rs.getMetaData()
    n = int(md.getColumnCount())
    names = [normalize.pg_column_label(str(md.getColumnLabel(i))) for i in range(1, n + 1)]
    labels = [normalize.stream_type_label(str(md.getColumnTypeName(i))) for i in range(1, n + 1)]
    return names, labels


def stream_ipc_batches(
    conn,
    lock,
    sql: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
    cancel_scope=None,
) -> Tuple[List[str], List[str], Iterator[bytes]]:
    """Execute ``sql``; return (column_names, duckdb_labels, ipc_batch_generator).

    Each yielded item is a self-contained Arrow IPC stream for one batch (schema +
    one record batch). The generator holds ``lock`` and the JVM/Arrow resources for
    its lifetime and releases them in ``finally`` (completion, early stop, error).
    This is the shared core: the in-process rows path (``stream_query``) and the
    Calcite-child socket bridge both consume it.

    ``cancel_scope`` (``calcite_backend.CancelScope``) publishes the JDBC Statement
    so a CancelRequest on another connection — or the statement_timeout watchdog —
    can abort it, and translates the resulting engine failure into SQLSTATE 57014
    (PGW-050/051). It stays armed for the generator's whole lifetime, because a
    cancel can land while rows are still streaming.
    """
    if cancel_scope is None:
        # No scope from the caller: still take the lock through the server-wide
        # queue bound, so a leaked holder fails this statement instead of wedging it.
        from pgwire_calcite.calcite_backend import CancelScope

        CancelScope(None, 0).acquire(lock)
    else:
        cancel_scope.acquire(lock)
    acquired = True
    stmt = None
    allocator = None
    try:
        C = _ArrowClasses.get()
        stmt = conn.createStatement()
        stmt.setFetchSize(batch_size)
        if cancel_scope is not None:
            cancel_scope.arm(stmt)
        try:
            rs = stmt.executeQuery(sql)
        except BaseException:
            if cancel_scope is not None:
                cancel_scope.raise_if_canceled()
            raise
        names, labels = _columns_from_metadata(rs)

        allocator = C["RootAllocator"]()
        config = (
            C["ConfigBuilder"]()
            .setAllocator(allocator)
            .setCalendar(C["JdbcToArrowUtils"].getUtcCalendar())
            .setTargetBatchSize(int(batch_size))
            .setJdbcConsumerGetter(_consumer_factory(C))
            .build()
        )
        iterator = C["JdbcToArrow"].sqlToArrowVectorIterator(rs, config)
    except BaseException:
        if cancel_scope is not None:
            cancel_scope.disarm()
        _cleanup(stmt, allocator)
        if acquired:
            lock.release()
        raise

    def _release() -> None:
        nonlocal acquired
        if not acquired:
            return
        acquired = False
        if cancel_scope is not None:
            cancel_scope.disarm()
        try:
            iterator.close()
        except Exception:
            pass
        _cleanup(stmt, allocator)
        lock.release()

    def _ipc_gen() -> Iterator[bytes]:
        try:
            while True:
                try:
                    if cancel_scope is not None:
                        cancel_scope.enter_engine()
                    if not bool(iterator.hasNext()):
                        break
                    root = iterator.next()
                except BaseException:
                    if cancel_scope is not None:
                        cancel_scope.raise_if_canceled()
                    raise
                try:
                    batch = _root_to_ipc_bytes(C, root)
                finally:
                    root.close()  # release this batch's off-heap buffers promptly
                # From here until the consumer asks for the next batch the engine is
                # idle: the time is the client's (reading these rows, or not).
                if cancel_scope is not None:
                    cancel_scope.leave_engine()
                yield batch
        finally:
            _release()

    # The statement has returned its first answer; until the first batch is pulled the
    # connection is held for the consumer, not by the engine.
    if cancel_scope is not None:
        cancel_scope.leave_engine()
    return names, labels, _ClosingIterator(_ipc_gen(), _release)


def batches_from_ipc(ipc_batches: Iterator[bytes]) -> Iterator[List[tuple]]:
    """Decode per-batch Arrow IPC bytes into batches of Python row tuples.

    Batch granularity is preserved (rather than flattened to rows) so the wire layer
    can bound resident memory to one batch and, when a column type must be inferred
    from data, buffer exactly one batch before sending RowDescription (PGW-020).

    Closing the result closes ``ipc_batches`` too -- also when it was never iterated -- so
    an early stop propagates down to the JDBC statement cancel/close in
    ``stream_ipc_batches`` (PGW-022).
    """
    import pyarrow as pa

    close_source = _closer(ipc_batches)

    def _gen() -> Iterator[List[tuple]]:
        try:
            for ipc in ipc_batches:
                table = pa.ipc.open_stream(ipc).read_all()
                pydata = [col.to_pylist() for col in table.columns]
                yield [tuple(col[r] for col in pydata) for r in range(table.num_rows)]
        finally:
            close_source()

    return _ClosingIterator(_gen(), close_source)


def rows_from_ipc(ipc_batches: Iterator[bytes]) -> Iterator[tuple]:
    """Decode a stream of per-batch Arrow IPC bytes into Python row tuples."""
    batches = batches_from_ipc(ipc_batches)

    def _gen() -> Iterator[tuple]:
        try:
            for batch in batches:
                yield from batch
        finally:
            batches.close()

    return _ClosingIterator(_gen(), batches.close)


def stream_query_batches(
    conn,
    lock,
    sql: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
    cancel_scope=None,
) -> Tuple[List[str], List[str], Iterator[List[tuple]]]:
    """Execute ``sql`` and return (column_names, duckdb_labels, batch_generator).

    This is the shape the wire layer wants: one batch of at most ``batch_size`` rows
    is resident at a time, and the first batch can be peeked for type inference
    without draining the result.
    """
    names, labels, ipc = stream_ipc_batches(conn, lock, sql, batch_size, cancel_scope=cancel_scope)
    return names, labels, batches_from_ipc(ipc)


def stream_query(
    conn,
    lock,
    sql: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
    cancel_scope=None,
) -> Tuple[List[str], List[str], Iterator[tuple]]:
    """Execute ``sql`` and return (column_names, duckdb_labels, row_generator)."""
    names, labels, ipc = stream_ipc_batches(conn, lock, sql, batch_size, cancel_scope=cancel_scope)
    return names, labels, rows_from_ipc(ipc)


def _root_to_ipc_bytes(C, root) -> bytes:
    baos = C["ByteArrayOutputStream"]()
    writer = C["ArrowStreamWriter"](root, None, C["Channels"].newChannel(baos))
    writer.start()
    writer.writeBatch()
    writer.end()
    writer.close()
    return bytes(baos.toByteArray())


def _cleanup(stmt, allocator) -> None:
    if stmt is not None:
        try:
            stmt.cancel()  # cancel any still-running Calcite query (PGW-022)
        except Exception:
            pass
        try:
            stmt.close()
        except Exception:
            pass
    if allocator is not None:
        try:
            allocator.close()
        except Exception:
            pass
