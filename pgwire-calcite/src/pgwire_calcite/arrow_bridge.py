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


def _columns_from_metadata(rs) -> Tuple[List[str], List[str]]:
    """Read (column_names, duckdb_labels) from ResultSetMetaData without consuming rows."""
    md = rs.getMetaData()
    n = int(md.getColumnCount())
    names = [normalize.pg_column_label(str(md.getColumnLabel(i))) for i in range(1, n + 1)]
    labels = [normalize.duckdb_label(str(md.getColumnTypeName(i))) for i in range(1, n + 1)]
    return names, labels


def stream_ipc_batches(
    conn,
    lock,
    sql: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> Tuple[List[str], List[str], Iterator[bytes]]:
    """Execute ``sql``; return (column_names, duckdb_labels, ipc_batch_generator).

    Each yielded item is a self-contained Arrow IPC stream for one batch (schema +
    one record batch). The generator holds ``lock`` and the JVM/Arrow resources for
    its lifetime and releases them in ``finally`` (completion, early stop, error).
    This is the shared core: the in-process rows path (``stream_query``) and the
    Calcite-child socket bridge both consume it.
    """
    C = _ArrowClasses.get()
    lock.acquire()
    acquired = True
    stmt = None
    allocator = None
    try:
        stmt = conn.createStatement()
        stmt.setFetchSize(batch_size)
        rs = stmt.executeQuery(sql)
        names, labels = _columns_from_metadata(rs)

        allocator = C["RootAllocator"]()
        config = (
            C["ConfigBuilder"]()
            .setAllocator(allocator)
            .setCalendar(C["JdbcToArrowUtils"].getUtcCalendar())
            .setTargetBatchSize(int(batch_size))
            .build()
        )
        iterator = C["JdbcToArrow"].sqlToArrowVectorIterator(rs, config)
    except BaseException:
        _cleanup(stmt, allocator)
        if acquired:
            lock.release()
        raise

    def _ipc_gen() -> Iterator[bytes]:
        nonlocal acquired
        try:
            while bool(iterator.hasNext()):
                root = iterator.next()
                try:
                    yield _root_to_ipc_bytes(C, root)
                finally:
                    root.close()  # release this batch's off-heap buffers promptly
        finally:
            try:
                iterator.close()
            except Exception:
                pass
            _cleanup(stmt, allocator)
            if acquired:
                acquired = False
                lock.release()

    return names, labels, _ipc_gen()


def rows_from_ipc(ipc_batches: Iterator[bytes]) -> Iterator[tuple]:
    """Decode a stream of per-batch Arrow IPC bytes into Python row tuples."""
    import pyarrow as pa

    for ipc in ipc_batches:
        table = pa.ipc.open_stream(ipc).read_all()
        pydata = [col.to_pylist() for col in table.columns]
        for r in range(table.num_rows):
            yield tuple(col[r] for col in pydata)


def stream_query(
    conn,
    lock,
    sql: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> Tuple[List[str], List[str], Iterator[tuple]]:
    """Execute ``sql`` and return (column_names, duckdb_labels, row_generator)."""
    names, labels, ipc = stream_ipc_batches(conn, lock, sql, batch_size)
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
