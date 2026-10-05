# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A streamed result that is closed without ever being read releases the backend lock.

``stream_ipc_batches`` takes the lock before it returns. A Describe closes its result
without reading a row (the column types came from metadata, so no batch was peeked);
the lock is re-entrant, so the same connection carried on, and every OTHER connection
waited out the queue bound and failed 'server is busy'. The JDBC/Arrow classes are
faked: what is asserted is the lock's lifetime, not the engine.
"""

from __future__ import annotations

import threading
from unittest.mock import MagicMock

import pytest

from pgwire_calcite import arrow_bridge
from pgwire_calcite.types import QueryResult


@pytest.fixture
def no_engine(monkeypatch):
    """Arrow/JDBC classes and metadata that need no JVM; the iterator yields nothing."""
    classes = MagicMock()
    classes["JdbcToArrow"].sqlToArrowVectorIterator.return_value.hasNext.return_value = False
    monkeypatch.setattr(arrow_bridge._ArrowClasses, "get", staticmethod(lambda: classes))
    monkeypatch.setattr(arrow_bridge, "_consumer_factory", lambda C: None)
    monkeypatch.setattr(
        arrow_bridge, "_columns_from_metadata", lambda rs: (["id"], ["INTEGER"])
    )


def _free_for_another_thread(lock) -> bool:
    got: list[bool] = []

    def other():
        acquired = lock.acquire(timeout=1)
        got.append(acquired)
        if acquired:
            lock.release()

    t = threading.Thread(target=other)
    t.start()
    t.join(2)
    return got == [True]


@pytest.mark.parametrize(
    "stream",
    [arrow_bridge.stream_ipc_batches, arrow_bridge.stream_query_batches, arrow_bridge.stream_query],
)
def test_closing_an_unread_stream_releases_the_lock(no_engine, stream):
    lock = threading.RLock()
    _names, _labels, batches = stream(MagicMock(), lock, "SELECT 1")
    assert not _free_for_another_thread(lock)  # held while the stream is open
    batches.close()
    assert _free_for_another_thread(lock)


def test_exhausting_a_stream_releases_the_lock(no_engine):
    lock = threading.RLock()
    _names, _labels, batches = arrow_bridge.stream_query_batches(MagicMock(), lock, "SELECT 1")
    assert list(batches) == []
    assert _free_for_another_thread(lock)
    batches.close()  # idempotent after exhaustion
    assert _free_for_another_thread(lock)


def test_a_described_result_closed_unread_frees_the_engine_for_other_connections(no_engine):
    """The wire layer's path: a Describe builds the result and closes it unread."""
    from pgwire_calcite.server import CalciteQueryResult

    lock = threading.RLock()
    names, labels, batches = arrow_bridge.stream_query_batches(MagicMock(), lock, "SELECT 1")
    result = CalciteQueryResult(
        QueryResult(column_names=names, column_types=labels, row_batches=batches), "SELECT 1"
    )
    result.close()
    assert _free_for_another_thread(lock)
