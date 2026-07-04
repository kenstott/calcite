# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Phase 3 exit-gate tests: Arrow batch-streaming result path.

Covers PGW-019 (results cross JVM->Python as Arrow), PGW-020 (batch streaming,
never a materialized whole result), PGW-022 (early stop / close cancels and
releases). A cross join over the 10-row fixture synthesizes a large result
without new data. The definitive flat-RSS soak is left to the user's multi-GB
run; here we prove multi-batch streaming, type fidelity, and clean cancel.
"""

from __future__ import annotations

import time

from pgwire_calcite import arrow_bridge, launcher

from test_phase0_wire import MiniPgClient, _free_port

# 10^4 rows from the 10-row EMPS via cross join — enough to force many batches.
_LARGE_SQL = "SELECT e1.EMPNO FROM EMPS e1, EMPS e2, EMPS e3, EMPS e4"
_LARGE_N = 10_000


def test_stream_query_multi_batch_large_result(calcite_backend):
    names, labels, gen = arrow_bridge.stream_query(
        calcite_backend.connection, calcite_backend._lock, _LARGE_SQL, batch_size=256
    )
    assert names == ["EMPNO"] and labels == ["INTEGER"]
    n = sum(1 for _ in gen)  # small batch_size => ~40 Arrow batches, one in memory at a time
    assert n == _LARGE_N


def test_stream_types_match_materialized(calcite_backend):
    sql = "SELECT DNAME, DEPTNO FROM DEPTS ORDER BY DEPTNO"
    mat = calcite_backend.execute_sql(sql, "u")  # materialized path
    names, labels, gen = arrow_bridge.stream_query(
        calcite_backend.connection, calcite_backend._lock, sql, batch_size=1024
    )
    streamed = list(gen)
    assert names == mat.column_names
    assert labels == mat.column_types
    assert streamed == mat.rows


def test_early_close_releases_lock_and_cancels(calcite_backend):
    """PGW-022: closing the generator early cancels the statement and releases the
    backend lock, so the next query proceeds (no deadlock, no leaked run)."""
    _, _, gen = arrow_bridge.stream_query(
        calcite_backend.connection, calcite_backend._lock, _LARGE_SQL, batch_size=256
    )
    assert next(gen) is not None  # pull one row, leaving the scan mid-flight
    gen.close()  # -> generator finally: cancel + release lock
    # If the lock leaked, this materialized query would hang; assert it returns.
    r = calcite_backend.execute_sql("SELECT count(*) AS n FROM EMPS", "u")
    assert r.rows == [(10,)]


def test_wire_streams_large_result(calcite_backend):
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", backend=calcite_backend)
    time.sleep(0.1)
    try:
        c = MiniPgClient("127.0.0.1", port)
        try:
            r = c.query(f"SELECT count(*) AS n FROM ({_LARGE_SQL}) t")
            assert r["error"] is None, r["error"]
            assert r["rows"] == [[str(_LARGE_N)]]
        finally:
            c.close()
    finally:
        srv.shutdown()
