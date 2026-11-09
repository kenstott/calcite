# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""insert_rows must write exactly what executemany writes, in one statement.

The catalog build used executemany, which DuckDB runs once per row: about 24k rows took over
a minute on the full corpus, so the first catalog query after a data-server start timed out.
"""

from __future__ import annotations

import time

import duckdb
import pytest

from pgwire_calcite.bulk_insert import insert_rows

_DDL = (
    "CREATE TABLE t (oid BIGINT, name VARCHAR, flag BOOLEAN, kind VARCHAR, n INTEGER, "
    "acl VARCHAR[], note VARCHAR)"
)
_ROWS = [
    (1, "a", True, 5, ["x", "y"]),
    (2, "b?", False, None, None),
    (3, None, None, 7, []),
]


def _table(sql, rows, bulk):
    db = duckdb.connect(":memory:")
    db.execute(_DDL)
    if bulk:
        insert_rows(db, sql, rows)
    else:
        db.executemany(sql, rows)
    return db.execute("SELECT * FROM t").fetchall(), db.execute("DESCRIBE t").fetchall()


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO t VALUES (?,?,?,'BASE TABLE',?,?,NULL)",
        "INSERT INTO t VALUES (?, ?, ?, 'what?', ?, ?, 'it''s')",
        "INSERT INTO t (oid, name, flag, n, acl) VALUES (?,?,?,?,?)",
    ],
)
def test_same_rows_as_executemany(sql):
    assert _table(sql, _ROWS, bulk=True) == _table(sql, _ROWS, bulk=False)


def test_insertion_order_kept():
    rows = [(i, f"r{i}", i % 2 == 0, i, None) for i in range(5000, 0, -1)]
    sql = "INSERT INTO t VALUES (?,?,?,NULL,?,?,NULL)"
    assert _table(sql, rows, bulk=True) == _table(sql, rows, bulk=False)


def test_no_rows_inserts_nothing():
    db = duckdb.connect(":memory:")
    db.execute(_DDL)
    insert_rows(db, "INSERT INTO t VALUES (?,?,?,NULL,?,?,NULL)", [])
    assert db.execute("SELECT count(*) FROM t").fetchone()[0] == 0


def test_row_width_mismatch_raises():
    db = duckdb.connect(":memory:")
    db.execute(_DDL)
    with pytest.raises(ValueError, match="3 values, statement has 5 placeholders"):
        insert_rows(db, "INSERT INTO t VALUES (?,?,?,NULL,?,?,NULL)", [(1, "a", True)])


def test_non_insert_statement_raises():
    db = duckdb.connect(":memory:")
    with pytest.raises(ValueError, match="needs INSERT INTO"):
        insert_rows(db, "UPDATE t SET n = ?", [(1,)])


def test_twenty_thousand_rows_in_one_statement_is_fast():
    db = duckdb.connect(":memory:")
    db.execute(_DDL)
    rows = [(i, f"col_{i}", True, i, ["r"]) for i in range(20_000)]
    started = time.monotonic()
    insert_rows(db, "INSERT INTO t VALUES (?,?,?,'c',?,?,NULL)", rows)
    elapsed = time.monotonic() - started
    assert db.execute("SELECT count(*) FROM t").fetchone()[0] == 20_000
    assert elapsed < 2.0, f"20k-row insert took {elapsed:.2f}s"
