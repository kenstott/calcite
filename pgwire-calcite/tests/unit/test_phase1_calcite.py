# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Phase 1 exit-gate tests: PG->Calcite transpile executed against embedded JDBC.

Covers PGW-015 (non-catalog queries transpiled + executed, returning rows/cols/
types) end to end, and the PG-only reject path (PGW-018) through the real engine.
Boots one embedded Calcite JVM per test session (JPype is a per-process
singleton). Skips with a clear reason only if the Calcite classpath cannot be
resolved (never silently passes).
"""

from __future__ import annotations

import pathlib
import time

import pytest

from pgwire_calcite import launcher
from pgwire_calcite.classpath import ClasspathError, resolve_classpath

from test_phase0_wire import MiniPgClient, _free_port

FIXTURES = pathlib.Path(__file__).resolve().parents[1] / "fixtures"
MODEL = str(FIXTURES / "file-model.json")


@pytest.fixture(scope="session")
def calcite_backend():
    try:
        resolve_classpath()
    except ClasspathError as exc:
        pytest.skip(f"Calcite classpath unavailable: {exc}")
    from pgwire_calcite.calcite_backend import CalciteBackend

    backend = CalciteBackend(model_path=MODEL, jvm_args=["-Xmx1g"])
    yield backend
    backend.close()


def test_select_one(calcite_backend):
    r = calcite_backend.execute_sql("SELECT 1", "u")
    assert r.rows == [(1,)]


def test_count_star(calcite_backend):
    r = calcite_backend.execute_sql("SELECT count(*) AS n FROM EMPS", "u")
    assert r.rows == [(10,)]
    assert r.column_names == ["N"]


def test_join_filter_group_aggregate_limit(calcite_backend):
    sql = (
        "SELECT d.DNAME, count(*) AS c, avg(e.SAL) AS a "
        "FROM EMPS e JOIN DEPTS d ON e.DEPTNO = d.DEPTNO "
        "WHERE e.SAL > 1000 GROUP BY d.DNAME ORDER BY c DESC LIMIT 5"
    )
    r = calcite_backend.execute_sql(sql, "u")
    assert r.column_names == ["DNAME", "C", "A"]
    assert r.column_types == ["VARCHAR", "BIGINT", "DOUBLE"]
    assert r.rows == [
        ("SALES", 5, 1690.0),
        ("RESEARCH", 2, 2987.5),
        ("ACCOUNTING", 2, 3725.0),
    ]


def test_limit_offset(calcite_backend):
    r = calcite_backend.execute_sql("SELECT ENAME FROM EMPS ORDER BY ENAME LIMIT 3", "u")
    assert [row[0] for row in r.rows] == ["ALLEN", "BLAKE", "CLARK"]


def test_pg_only_construct_rejected_through_engine(calcite_backend):
    from pgwire_calcite.dialect import UnsupportedConstruct

    with pytest.raises(UnsupportedConstruct):
        calcite_backend.execute_sql("SELECT DISTINCT ON (DEPTNO) DEPTNO FROM EMPS", "u")


def test_end_to_end_through_wire_server(calcite_backend):
    """psql-style client -> wire server -> CalciteBackend -> file adapter."""
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", backend=calcite_backend)
    time.sleep(0.1)
    try:
        c = MiniPgClient("127.0.0.1", port)
        try:
            r = c.query(
                "SELECT d.DNAME, count(*) AS c FROM EMPS e "
                "JOIN DEPTS d ON e.DEPTNO = d.DEPTNO GROUP BY d.DNAME ORDER BY d.DNAME"
            )
            assert r["error"] is None, r["error"]
            assert r["columns"] == ["DNAME", "C"]
            # Values arrive as text over the simple-query protocol.
            assert r["rows"] == [["ACCOUNTING", "2"], ["RESEARCH", "3"], ["SALES", "5"]]
        finally:
            c.close()
    finally:
        srv.shutdown()
