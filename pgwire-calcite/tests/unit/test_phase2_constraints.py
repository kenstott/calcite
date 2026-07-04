# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Keys -> pg_constraint -> ER-diagram data (PGW-006/012).

The file CSV adapter exposes no keys (Statistic.getKeys() is None) so it correctly
reports empty (PGW-013). This test drives the OTHER side: given a key-bearing
catalog model (as an adapter exposing PKs/FKs would produce), the intercept must
render the pg_constraint PK ('p') and FK ('f') rows — the exact metadata DBeaver/
DataGrip read to draw ER relationship lines. Uses the StubBackend (catalog queries
are intercepted, no JVM needed) with a synthetic keyed CompilationContext.
"""

from __future__ import annotations

import time

import pytest

from pgwire_calcite import launcher
from pgwire_calcite.backend import StubBackend
from pgwire_calcite.catalog_populate import install_catalog
from pgwire_calcite.compiler.sql_gen import ColumnMeta, CompilationContext, JoinMeta, TableMeta

from test_phase0_wire import MiniPgClient, _free_port


def _keyed_catalog():
    depts = TableMeta(1, "calcite", "SALES", "depts", "SALES.depts")
    emps = TableMeta(2, "calcite", "SALES", "emps", "SALES.emps")
    ctx = CompilationContext(
        tables={depts.type_name: depts, emps.type_name: emps},
        pk_columns={1: ["deptno"], 2: ["empno"]},
        joins={
            ("SALES.emps", "deptno"): JoinMeta(
                source_column="deptno", target_column="deptno", target=depts,
                cardinality="many-to-one",
            )
        },
    )
    column_types = {
        1: [ColumnMeta("deptno", "INTEGER", False), ColumnMeta("dname", "VARCHAR")],
        2: [
            ColumnMeta("empno", "INTEGER", False),
            ColumnMeta("deptno", "INTEGER"),
            ColumnMeta("ename", "VARCHAR"),
        ],
    }
    return ctx, column_types


@pytest.fixture()
def keyed_server():
    ctx, column_types = _keyed_catalog()
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", backend=StubBackend())
    import pgwire_calcite.server as server_mod

    install_catalog(server_mod.state, ctx, column_types)  # inject the keyed catalog
    assert server_mod.state.catalog_enabled
    time.sleep(0.1)
    yield "127.0.0.1", port
    srv.shutdown()


def test_pg_constraint_has_primary_keys(keyed_server):
    host, port = keyed_server
    c = MiniPgClient(host, port)
    try:
        r = c.query(
            "SELECT r.relname, c.contype FROM pg_catalog.pg_constraint c "
            "JOIN pg_catalog.pg_class r ON r.oid = c.conrelid "
            "WHERE c.contype = 'p' ORDER BY r.relname"
        )
        assert r["error"] is None, r["error"]
        assert r["rows"] == [["depts", "p"], ["emps", "p"]]
    finally:
        c.close()


def test_pg_constraint_has_foreign_key_for_er_diagram(keyed_server):
    """The FK row DBeaver/DataGrip read to draw a relationship line emps -> depts."""
    host, port = keyed_server
    c = MiniPgClient(host, port)
    try:
        r = c.query(
            "SELECT src.relname AS from_tbl, tgt.relname AS to_tbl, c.contype "
            "FROM pg_catalog.pg_constraint c "
            "JOIN pg_catalog.pg_class src ON src.oid = c.conrelid "
            "JOIN pg_catalog.pg_class tgt ON tgt.oid = c.confrelid "
            "WHERE c.contype = 'f'"
        )
        assert r["error"] is None, r["error"]
        assert r["rows"] == [["emps", "depts", "f"]]  # ER relationship: emps -> depts
    finally:
        c.close()


def test_keyless_catalog_still_reports_empty(keyed_server):
    """Sanity: a table with no PK/FK contributes no constraint rows (PGW-013)."""
    ctx, column_types = _keyed_catalog()
    ctx.pk_columns.clear()
    ctx.joins.clear()
    import pgwire_calcite.server as server_mod

    install_catalog(server_mod.state, ctx, column_types)
    host, port = keyed_server
    c = MiniPgClient(host, port)
    try:
        r = c.query(
            "SELECT count(*) AS n FROM pg_catalog.pg_constraint c "
            "JOIN pg_catalog.pg_class r ON r.oid = c.conrelid "
            "WHERE lower(r.relname) IN ('emps','depts')"
        )
        assert r["error"] is None, r["error"]
        assert r["rows"] == [["0"]]
    finally:
        c.close()
