# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""information_schema.schemata / tables / columns / views are answered from the role's catalog
cache, never by Calcite (PGW-012, PGW-045).

Calcite's InformationSchema resolves every table it lists, and resolving an Iceberg table is a
round trip to the object store: a listing its filter could not narrow (lower(table_name) = ...,
or no filter at all) walked the whole corpus over the network, timed out, and left the shared
connection wedged. The backend here refuses every statement, so any read that reached it fails.
"""

from __future__ import annotations

import pytest

from pgwire_calcite import catalog, launcher
from pgwire_calcite.authz import RoleGrants
from pgwire_calcite.backend import StubBackend
from pgwire_calcite.catalog_populate import (
    deserialize_catalog,
    install_catalog,
    serialize_catalog,
)
from pgwire_calcite.compiler.sql_gen import ColumnMeta, CompilationContext, TableMeta


class _RefusingBackend(StubBackend):
    def execute_sql(self, *args, **kwargs):
        raise AssertionError(f"information_schema read reached Calcite: {args[0]!r}")


def _catalog():
    depts = TableMeta(1, "postgres", "SALES", "depts", "SALES.depts", comment="Departments")
    emps = TableMeta(2, "postgres", "SALES", "emps", "SALES.emps")
    big = TableMeta(
        3, "postgres", "HR", "big_emps", "HR.big_emps", comment="Large staff", table_type="VIEW"
    )
    ctx = CompilationContext(
        tables={t.type_name: t for t in (depts, emps, big)},
        schema_comments={"SALES": "Sales data", "HR": None, "EMPTY": "No tables yet"},
    )
    column_types = {
        1: [
            ColumnMeta("deptno", "INTEGER", False, comment="Department number"),
            ColumnMeta("dname", "VARCHAR"),
        ],
        2: [ColumnMeta("empno", "INTEGER", False), ColumnMeta("sal", "DOUBLE")],
        3: [ColumnMeta("empno", "INTEGER")],
    }
    return ctx, column_types


def _state(grants=None):
    catalog.invalidate_catalog_cache()
    state = launcher.build_state(backend=_RefusingBackend())
    if grants is not None:
        state.authz_grants = RoleGrants.from_dict(grants)
    ctx, column_types = _catalog()
    install_catalog(state, ctx, column_types)
    return state


def _rows(state, sql, role="tester"):
    assert catalog.classify(sql) == "INTERCEPT", sql
    return [tuple(r) for r in catalog.answer(sql, role, state).rows]


@pytest.mark.parametrize("view", ["schemata", "tables", "columns", "views"])
def test_the_four_views_are_answered_from_the_catalog(view):
    assert catalog.classify(f"SELECT * FROM information_schema.{view}") == "INTERCEPT"


def test_tables_carry_type_and_comment():
    rows = _rows(
        _state(),
        "SELECT table_schema, table_name, table_type, remarks FROM information_schema.tables "
        "ORDER BY 1, 2",
    )
    assert rows == [
        ("HR", "big_emps", "VIEW", "Large staff"),
        ("SALES", "depts", "BASE TABLE", "Departments"),
        ("SALES", "emps", "BASE TABLE", None),
    ]


def test_a_filter_on_lower_names_is_answered_without_calcite():
    rows = _rows(
        _state(),
        "SELECT table_type, remarks FROM information_schema.tables "
        "WHERE lower(table_schema) = 'sales' AND lower(table_name) = 'depts'",
    )
    assert rows == [("BASE TABLE", "Departments")]


def test_columns_report_pg_type_names_order_and_comments():
    rows = _rows(
        _state(),
        "SELECT column_name, data_type, is_nullable, remarks FROM information_schema.columns "
        "WHERE table_schema = 'SALES' AND table_name = 'depts' ORDER BY ordinal_position",
    )
    assert rows == [
        ("deptno", "integer", "NO", "Department number"),
        ("dname", "character varying", "YES", None),
    ]
    rows = _rows(
        _state(),
        "SELECT data_type FROM information_schema.columns WHERE table_name = 'emps' "
        "AND column_name = 'sal'",
    )
    assert rows == [("double precision",)]


def test_schemata_lists_every_walked_schema_with_its_comment():
    rows = _rows(
        _state(), "SELECT schema_name, remarks FROM information_schema.schemata ORDER BY 1"
    )
    assert rows == [("EMPTY", "No tables yet"), ("HR", None), ("SALES", "Sales data")]


def test_the_identity_probe_keeps_its_aggregate():
    rows = _rows(
        _state(),
        "SELECT count(*) FROM information_schema.schemata WHERE schema_name IN ('SALES', 'HR')",
    )
    assert rows == [(2,)]


def test_an_unfiltered_listing_is_answered_without_calcite():
    assert _rows(_state(), "SELECT count(*) FROM information_schema.tables") == [(3,)]
    assert _rows(
        _state(), "SELECT DISTINCT column_name FROM information_schema.columns ORDER BY 1"
    ) == [("deptno",), ("dname",), ("empno",), ("sal",)]


def test_a_role_sees_only_its_granted_tables_and_their_schemas():
    state = _state({"tester": {"SALES.depts"}})
    assert _rows(state, "SELECT table_schema, table_name FROM information_schema.tables") == [
        ("SALES", "depts")
    ]
    assert _rows(state, "SELECT schema_name FROM information_schema.schemata") == [("SALES",)]
    assert _rows(
        state, "SELECT DISTINCT table_name FROM information_schema.columns"
    ) == [("depts",)]


def test_a_role_with_nothing_granted_sees_no_rows():
    state = _state({"someone_else": {"*"}})
    assert _rows(state, "SELECT * FROM information_schema.tables") == []
    assert _rows(state, "SELECT * FROM information_schema.schemata") == []


def test_table_type_and_schema_comments_survive_the_bridge():
    ctx, column_types = _catalog()
    back, _ = deserialize_catalog(serialize_catalog(ctx, column_types))
    assert back.tables["HR.big_emps"].table_type == "VIEW"
    assert back.schema_comments == ctx.schema_comments
