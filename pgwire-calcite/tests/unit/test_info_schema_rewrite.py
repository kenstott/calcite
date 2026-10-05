# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""info_schema.rewrite: the SQL a PG client's information_schema read becomes before Calcite runs
it (PGW-012, PGW-045). The end-to-end contract — what a client sees — is test_phase2_catalog,
test_phase5_sidecar and test_phase5b_authz; these pin the rewrite itself without a JVM."""

from __future__ import annotations

import sqlglot
import sqlglot.expressions as exp

from pgwire_calcite import info_schema

VISIBLE = [("SALES", "depts"), ("SALES", "emps")]
COLUMNS = ["TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "DATA_TYPE"]


def _columns_of():
    return COLUMNS


def _never():
    raise AssertionError("only a statement reading information_schema.columns asks for them")


def test_a_statement_without_information_schema_is_left_alone():
    assert info_schema.rewrite("SELECT * FROM SALES.emps", VISIBLE, _never) is None
    assert not info_schema.references("SELECT * FROM SALES.emps")


def test_tables_is_scoped_to_the_roles_catalog():
    sql = info_schema.rewrite(
        "SELECT table_name FROM information_schema.tables WHERE table_schema <> 'pg_catalog'",
        VISIBLE,
        _never,
    )
    assert sql is not None
    tree = sqlglot.parse_one(sql, read="postgres")
    subquery = tree.find(exp.Subquery)
    assert subquery is not None and subquery.alias == "tables"
    inner = subquery.this.sql(dialect="postgres")
    assert "table_schema IN ('SALES')" in inner
    assert "IN ('SALES.depts', 'SALES.emps')" in inner
    # the client's own predicate still applies, outside the scope
    assert tree.args["where"] is not None


def test_a_role_with_nothing_granted_sees_no_rows():
    sql = info_schema.rewrite("SELECT * FROM information_schema.schemata", [], _never)
    assert sql is not None and "schema_name IN (NULL)" in sql


def test_columns_restates_data_type_in_pg_names_and_keeps_every_column():
    sql = info_schema.rewrite(
        "SELECT c.column_name, c.data_type FROM information_schema.columns AS c", VISIBLE, _columns_of
    )
    assert sql is not None
    for column in COLUMNS:
        assert f'"{column}"' in sql
    for calcite, pg in (
        ("DOUBLE PRECISION", "double precision"),
        ("INTEGER", "integer"),
        ("VARCHAR", "character varying"),
    ):
        assert f"LIKE '{calcite} %' THEN '{pg}'" in sql
    # the longer name is tested before the shorter one it starts with
    assert sql.index("'DOUBLE PRECISION'") < sql.index("'DOUBLE'")
    # the client's alias still names the derived table
    subquery = sqlglot.parse_one(sql, read="postgres").find(exp.Subquery)
    assert subquery is not None and subquery.alias == "c"


def test_the_identity_probe_keeps_its_aggregate():
    sql = info_schema.rewrite(
        "SELECT count(*) FROM information_schema.schemata WHERE schema_name IN ('SALES')",
        VISIBLE,
        _never,
    )
    assert sql is not None
    tree = sqlglot.parse_one(sql, read="postgres")
    assert tree.find(exp.Count) is not None
    subquery = tree.find(exp.Subquery)
    assert subquery is not None
    assert "schema_name IN ('SALES')" in subquery.this.sql(dialect="postgres")
