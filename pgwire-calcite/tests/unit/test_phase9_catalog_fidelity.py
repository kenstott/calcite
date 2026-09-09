# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Phase 2 catalog fidelity (PGW-051).

The catalog intercept answered the shape of the DBeaver/DataGrip introspection
joins but not their content: chained reg-casts collapsed to 0, format_type()
returned PG's internal type names, pg_class.reltuples was hardcoded 0, and
pg_proc / information_schema.routines / .parameters / pg_available_extension_versions
were created and never filled. These tests pin each surface to what real
PostgreSQL answers.
"""

from __future__ import annotations

import pytest

from pgwire_calcite import catalog, launcher
from pgwire_calcite.backend import StubBackend
from pgwire_calcite.catalog_populate import install_catalog, populate_state
from pgwire_calcite.compiler.sql_gen import ColumnMeta, CompilationContext, JoinMeta, TableMeta


def _rows(state, sql):
    result = catalog.answer(sql, "tester", state)
    assert result is not None, f"not intercepted: {sql}"
    return [tuple(r) for r in result.rows]


def _scalar(state, sql):
    rows = _rows(state, sql)
    assert len(rows) == 1 and len(rows[0]) == 1, rows
    return rows[0][0]


@pytest.fixture(scope="module")
def calcite_state(calcite_backend):
    """Server state whose catalog is populated from the live Calcite file adapter."""
    state = launcher.build_state(backend=calcite_backend)
    populate_state(calcite_backend.connection, state)
    yield state
    catalog.invalidate_catalog_cache()


# --------------------------------------------------------------------------- 1
# Chained ::regclass::oid and class-name -> OID casts.


def test_chained_regclass_oid_cast_yields_the_real_class_oid(calcite_state):
    assert _scalar(calcite_state, "SELECT 'pg_extension'::regclass::oid") == 3079
    assert _scalar(calcite_state, "SELECT 'pg_catalog.pg_class'::regclass::oid") == 1259


def test_oid_cast_preserves_a_numeric_operand(calcite_state):
    """DataGrip binds `relnamespace = 2215::oid`; collapsing to 0 broke the predicate."""
    assert _scalar(calcite_state, "SELECT 2215::oid") == 2215


def test_pg_depend_refclassid_join_parses_and_runs(calcite_state):
    """The RetrieveExtensions/RetrieveDependencies shape: a chained cast in a predicate."""
    assert (
        _scalar(
            calcite_state,
            "SELECT count(*) FROM pg_catalog.pg_depend d "
            "WHERE d.refclassid = 'pg_extension'::regclass::oid",
        )
        == 0
    )


def test_regclass_literal_still_matches_pg_description_classoid(calcite_state):
    """KEEP: the short-name mapping the removed regex pre-pass used to provide."""
    assert _scalar(calcite_state, "SELECT 'pg_catalog.pg_class'::regclass") == "pg_class"


# --------------------------------------------------------------------------- 2
# format_type() returns SQL display names, with the type modifier PG prints.


def test_format_type_matches_postgres_display_names(calcite_state):
    rows = _rows(
        calcite_state,
        "SELECT a.attname, format_type(a.atttypid, a.atttypmod) "
        "FROM pg_catalog.pg_attribute a "
        "JOIN pg_catalog.pg_class c ON c.oid = a.attrelid "
        "WHERE lower(c.relname) = 'emps' AND a.attnum > 0 ORDER BY a.attnum",
    )
    assert rows == [
        ("empno", "integer"),
        ("ename", "character varying"),
        ("deptno", "integer"),
        ("sal", "double precision"),
    ]


@pytest.mark.parametrize(
    "typid,typmod,expected",
    [
        (23, -1, "integer"),  # int4
        (20, -1, "bigint"),  # int8
        (21, -1, "smallint"),  # int2
        (16, -1, "boolean"),  # bool
        (700, -1, "real"),  # float4
        (701, -1, "double precision"),  # float8
        (1043, -1, "character varying"),  # varchar, no modifier
        (1043, 54, "character varying(50)"),  # varchar(50): typmod = len + 4
        (1042, 14, "character(10)"),  # bpchar(10)
        (1700, 655366, "numeric(10,2)"),  # numeric(10,2): ((p << 16) | s) + 4
        (1114, -1, "timestamp"),  # timestamp without precision
        (1114, 3, "timestamp(3)"),
        (1184, 3, "timestamp(3) with time zone"),  # precision precedes the suffix
        (1083, 6, "time(6)"),
    ],
)
def test_format_type_typmod_rendering(calcite_state, typid, typmod, expected):
    assert _scalar(calcite_state, f"SELECT format_type({typid}, {typmod})") == expected


def test_format_type_on_a_schema_qualified_argument(calcite_state):
    """PGW-051 item 3: the argument must be run through the rewrite before it is
    spliced into the replacement subquery, or DuckDB cannot bind the db prefix."""
    rows = _rows(
        calcite_state,
        "SELECT format_type(pg_catalog.pg_attribute.atttypid, pg_catalog.pg_attribute.atttypmod) "
        "FROM pg_catalog.pg_attribute "
        "JOIN pg_catalog.pg_class ON pg_catalog.pg_class.oid = pg_catalog.pg_attribute.attrelid "
        "WHERE lower(pg_catalog.pg_class.relname) = 'emps' AND pg_catalog.pg_attribute.attnum = 2",
    )
    assert rows == [("character varying",)]


def test_col_description_on_a_schema_qualified_argument(calcite_state):
    """Same argument-transform bug, on the other two rewrite sites."""
    rows = _rows(
        calcite_state,
        "SELECT col_description(pg_catalog.pg_attribute.attrelid, pg_catalog.pg_attribute.attnum) "
        "FROM pg_catalog.pg_attribute WHERE pg_catalog.pg_attribute.attnum = 1 LIMIT 1",
    )
    assert len(rows) == 1


def test_obj_description_on_a_schema_qualified_argument(calcite_state):
    rows = _rows(
        calcite_state,
        "SELECT obj_description(pg_catalog.pg_class.oid) FROM pg_catalog.pg_class "
        "WHERE lower(pg_catalog.pg_class.relname) = 'emps'",
    )
    assert len(rows) == 1


# --------------------------------------------------------------------------- 4
# Real pg_class.reltuples.


def test_reltuples_are_real_row_counts(calcite_state):
    rows = _rows(
        calcite_state,
        "SELECT lower(relname), reltuples FROM pg_catalog.pg_class "
        "WHERE lower(relname) IN ('emps', 'depts') AND relkind = 'r' ORDER BY 1",
    )
    assert rows == [("depts", 4.0), ("emps", 10.0)]


def test_row_count_mode_off_disables_counting(calcite_backend):
    catalog.set_row_count_mode("off")
    try:
        state = launcher.build_state(backend=calcite_backend)
        populate_state(calcite_backend.connection, state)
        rows = _rows(
            state,
            "SELECT reltuples FROM pg_catalog.pg_class WHERE lower(relname) = 'emps'",
        )
        assert rows == [(0.0,)]
    finally:
        catalog.set_row_count_mode("count")
        catalog.invalidate_catalog_cache()


def test_row_count_mode_rejects_an_unknown_mode():
    with pytest.raises(ValueError):
        catalog.set_row_count_mode("estimate")


def test_backend_table_row_count_is_exact(calcite_backend):
    assert calcite_backend.table_row_count("SALES", "emps") == 10


# --------------------------------------------------------------------------- 5
# pg_proc / information_schema.routines / .parameters populated from Calcite.


def test_pg_proc_carries_calcite_operators(calcite_state):
    names = {
        r[0]
        for r in _rows(
            calcite_state,
            "SELECT proname FROM pg_catalog.pg_proc WHERE proname IN "
            "('abs', 'upper', 'lower', 'count', 'sum', 'substring')",
        )
    }
    assert {"abs", "upper", "lower", "count", "sum", "substring"} <= names


def test_pg_proc_rows_live_in_pg_catalog_and_join_pg_type(calcite_state):
    rows = _rows(
        calcite_state,
        "SELECT n.nspname, t.typname, p.prokind FROM pg_catalog.pg_proc p "
        "JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace "
        "JOIN pg_catalog.pg_type t ON t.oid = p.prorettype "
        "WHERE p.proname = 'abs'",
    )
    assert rows == [("pg_catalog", "any", "f")]


def test_aggregates_are_marked_prokind_a(calcite_state):
    assert _scalar(
        calcite_state, "SELECT prokind FROM pg_catalog.pg_proc WHERE proname = 'sum'"
    ) == "a"


def test_information_schema_routines_lists_scalar_functions_only(calcite_state):
    rows = _rows(
        calcite_state,
        "SELECT routine_schema, routine_type FROM information_schema.routines "
        "WHERE routine_name = 'upper'",
    )
    assert rows == [("pg_catalog", "FUNCTION")]
    # PG's information_schema.routines excludes aggregates (prokind 'a').
    assert _rows(
        calcite_state,
        "SELECT routine_name FROM information_schema.routines WHERE routine_name = 'sum'",
    ) == []


def test_information_schema_parameters_are_positional_and_linked(calcite_state):
    rows = _rows(
        calcite_state,
        "SELECT p.ordinal_position, p.parameter_mode, p.udt_name "
        "FROM information_schema.routines r "
        "JOIN information_schema.parameters p ON p.specific_name = r.specific_name "
        "WHERE r.routine_name = 'upper' ORDER BY p.ordinal_position",
    )
    assert rows == [(1, "IN", "any")]


def test_stub_backend_projects_no_functions():
    """A backend that executes no Calcite operators advertises none (no invented rows)."""
    state = _keyed_state(StubBackend())
    assert _rows(state, "SELECT count(*) FROM pg_catalog.pg_proc") == [(0,)]
    catalog.invalidate_catalog_cache()


# --------------------------------------------------------------------------- 6
# pg_available_extension_versions() served from _pg_extension.


class _JsonBackend(StubBackend):
    """Stub that declares extension surfaces, as a configured CalciteBackend does."""

    @property
    def extensions(self) -> frozenset:
        return frozenset({"json", "vector"})


def _keyed_state(backend):
    """State with a key-bearing synthetic catalog (the file adapter declares none)."""
    depts = TableMeta(1, "postgres", "SALES", "depts", "SALES.depts")
    emps = TableMeta(2, "postgres", "SALES", "emps", "SALES.emps")
    ctx = CompilationContext(
        tables={depts.type_name: depts, emps.type_name: emps},
        pk_columns={1: ["deptno"], 2: ["empno"]},
        joins={
            ("SALES.emps", "deptno"): JoinMeta(
                source_column="deptno",
                target_column="deptno",
                target=depts,
                cardinality="many-to-one",
            )
        },
    )
    column_types = {
        1: [ColumnMeta("deptno", "INTEGER", False), ColumnMeta("dname", "VARCHAR")],
        2: [ColumnMeta("empno", "INTEGER", False), ColumnMeta("deptno", "INTEGER")],
    }
    state = launcher.build_state(backend=backend)
    install_catalog(state, ctx, column_types)
    return state


def test_pg_extension_advertises_the_backend_surfaces():
    state = _keyed_state(_JsonBackend())
    assert _rows(
        state, "SELECT extname, extversion FROM pg_catalog.pg_extension ORDER BY extname"
    ) == [("json", "1.0"), ("vector", "0.7")]
    catalog.invalidate_catalog_cache()


def test_pg_available_extension_versions_reports_the_installed_surfaces():
    state = _keyed_state(_JsonBackend())
    assert _rows(
        state, "SELECT name, version FROM pg_available_extension_versions() ORDER BY name"
    ) == [("json", "1.0"), ("vector", "0.7")]
    catalog.invalidate_catalog_cache()


def test_datagrip_retrieve_extensions_join_keeps_its_rows():
    """The LIMIT 0 macro made this LEFT JOIN drop av.version for every extension."""
    state = _keyed_state(_JsonBackend())
    rows = _rows(
        state,
        "select ext.extname, av.version, av.installed from pg_catalog.pg_extension ext "
        "left join pg_available_extension_versions() av "
        "on av.name = ext.extname and av.version = ext.extversion order by ext.extname",
    )
    assert rows == [("json", "1.0", True), ("vector", "0.7", True)]
    catalog.invalidate_catalog_cache()


def test_extension_comment_is_readable_through_obj_description():
    state = _keyed_state(_JsonBackend())
    assert _rows(
        state,
        "SELECT obj_description(oid) FROM pg_catalog.pg_extension WHERE extname = 'json'",
    ) == [("JSON operators -> Calcite JSON_VALUE/JSON_QUERY",)]
    catalog.invalidate_catalog_cache()


def test_no_extensions_declared_means_no_rows():
    state = _keyed_state(StubBackend())
    assert _rows(state, "SELECT count(*) FROM pg_catalog.pg_extension") == [(0,)]
    catalog.invalidate_catalog_cache()


# --------------------------------------------------------------------------- 7
# information_schema.referential_constraints populated from the FK metadata.


def test_referential_constraints_link_each_fk_to_the_referenced_key():
    state = _keyed_state(StubBackend())
    rows = _rows(
        state,
        "SELECT constraint_schema, constraint_name, unique_constraint_schema, "
        "unique_constraint_name, match_option, update_rule, delete_rule "
        "FROM information_schema.referential_constraints",
    )
    assert len(rows) == 1, rows
    (schema, fk_name, uq_schema, uq_name, match, update, delete) = rows[0]
    assert schema == "SALES" and uq_schema == "SALES"
    assert "deptno" in fk_name
    assert (match, update, delete) == ("NONE", "NO ACTION", "NO ACTION")
    # The referenced key must be the target table's modelled primary key.
    pk_rows = _rows(
        state,
        "SELECT constraint_name FROM information_schema.table_constraints "
        "WHERE table_name = 'depts' AND constraint_type = 'PRIMARY KEY'",
    )
    assert pk_rows == [(uq_name,)]
    catalog.invalidate_catalog_cache()


def test_referential_constraints_joins_key_column_usage():
    """DBeaver's FK introspection joins the two views on constraint name + schema."""
    state = _keyed_state(StubBackend())
    rows = _rows(
        state,
        "SELECT rc.constraint_name, kcu.table_name, kcu.column_name "
        "FROM information_schema.referential_constraints rc "
        "JOIN information_schema.key_column_usage kcu "
        "ON kcu.constraint_name = rc.constraint_name "
        "AND kcu.constraint_schema = rc.constraint_schema "
        "ORDER BY kcu.ordinal_position",
    )
    assert [(r[1], r[2]) for r in rows] == [("emps", "deptno")]
    catalog.invalidate_catalog_cache()


def test_keyless_catalog_reports_no_referential_constraints(calcite_state):
    """PGW-013: the file adapter declares no keys, so the view is a well-formed empty."""
    assert _rows(
        calcite_state, "SELECT count(*) FROM information_schema.referential_constraints"
    ) == [(0,)]
