# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Catalog functions and casts introspection relies on, as Provisa answers them (REQ-2255 P4).

- A name cast to regclass, regtype or regnamespace is the object's oid where it is compared or
  cast to oid (``attrelid = 'emps'::regclass``, ``'integer'::regtype::oid``); a name naming no
  object is an error naming it.
- max_identifier_length is 63 (SQLAlchemy reads it at connect); current_schemas follows
  search_path; pg_backend_pid is the connection's pid; pg_postmaster_start_time answers;
  pg_get_keywords lists PostgreSQL's keywords.
"""

# Requirements: REQ-2255

from __future__ import annotations

import datetime

import pytest

from pgwire_calcite import catalog, launcher
from pgwire_calcite.catalog_populate import populate_state


@pytest.fixture(scope="module")
def state(calcite_backend):
    s = launcher.build_state(backend=calcite_backend)
    populate_state(calcite_backend.connection, s)
    yield s
    catalog.invalidate_catalog_cache()


def _rows(state, sql):
    result = catalog.answer(sql, "tester", state)
    assert result is not None, f"not intercepted: {sql}"
    return [tuple(r) for r in result.rows]


def _scalar(state, sql):
    rows = _rows(state, sql)
    assert len(rows) == 1 and len(rows[0]) == 1, rows
    return rows[0][0]


def test_a_relation_name_cast_to_regclass_is_its_oid_where_compared(state):
    oid = _scalar(state, "SELECT oid FROM pg_catalog.pg_class WHERE relname = 'emps'")
    assert _scalar(state, "SELECT 'emps'::regclass::oid") == oid
    # The fixture's schema is SALES: an unquoted name folds to lower case, so it is quoted.
    assert _scalar(state, """SELECT '"SALES".emps'::regclass::oid""") == oid
    ncols = _scalar(
        state, "SELECT count(*) FROM pg_catalog.pg_attribute WHERE attrelid = 'emps'::regclass"
    )
    assert ncols > 0


def test_a_name_naming_no_relation_is_an_error_naming_it(state):
    with pytest.raises(Exception, match='relation "nope" does not exist'):
        _rows(state, "SELECT 'nope'::regclass::oid")


@pytest.mark.parametrize(
    "name, oid",
    [("integer", 23), ("int4", 23), ("character varying", 1043), ("boolean", 16),
     ("double precision", 701), ("timestamp without time zone", 1114)],
)
def test_a_type_name_cast_to_regtype_is_its_oid(state, name, oid):
    assert _scalar(state, f"SELECT '{name}'::regtype::oid") == oid


def test_a_schema_name_cast_to_regnamespace_is_its_oid(state):
    oid = _scalar(state, "SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'SALES'")
    assert _scalar(state, """SELECT '"SALES"'::regnamespace::oid""") == oid
    assert _scalar(state, "SELECT 'pg_catalog'::regnamespace::oid") == 11


def test_max_identifier_length_is_63(state):
    assert _scalar(state, "SHOW max_identifier_length") == "63"
    assert _scalar(state, "SELECT current_setting('max_identifier_length')") == "63"
    assert _rows(
        state, "SELECT setting FROM pg_catalog.pg_settings WHERE name = 'max_identifier_length'"
    ) == [("63",)]


def test_current_schemas_follows_search_path(state):
    assert list(_scalar(state, "SELECT current_schemas(true)")) == ["pg_catalog", "public"]
    assert list(_scalar(state, "SELECT current_schemas(false)")) == ["public"]


def test_pg_backend_pid_is_the_connections(state):
    catalog.publish_backend_pid(4242)
    try:
        assert _scalar(state, "SELECT pg_backend_pid()") == 4242
        assert _scalar(state, "SELECT pg_backend_pid() + 0 AS p") == 4242
    finally:
        catalog.publish_backend_pid(0)


def test_pg_postmaster_start_time_answers(state):
    started = _scalar(state, "SELECT pg_postmaster_start_time()")
    assert isinstance(started, datetime.datetime)
    assert _rows(state, "SELECT pg_postmaster_start_time() AS t, 1 AS one")[0][1] == 1


def test_pg_get_keywords_lists_postgresqls_keywords(state):
    assert _rows(
        state, "SELECT word, catcode FROM pg_catalog.pg_get_keywords() WHERE word = 'select'"
    ) == [("select", "R")]
    assert _scalar(state, "SELECT count(*) FROM pg_get_keywords()") == 491
