# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The govdata catalog cache shipped beside model.json serves columns for every table, through
both information_schema.columns and the pg_catalog join pgjdbc's getColumns uses."""

from __future__ import annotations

import os

import pytest

from pgwire_calcite import catalog, catalog_populate

_MODEL = os.path.join(os.path.dirname(__file__), "..", "..", "..", "pgwire-govdata", "model.json")

_EXPECTED = {
    ("econ", "inflation_metrics"): 9,
    ("ref", "countries"): 15,
    ("fec", "candidates"): 18,
    ("census", "poverty_history_national"): 15,
    ("officials", "governor_election_results"): 18,
    ("officials", "presidential_state_winners"): 15,
}


class _State:
    authz_grants = None


@pytest.fixture(scope="module")
def state():
    s = _State()
    catalog_populate.populate_state_cached(None, s, os.path.abspath(_MODEL))
    yield s
    catalog.invalidate_catalog_cache()


def test_information_schema_columns_by_schema_list(state):
    rows = catalog.answer(
        "SELECT table_schema, table_name, column_name FROM information_schema.columns "
        "WHERE table_schema IN ('econ','ref','fec','census','officials')", "", state).rows
    counts = {}
    for schema, table, _ in rows:
        counts[(schema, table)] = counts.get((schema, table), 0) + 1
    for key, n in _EXPECTED.items():
        assert counts.get(key) == n, key


def test_every_cached_table_has_columns(state):
    ctx = state.contexts.get("")
    empty = [t for t, tm in ctx.tables.items() if not state.schema_build_cache["column_types"][tm.table_id]]
    assert empty == []


_GET_COLUMNS = (
    "SELECT * FROM (SELECT n.nspname,c.relname,a.attname,a.atttypid,"
    "a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull,a.atttypmod,a.attlen,"
    "t.typtypmod,row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum) AS attnum,"
    "nullif(a.attidentity, '') as attidentity,nullif(a.attgenerated, '') as attgenerated,"
    "pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,dsc.description,t.typbasetype,t.typtype "
    "FROM pg_catalog.pg_namespace n JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid) "
    "JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid) JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid) "
    "LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum) "
    "LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid) "
    "WHERE c.relkind in ('r','p','v','f','m') and a.attnum > 0 AND NOT a.attisdropped "
    "AND n.nspname LIKE '{schema}' AND c.relname LIKE '{table}') c WHERE true ORDER BY nspname,c.relname,attnum"
)


def test_current_catalog_is_answered_by_the_catalog(state):
    # pgjdbc and Trino resolve the catalog with `select current_catalog` before listing columns.
    assert catalog.classify("select current_catalog") == "INTERCEPT"
    result = catalog.answer("select current_catalog", "", state)
    assert result.rows == [(catalog._DATABASE_NAME,)]


@pytest.mark.parametrize("schema, table", [
    ("econ", "inflation_metrics"), ("weather", "nws_stations"),
    ("fec", "candidates"), ("ref", "countries"),
])
def test_driver_getcolumns_lists_columns_for_a_table_of_each_schema(state, schema, table):
    # The table-name pattern arrives with its underscores escaped, as pgjdbc sends it.
    sql = _GET_COLUMNS.format(schema=schema, table=table.replace("_", "\\_"))
    assert catalog.classify(sql) == "INTERCEPT"
    rows = catalog.answer(sql, "", state).rows
    assert rows and {(r[0], r[1]) for r in rows} == {(schema, table)}
