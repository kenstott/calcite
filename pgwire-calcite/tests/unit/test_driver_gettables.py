# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""pgjdbc's DatabaseMetaData.getTables, which the Trino PostgreSQL connector calls to resolve a
table before it asks for columns, is answered from the shipped govdata catalog: one row per
table, named as the catalog names it, for the tables a describe was reported empty for."""

from __future__ import annotations

import os

import pytest

from pgwire_calcite import catalog, catalog_populate

_MODEL = os.path.join(os.path.dirname(__file__), "..", "..", "..", "pgwire-govdata", "model.json")

_GET_TABLES = (
    'SELECT current_database() AS "TABLE_CAT", n.nspname AS "TABLE_SCHEM", c.relname AS "TABLE_NAME",  CASE n.nspna'
    "me ~ '^pg_' OR n.nspname = 'information_schema'  WHEN true THEN CASE  WHEN n.nspname = 'pg_catalog' OR n.nspna"
    "me = 'information_schema' THEN CASE c.relkind   WHEN 'r' THEN 'SYSTEM TABLE'   WHEN 'v' THEN 'SYSTEM VIEW'   W"
    "HEN 'i' THEN 'SYSTEM INDEX'   ELSE NULL   END  WHEN n.nspname = 'pg_toast' THEN CASE c.relkind   WHEN 'r' THEN"
    " 'SYSTEM TOAST TABLE'   WHEN 'i' THEN 'SYSTEM TOAST INDEX'   ELSE NULL   END  ELSE CASE c.relkind   WHEN 'r' T"
    "HEN 'TEMPORARY TABLE'   WHEN 'p' THEN 'TEMPORARY TABLE'   WHEN 'i' THEN 'TEMPORARY INDEX'   WHEN 'S' THEN 'TEM"
    "PORARY SEQUENCE'   WHEN 'v' THEN 'TEMPORARY VIEW'   ELSE NULL   END  END  WHEN false THEN CASE c.relkind  WHEN"
    " 'r' THEN 'TABLE'  WHEN 'p' THEN 'PARTITIONED TABLE'  WHEN 'i' THEN 'INDEX'  WHEN 'P' then 'PARTITIONED INDEX'"
    "  WHEN 'S' THEN 'SEQUENCE'  WHEN 'v' THEN 'VIEW'  WHEN 'c' THEN 'TYPE'  WHEN 'f' THEN 'FOREIGN TABLE'  WHEN 'm"
    '\' THEN \'MATERIALIZED VIEW\'  ELSE NULL  END  ELSE NULL  END  AS "TABLE_TYPE", d.description AS "REMARKS",  \'\' a'
    's "TYPE_CAT", \'\' as "TYPE_SCHEM", \'\' as "TYPE_NAME", \'\' AS "SELF_REFERENCING_COL_NAME", \'\' AS "REF_GENERATION"'
    '  FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c  LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.o'
    "bjoid AND d.objsubid = 0  and d.classoid = 'pg_class'::regclass)  WHERE c.relnamespace = n.oid  AND n.nspname "
    'LIKE \'{schema}\' AND c.relname LIKE \'{table}\' ORDER BY "TABLE_TYPE","TABLE_SCHEM","TABLE_NAME"'
)


class _State:
    authz_grants = None


@pytest.fixture(scope="module")
def state():
    s = _State()
    catalog_populate.populate_state_cached(None, s, os.path.abspath(_MODEL))
    yield s
    catalog.invalidate_catalog_cache()


@pytest.mark.parametrize("schema, table", [
    ("econ", "inflation_metrics"), ("weather", "nws_stations"),
    ("fec", "candidates"), ("ref", "countries"),
    ("census", "poverty_history_national"), ("officials", "governor_election_results"),
    ("officials", "presidential_state_winners"),
])
def test_driver_gettables_returns_the_table(state, schema, table):
    sql = _GET_TABLES.format(schema=schema, table=table.replace("_", "\\_"))
    assert catalog.classify(sql) == "INTERCEPT"
    rows = catalog.answer(sql, "", state).rows
    assert [(r[1], r[2]) for r in rows] == [(schema, table)]


def test_one_catalog_name_across_current_catalog_current_database_and_table_cat(state):
    catalog.set_database_name("govdata")
    try:
        names = {
            catalog.answer("select current_catalog", "", state).rows[0][0],
            catalog.answer("select current_database()", "", state).rows[0][0],
        }
        sql = _GET_TABLES.format(schema="econ", table="inflation\\_metrics")
        names.add(catalog.answer(sql, "", state).rows[0][0])
        assert names == {"govdata"}
    finally:
        catalog.set_database_name("postgres")
