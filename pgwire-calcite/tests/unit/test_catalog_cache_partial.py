# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The disk catalog cache must only ever hold a complete catalog walk.

The cache file is keyed by model.json's hash and never invalidated otherwise, so a
walk that dropped a schema (its getTables threw) or left a table column-less (its
getColumns threw) and was cached anyway gets served as the whole catalog on every
later launch -- seen live: nine govdata schemas missing from Claude Desktop's
metadata until the file was deleted by hand.
"""

from __future__ import annotations

import os

from pgwire_calcite import catalog_populate


class _Meta:
    def __init__(self, labels):
        self._labels = labels

    def getColumnCount(self):
        return len(self._labels)

    def getColumnLabel(self, i):
        return self._labels[i - 1]


class _ResultSet:
    """Minimal JDBC ResultSet over a list of dicts; ``fail_after`` raises from next()."""

    def __init__(self, labels, rows, fail_after=None,
                 message="table broken is not found (case sensitive)"):
        self._labels = labels
        self._rows = rows
        self._i = -1
        self._fail_after = fail_after
        self._message = message

    def getMetaData(self):
        return _Meta(self._labels)

    def next(self):
        self._i += 1
        if self._fail_after is not None and self._i >= self._fail_after:
            raise RuntimeError(self._message)
        return self._i < len(self._rows)

    def getString(self, i):
        return self._rows[self._i].get(self._labels[i - 1])

    def close(self):
        pass


_TABLE_LABELS = ["TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS"]
_COLUMN_LABELS = ["COLUMN_NAME", "TYPE_NAME", "NULLABLE", "REMARKS"]


class _DatabaseMetaData:
    def __init__(self, tables, broken_schemas=(), broken_tables=(), failing_views=None):
        self._tables = tables  # {schema: [table, ...]}
        self._broken_schemas = set(broken_schemas)
        self._broken_tables = set(broken_tables)
        # {schema: view}: listed until its first resolution fails, then no longer listed --
        # DuckDBPendingViews' behaviour for a view whose CREATE fails.
        self._failing_views = dict(failing_views or {})

    def _table_rows(self, schemas):
        return [
            {"TABLE_SCHEM": s, "TABLE_NAME": t, "TABLE_TYPE": "TABLE"}
            for s in schemas
            for t in self._tables[s]
        ]

    def getTables(self, _catalog, schema, _pattern, _types):
        if schema is None:
            fail = 0 if self._broken_schemas or self._failing_views else None
            return _ResultSet(_TABLE_LABELS, self._table_rows(self._tables), fail)
        if schema in self._broken_schemas:
            return _ResultSet(_TABLE_LABELS, [], fail_after=0)
        view = self._failing_views.pop(schema, None)
        if view is not None:
            return _ResultSet(_TABLE_LABELS, [], fail_after=0, message=f"table {view} is not found")
        return _ResultSet(_TABLE_LABELS, self._table_rows([schema]))

    def getSchemas(self, _catalog, _pattern):
        return _ResultSet(["TABLE_SCHEM"], [{"TABLE_SCHEM": s} for s in self._tables])

    def getColumns(self, _catalog, schema, table, _pattern):
        if (schema, table) in self._broken_tables:
            raise RuntimeError("Iceberg metadata unreadable")
        return _ResultSet(
            _COLUMN_LABELS, [{"COLUMN_NAME": "id", "TYPE_NAME": "INTEGER", "NULLABLE": "0"}]
        )

    def getPrimaryKeys(self, _catalog, _schema, _table):
        return _ResultSet(["COLUMN_NAME"], [])

    def getImportedKeys(self, _catalog, _schema, _table):
        return _ResultSet(["PKTABLE_SCHEM"], [])


class _Connection:
    def __init__(self, md):
        self._md = md

    def getMetaData(self):
        return self._md

    def unwrap(self, _cls):
        raise RuntimeError("not a CalciteConnection")


_TABLES = {"econ": ["gdp", "cpi"], "law": ["bills"]}


def _model(tmp_path):
    path = tmp_path / "model.json"
    path.write_text('{"version": "1.0"}')
    return str(path)


def test_complete_walk_is_cached(tmp_path):
    model_path = _model(tmp_path)
    ctx, _ = catalog_populate.build_and_cache_context(
        _Connection(_DatabaseMetaData(_TABLES)), model_path)
    assert set(ctx.tables) == {"econ.gdp", "econ.cpi", "law.bills"}
    assert os.path.isfile(catalog_populate.catalog_cache_path(model_path))


def test_walk_missing_a_schema_is_served_but_not_cached(tmp_path):
    model_path = _model(tmp_path)
    ctx, _ = catalog_populate.build_and_cache_context(
        _Connection(_DatabaseMetaData(_TABLES, broken_schemas=["law"])), model_path)
    assert set(ctx.tables) == {"econ.gdp", "econ.cpi"}
    assert not os.path.exists(catalog_populate.catalog_cache_path(model_path))


def test_walk_missing_a_tables_columns_is_not_cached(tmp_path):
    model_path = _model(tmp_path)
    ctx, column_types = catalog_populate.build_and_cache_context(
        _Connection(_DatabaseMetaData(_TABLES, broken_tables=[("econ", "cpi")])), model_path)
    assert column_types[ctx.tables["econ.cpi"].table_id] == []
    assert not os.path.exists(catalog_populate.catalog_cache_path(model_path))


def test_view_that_fails_to_resolve_once_does_not_drop_its_schema(tmp_path):
    model_path = _model(tmp_path)
    ctx, _ = catalog_populate.build_and_cache_context(
        _Connection(_DatabaseMetaData(_TABLES, failing_views={"law": "scotus_cases"})),
        model_path)
    assert set(ctx.tables) == {"econ.gdp", "econ.cpi", "law.bills"}
    assert os.path.isfile(catalog_populate.catalog_cache_path(model_path))


def test_gaps_are_reported(tmp_path):
    _, _, gaps = catalog_populate.build_context_reporting_gaps(
        _Connection(_DatabaseMetaData(
            _TABLES, broken_schemas=["law"], broken_tables=[("econ", "cpi")])))
    assert gaps == ["schema law", "table econ.cpi"]
