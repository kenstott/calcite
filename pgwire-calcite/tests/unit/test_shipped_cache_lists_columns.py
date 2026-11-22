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
