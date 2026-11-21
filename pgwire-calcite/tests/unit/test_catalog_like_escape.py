# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A catalog LIKE escapes with a backslash unless the query names an ESCAPE, as PostgreSQL
does; DuckDB has no default escape, so the rewrite names it."""

from __future__ import annotations

import duckdb
import pytest

from pgwire_calcite import catalog


def _matches(sql):
    return duckdb.sql(catalog._rewrite_for_duckdb(sql)).fetchall() == [(1,)]


@pytest.mark.parametrize(
    "sql, expected",
    [
        (r"SELECT 1 WHERE 'inflation_metrics' LIKE 'inflation\_metrics'", True),
        (r"SELECT 1 WHERE 'inflationXmetrics' LIKE 'inflation\_metrics'", False),
        (r"SELECT 1 WHERE 'INFLATION_METRICS' ILIKE 'inflation\_metrics'", True),
        (r"SELECT 1 WHERE 'inflationXmetrics' NOT LIKE 'inflation\_metrics'", True),
        (r"SELECT 1 WHERE 'nws_stations' LIKE 'nws%'", True),
        (r"SELECT 1 WHERE 'a\b' LIKE 'a\\b'", True),
        (r"SELECT 1 WHERE 'a_b' LIKE 'a!_b' ESCAPE '!'", True),
        (r"SELECT 1 WHERE 'aXb' LIKE 'a!_b' ESCAPE '!'", False),
    ],
)
def test_like_escape(sql, expected):
    assert _matches(sql) is expected
