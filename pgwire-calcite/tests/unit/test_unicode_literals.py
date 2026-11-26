# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A string literal may hold any Unicode character, not only those in Calcite's ISO-8859-1
default."""

from __future__ import annotations

import pytest

TEXT = "Ábrego García — “q” 中"


def _rows(backend, sql):
    return [tuple(row) for row in backend.execute_sql(sql, "").rows]


@pytest.mark.parametrize(
    "literal",
    [f"'{TEXT}'", f"N'{TEXT}'"],
    ids=["plain", "national"],
)
def test_a_non_latin_1_literal_is_returned_unchanged(calcite_backend, literal):
    assert _rows(calcite_backend, f"SELECT {literal}") == [(TEXT,)]


def test_a_non_latin_1_literal_is_evaluated(calcite_backend):
    assert _rows(calcite_backend, "SELECT UPPER('“é中”')") == [("“É中”",)]


def test_a_non_latin_1_literal_compares_with_a_column(calcite_backend):
    r = calcite_backend.execute_sql(
        "SELECT table_name FROM information_schema.tables WHERE table_name = '“refugee” — 中'", ""
    )
    assert list(r.rows) == []
