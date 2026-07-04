# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Phase 1 dialect + normalization + transform-layer tests (no JVM).

Covers PGW-016 (canonical normalization), PGW-017 (bidirectional Calcite
dialect), PGW-018 (PG-only constructs converted or rejected, never silently
mistranslated).
"""

from __future__ import annotations

import pytest

from pgwire_calcite import normalize
from pgwire_calcite.dialect import UnsupportedConstruct, transpile_pg_to_calcite


# --- normalize.py -------------------------------------------------------------

def test_unquoted_identifier_folds_upper():
    assert normalize.fold_unquoted("customer") == "CUSTOMER"
    assert normalize.canonical_identifier("customer", was_quoted=False) == "CUSTOMER"


def test_quoted_identifier_preserved():
    assert normalize.canonical_identifier("Customer", was_quoted=True) == "Customer"
    assert normalize.needs_quoting("Customer") is True
    assert normalize.needs_quoting("CUSTOMER") is False


def test_type_mapping_oids_are_stable_and_consistent():
    assert normalize.pg_oid("INTEGER") == 23
    assert normalize.pg_oid("BIGINT") == 20
    assert normalize.pg_oid("VARCHAR(255)") == 1043  # precision stripped
    assert normalize.pg_typname("BOOLEAN") == "bool"
    # Unknown types are reported as text, never dropped.
    assert normalize.pg_typname("SOMEFUTURETYPE") == "text"


def test_duckdb_label_bridges_to_wire_encoder():
    assert normalize.duckdb_label("DECIMAL(10,2)") == "DECIMAL"
    assert normalize.duckdb_label("DOUBLE") == "DOUBLE"


# --- dialect transpile --------------------------------------------------------

def test_basic_select_transpiles():
    out = transpile_pg_to_calcite("SELECT a, b FROM t WHERE a > 1")
    assert "SELECT" in out.upper()
    assert "FROM" in out.upper()


def test_limit_becomes_fetch_first():
    out = transpile_pg_to_calcite("SELECT a FROM t ORDER BY a LIMIT 10")
    assert "FETCH" in out.upper() and "ONLY" in out.upper()


def test_join_and_aggregate_transpile():
    sql = (
        "SELECT t.k, COUNT(*) AS c FROM t JOIN u ON t.k = u.k "
        "WHERE t.v > 0 GROUP BY t.k HAVING COUNT(*) > 2 ORDER BY c DESC LIMIT 5"
    )
    out = transpile_pg_to_calcite(sql)
    up = out.upper()
    assert "JOIN" in up and "GROUP BY" in up and "HAVING" in up


def test_string_agg_converted_to_listagg():
    out = transpile_pg_to_calcite("SELECT string_agg(name, ',') FROM t")
    assert "LISTAGG" in out.upper()
    assert "STRING_AGG" not in out.upper()


# --- PG-only constructs: rejected explicitly (PGW-018) ------------------------

@pytest.mark.parametrize(
    "sql",
    [
        "SELECT DISTINCT ON (a) a, b FROM t",
        "SELECT * FROM t WHERE name ~ '^A'",
        "SELECT * FROM t WHERE name ~* 'abc'",
        "SELECT generate_series(1, 10)",
        "SELECT tags @> ARRAY['x'] FROM t",
        "SELECT data->>'k' FROM t",
    ],
)
def test_pg_only_constructs_rejected_not_mistranslated(sql):
    with pytest.raises(UnsupportedConstruct):
        transpile_pg_to_calcite(sql)
