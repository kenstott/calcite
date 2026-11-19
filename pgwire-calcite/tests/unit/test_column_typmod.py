# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A column's declared size reaches the client (REQ-2255, ported from Provisa's REQ-1961).

pg_attribute.atttypmod was written as -1 for every column, so DuckDB's ATTACH TYPE postgres
read a DECIMAL(7,2) column as an unsized numeric and typed it DOUBLE. The size now flows from
JDBC COLUMN_SIZE / DECIMAL_DIGITS into atttypmod, format_type() and information_schema.columns.
"""

# Requirements: REQ-2255

from __future__ import annotations

import decimal
import time

import pytest

from pgwire_calcite import catalog, launcher, normalize
from pgwire_calcite.catalog_populate import populate_state

from test_phase0_wire import _free_port


@pytest.mark.parametrize(
    "sql_type, size, digits, typmod",
    [
        ("DECIMAL(7, 2)", "7", "2", (7 << 16 | 2) + 4),
        ("DECIMAL", "10", "0", (10 << 16) + 4),
        ("DECIMAL", "10", None, (10 << 16) + 4),
        ("DECIMAL", None, None, -1),
        ("DECIMAL", "1001", "2", -1),
        ("VARCHAR(10)", "10", None, 14),
        ("CHAR(3)", "3", None, 7),
        ("VARCHAR", "-1", None, -1),
        ("TIMESTAMP(3)", "3", None, 3),
        ("TIMESTAMP(0)", "0", None, 0),
        ("TIME(0)", "0", None, 0),
        ("TIMESTAMP(9)", "9", None, -1),
        ("INTEGER", "-1", None, -1),
        ("DOUBLE", "15", None, -1),
    ],
)
def test_pg_typmod_is_postgresqls_encoding(sql_type, size, digits, typmod):
    assert normalize.pg_typmod(sql_type, size, digits) == typmod


@pytest.fixture(scope="module")
def sized_state(sized_backend):
    state = launcher.build_state(backend=sized_backend)
    populate_state(sized_backend.connection, state)
    yield state
    catalog.invalidate_catalog_cache()


def _rows(state, sql):
    result = catalog.answer(sql, "tester", state)
    assert result is not None, f"not intercepted: {sql}"
    return [tuple(r) for r in result.rows]


_EXPECTED_FORMAT = {
    "id": "integer",
    "amount": "numeric(7,2)",
    "whole": "numeric(10,0)",
    "name": "character varying(10)",
    "code": "character varying(3)",
    "free": "character varying",
    "ts3": "timestamp(3) without time zone",
    "ts": "timestamp(0) without time zone",
    "t0": "time(0) without time zone",
}


def test_pg_attribute_carries_each_columns_size(sized_state):
    rows = _rows(
        sized_state,
        "SELECT a.attname, a.atttypmod, format_type(a.atttypid, a.atttypmod) "
        "FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON a.attrelid = c.oid "
        "WHERE c.relname = 'sized' AND a.attnum > 0 ORDER BY a.attnum",
    )
    typmods = {name: mod for name, mod, _ in rows}
    assert typmods["amount"] == (7 << 16 | 2) + 4
    assert typmods["name"] == 14
    assert typmods["free"] == -1
    assert typmods["ts3"] == 3
    assert {name: fmt for name, _, fmt in rows} == _EXPECTED_FORMAT


def test_information_schema_columns_reports_sizes(sized_state):
    rows = _rows(
        sized_state,
        "SELECT column_name, character_maximum_length, numeric_precision, numeric_scale, "
        "datetime_precision FROM information_schema.columns WHERE table_name = 'sized' "
        "ORDER BY ordinal_position",
    )
    got = {r[0]: r[1:] for r in rows}
    assert got["amount"] == (None, 7, 2, None)
    assert got["name"] == (10, None, None, None)
    assert got["free"] == (None, None, None, None)
    assert got["ts3"] == (None, None, None, 3)
    assert got["id"] == (None, 32, 0, None)


def test_duckdb_attach_reads_a_sized_decimal_as_decimal(sized_backend):
    duckdb = pytest.importorskip("duckdb", reason="duckdb required for the ATTACH e2e")
    try:
        con = duckdb.connect()
        con.execute("INSTALL postgres")
        con.execute("LOAD postgres")
    except Exception as exc:  # the extension is not installable in this environment
        pytest.skip(f"duckdb postgres extension unavailable: {exc}")
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", backend=sized_backend)
    time.sleep(0.2)
    try:
        con.execute(
            f"ATTACH 'host=127.0.0.1 port={port} user=tester dbname=postgres' "
            "AS pg (TYPE postgres, READ_ONLY)"
        )
        types = {r[0]: r[1] for r in con.execute('DESCRIBE pg."SIZED"."sized"').fetchall()}
        assert types["amount"] == "DECIMAL(7,2)", types
        assert types["whole"] == "DECIMAL(10,0)", types
        amount, whole = con.execute('SELECT "amount", "whole" FROM pg."SIZED"."sized"').fetchone()
        assert amount == decimal.Decimal("5000.00")
        assert whole == decimal.Decimal("15")
    finally:
        con.close()
        srv.shutdown()
