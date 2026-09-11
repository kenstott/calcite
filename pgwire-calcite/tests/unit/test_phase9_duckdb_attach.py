# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Live DuckDB ``ATTACH ... (TYPE postgres)`` against the Calcite pgwire server.

Ported from provisa's tests/integration/test_duckdb_attach_pgwire.py. A real
DuckDB engine speaks libpq to a real server backed by the file-adapter fixture,
so the whole read path is under test end to end:

  (1) libpq startup + trust auth
  (2) catalog introspection (pg_namespace/pg_class/pg_attribute/pg_type)
  (3) the bulk read: DuckDB wraps every scan as
      ``COPY (SELECT ...) TO STDOUT (FORMAT binary)`` and decodes the stream with
      the OIDs the catalog reported -- so an OID/width mismatch surfaces as a
      wrong value or a decode error here, not silently in production (PGW-016/021)
  (4) filter pushdown: the predicate reaches the server inside the COPY query

The multi-flush (>64 KiB) case is covered by test_phase4_copy.py's attach test,
which has a fixture large enough to cross the flush threshold.
"""

from __future__ import annotations

import datetime
import logging
import time

import pytest

duckdb = pytest.importorskip("duckdb", reason="duckdb required for the ATTACH e2e")

from pgwire_calcite import launcher  # noqa: E402  # import follows the duckdb guard

from test_phase0_wire import _free_port  # noqa: E402  # import follows the duckdb guard


#: The WIDETYPES.csv fixture, as the file adapter types it and DuckDB should read it.
_WIDE_EXPECTED = [
    (
        1,
        True,
        7,
        42,
        9007199254740991,
        1.5,
        2.25,
        "hello",
        datetime.date(2021, 3, 4),
        datetime.time(12, 34, 56),
        datetime.datetime(2021, 3, 4, 12, 34, 56),
    ),
    (
        2,
        False,
        -7,
        -42,
        -9007199254740991,
        -1.5,
        -2.25,
        'wide, "quoted" text',
        datetime.date(1970, 1, 1),
        datetime.time(0, 0),
        datetime.datetime(1970, 1, 1, 0, 0),
    ),
    (3, None, None, None, None, None, None, "", None, None, None),
]


@pytest.fixture
def attached(widetypes_backend):
    """A live server plus a DuckDB connection with it ATTACHed as ``pg``."""
    try:
        con = duckdb.connect()
        con.execute("INSTALL postgres")
        con.execute("LOAD postgres")
    except Exception as exc:  # the extension is not installable in this environment
        pytest.skip(f"duckdb postgres extension unavailable: {exc}")

    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", backend=widetypes_backend)
    time.sleep(0.2)
    try:
        con.execute(
            f"ATTACH 'host=127.0.0.1 port={port} user=tester dbname=postgres' "
            "AS pg (TYPE postgres, READ_ONLY)"
        )
        yield con
    finally:
        con.close()
        srv.shutdown()


def test_attach_lists_fixture_tables(attached):
    """(1)+(2): the handshake completes and DuckDB discovers the fixture tables."""
    names = {
        r[0]
        for r in attached.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_catalog = 'pg' "
            "AND table_schema NOT IN ('pg_catalog', 'information_schema')"
        ).fetchall()
    }
    assert names == {"widetypes", "bintypes"}, sorted(names)


def test_scan_reads_a_binary_column_as_blob(attached):
    """(2)+(3): a VARBINARY column is bytea (OID 17) in the catalog DuckDB reads,
    so DuckDB types it BLOB and the binary COPY body decodes to the same octets."""
    typ = attached.execute(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_catalog = 'pg' AND table_name = 'bintypes' AND column_name = 'c_bin'"
    ).fetchall()
    assert typ == [("BLOB",)], typ
    rows = attached.execute('SELECT * FROM pg."WIDEBIN"."bintypes" ORDER BY 1').fetchall()
    assert rows == [(1, b"\xde\xad\xbe\xef"), (2, b"\xde\xad\xbe\xef"), (3, b"\xde\xad\xbe\xef")]


def test_scan_reads_every_supported_type_through_binary_copy(attached):
    """(3): every fixture type -- and a NULL of each -- survives the binary COPY.

    A width/OID disagreement (int2 OID under a 4-byte body, float4 under an
    8-byte body) misaligns every following field, so an all-columns scan of a
    wide row is the assertion that catches it.
    """
    rows = attached.execute('SELECT * FROM pg."WIDE"."WIDETYPES" ORDER BY 1').fetchall()
    assert rows == _WIDE_EXPECTED


def test_null_row_stays_null_not_empty_string(attached):
    """A NULL of each type must arrive as NULL, not as a zero value or ''."""
    row = attached.execute(
        'SELECT * FROM pg."WIDE"."WIDETYPES" WHERE "ID" = 3'
    ).fetchall()[0]
    assert row[0] == 3
    assert all(v is None for v in row[1:7])
    assert row[7] == ""  # the CSV fixture holds an empty string here, not a NULL
    assert all(v is None for v in row[8:])


def test_filter_pushdown_reaches_the_server(attached, caplog):
    """(4): the predicate travels to the server inside the COPY query text."""
    with caplog.at_level(logging.INFO, logger="pgwire_calcite.binary_copy"):
        rows = attached.execute(
            'SELECT "C_STRING" FROM pg."WIDE"."WIDETYPES" WHERE "C_INT" = 42'
        ).fetchall()
    assert rows == [("hello",)]
    copy_sql = [r.getMessage() for r in caplog.records if "[COPY]" in r.getMessage()]
    assert copy_sql, "the scan went through the COPY dispatcher"
    assert any('"c_int"' in m and "42" in m for m in copy_sql), copy_sql
    # ... and DuckDB projected only the columns it needs, so the scan is not a full read
    assert any("ncols=2" in m for m in copy_sql), copy_sql
