# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Phase 4 tests: the COPY dispatcher — parsing, codecs, framing, governance.

Covers PGW-021/005/045 at the level pgwire-calcite owns:

- one parser for ``COPY (query|table) TO STDOUT`` across every accepted spelling,
  and a loud error for a format it does not serve
- per-column binary codecs keyed on the *advertised* type OID, with no UTF-8
  fallback for a type that has no binary form (a mis-typed stream decodes
  silently, so it must fail instead)
- text and csv served by the same dispatcher, with the CopyOutResponse
  overall-format byte taken from the request rather than hardcoded
- CopyOut/CopyData/CopyDone framing over the extended protocol (DuckDB's read path)
- COPY governed exactly like the SELECT it wraps (SQLSTATE 42501 on denial)
- the ``to_regclass`` attach-probe intercept and int4 width/OID consistency (PGW-016)

The live DuckDB ATTACH end-to-end read lives in test_phase9_duckdb_attach.py.
"""

from __future__ import annotations

import datetime
import decimal
import struct
import time

import pytest

from buenavista.core import BVType

from pgwire_calcite import launcher
from pgwire_calcite.authz import RoleGrants
from pgwire_calcite.binary_copy import (
    BinaryCopyError,
    advertised_oid,
    binary_codec,
    encode_binary_copy,
    encode_csv_copy,
    encode_text_copy,
    parse_copy_to_stdout,
)

from test_phase0_wire import MiniPgClient, _free_port

_PGCOPY_SIG = b"PGCOPY\n\xff\r\n\x00"


class _FakeResult:
    def __init__(self, cols, rows):
        self._cols = cols  # list[(name, BVType)]
        self._rows = rows

    def column_count(self):
        return len(self._cols)

    def column(self, i):
        return self._cols[i]

    def rows(self):
        return iter(self._rows)


@pytest.mark.parametrize(
    ("sql", "query", "fmt"),
    [
        # DuckDB's own spelling, quoted and bare
        ('COPY (SELECT a FROM t) TO STDOUT (FORMAT "binary")', "SELECT a FROM t", "binary"),
        ("COPY (SELECT a FROM t) TO STDOUT (FORMAT binary)", "SELECT a FROM t", "binary"),
        # with / without WITH, with / without an option list, any case
        ("COPY (SELECT a FROM t) TO STDOUT WITH (FORMAT binary)", "SELECT a FROM t", "binary"),
        ("copy (select a from t) to stdout with (format BINARY)", "select a from t", "binary"),
        ("COPY (SELECT a FROM t) TO STDOUT WITH (FORMAT 'csv')", "SELECT a FROM t", "csv"),
        ("COPY (SELECT a FROM t) TO STDOUT (FORMAT csv, HEADER)", "SELECT a FROM t", "csv"),
        ("COPY (SELECT a FROM t) TO STDOUT (FORMAT text)", "SELECT a FROM t", "text"),
        # PostgreSQL's legacy shorthand
        ("COPY (SELECT a FROM t) TO STDOUT WITH BINARY", "SELECT a FROM t", "binary"),
        ("COPY t TO STDOUT WITH CSV", "SELECT * FROM t", "csv"),
        # no FORMAT option at all -> PostgreSQL's default, text
        ("COPY t TO STDOUT", "SELECT * FROM t", "text"),
        ("COPY t TO STDOUT;", "SELECT * FROM t", "text"),
        # table form: qualified name, explicit column list
        ('COPY "S"."T" TO STDOUT (FORMAT binary)', 'SELECT * FROM "S"."T"', "binary"),
        ("COPY t (a, b) TO STDOUT (FORMAT csv)", "SELECT a, b FROM t", "csv"),
    ],
)
def test_parse_copy_to_stdout_variants(sql, query, fmt):
    assert parse_copy_to_stdout(sql) == (query, fmt)


def test_parse_rejects_non_copy_to_stdout():
    assert parse_copy_to_stdout("COPY t FROM STDIN") is None
    assert parse_copy_to_stdout("COPY t TO '/tmp/x.csv'") is None


def test_parse_rejects_unknown_format_loudly():
    """An unrecognised FORMAT is an error, never a silent downgrade to text."""
    with pytest.raises(BinaryCopyError, match="unsupported COPY format 'parquet'"):
        parse_copy_to_stdout("COPY (SELECT 1) TO STDOUT (FORMAT parquet)")


# --- per-column codecs are explicit and keyed on the advertised OID ------------


@pytest.mark.parametrize(
    ("bvtype", "oid", "value", "expected"),
    [
        (BVType.BOOL, 16, True, b"\x01"),
        (BVType.BYTES, 17, b"\xde\xad", b"\xde\xad"),
        (BVType.BIGINT, 20, -9007199254740991, struct.pack("!q", -9007199254740991)),
        (BVType.INTEGER, 23, -42, struct.pack("!i", -42)),
        (BVType.TEXT, 25, "hi", b"hi"),
        (BVType.FLOAT, 701, 2.25, struct.pack("!d", 2.25)),
        (BVType.DATE, 1082, datetime.date(2021, 3, 4), struct.pack("!i", 7733)),
        # a pre-epoch date is negative -- an unsigned encoder raises OverflowError here
        (BVType.DATE, 1082, datetime.date(1970, 1, 1), struct.pack("!i", -10957)),
        (BVType.TIME, 1083, datetime.time(12, 34, 56), struct.pack("!q", 45296000000)),
        (
            BVType.TIMESTAMP,
            1114,
            datetime.datetime(1970, 1, 1),
            struct.pack("!q", -946684800000000),
        ),
                (
            BVType.DECIMAL,
            1700,
            decimal.Decimal("1.25"),
            # ndigits=2, weight=0, sign=+, dscale=2, base-10000 digits [1, 2500]
            struct.pack("!HhHHHH", 2, 0, 0, 2, 1, 2500),
        ),
    ],
)
def test_binary_codec_matches_the_advertised_oid(bvtype, oid, value, expected):
    assert advertised_oid(bvtype) == oid
    assert binary_codec(bvtype)(value) == expected


def test_timestamp_codec_agrees_across_naive_and_aware_values():
    """The Arrow stream yields tz-aware UTC, the JDBC path naive -- same instant,
    so the same bytes; otherwise a streamed scan and a materialized one disagree."""
    enc = binary_codec(BVType.TIMESTAMP)
    naive = datetime.datetime(2021, 3, 4, 12, 34, 56)
    aware = naive.replace(tzinfo=datetime.timezone.utc)
    assert enc(naive) == enc(aware)


@pytest.mark.parametrize("bvtype", [BVType.NULL, BVType.UNKNOWN, BVType.ARRAY])
def test_binary_codec_is_loud_for_types_with_no_binary_form(bvtype):
    """No UTF-8 fallback under a non-text OID: a wrongly-typed but well-framed
    stream is decoded silently by the client, which is worse than an error."""
    with pytest.raises(BinaryCopyError, match="cannot encode column type"):
        binary_codec(bvtype)


def test_binary_copy_refuses_before_writing_any_bytes():
    result = _FakeResult([("x", BVType.UNKNOWN)], [("v",)])
    with pytest.raises(BinaryCopyError):
        list(encode_binary_copy(result))


def test_bytea_column_carrying_a_string_is_an_error_not_utf8_bytes():
    with pytest.raises(BinaryCopyError, match="non-bytes value"):
        binary_codec(BVType.BYTES)("not bytes")


# --- text / csv share the dispatcher ------------------------------------------


def test_text_format_escapes_and_marks_nulls():
    result = _FakeResult(
        [("a", BVType.TEXT), ("b", BVType.INTEGER)],
        [("tab\there", None), ("back\\slash", 3)],
    )
    assert list(encode_text_copy(result)) == [
        b"tab\\there\t\\N\n",
        b"back\\\\slash\t3\n",
    ]


def test_csv_format_quotes_and_blanks_nulls():
    result = _FakeResult(
        [("a", BVType.TEXT), ("b", BVType.BOOL)],
        [('has, comma', True), (None, False)],
    )
    assert list(encode_csv_copy(result)) == [b'"has, comma",t\n', b",f\n"]


def test_binary_copy_encoding_is_spec_exact():
    result = _FakeResult(
        [("dname", BVType.TEXT), ("deptno", BVType.INTEGER)],
        [("ACCOUNTING", 10), ("SALES", None)],
    )
    chunks = list(encode_binary_copy(result))
    # header + 2 rows + trailer
    assert chunks[0] == _PGCOPY_SIG + struct.pack("!ii", 0, 0)
    # row 1: fieldcount=2, len=10 "ACCOUNTING", int4 len=4 value=10
    assert chunks[1] == (
        struct.pack("!h", 2)
        + struct.pack("!i", 10)
        + b"ACCOUNTING"
        + struct.pack("!i", 4)
        + struct.pack("!i", 10)
    )
    # row 2: "SALES", NULL (deptno) -> length -1
    assert chunks[2] == (
        struct.pack("!h", 2)
        + struct.pack("!i", 5)
        + b"SALES"
        + struct.pack("!i", -1)
    )
    assert chunks[3] == struct.pack("!h", -1)  # trailer


def test_int4_encodes_as_four_bytes():
    """PGW-016: INTEGER must be int4 (4 bytes / OID 23), matching the catalog OID,
    not 8-byte BIGINT — else a binary reader misaligns."""
    result = _FakeResult([("n", BVType.INTEGER)], [(10,)])
    row = list(encode_binary_copy(result))[1]
    # fieldcount(2) + length(4) + 4 value bytes
    assert row == struct.pack("!h", 1) + struct.pack("!i", 4) + struct.pack("!i", 10)


# --- wire-level: server emits a spec-valid PGCOPY stream (extended protocol) ---


def _extended_copy(client: MiniPgClient, copy_sql: str) -> bytes:
    """Drive Parse/Bind/Describe/Execute/Sync for a COPY; return concatenated CopyData."""
    q = copy_sql.encode() + b"\x00"
    client._send(b"P", b"\x00" + q + struct.pack("!h", 0))
    client._send(b"B", b"\x00\x00" + struct.pack("!hhh", 0, 0, 0))
    client._send(b"D", b"P\x00")
    client._send(b"E", b"\x00" + struct.pack("!i", 0))
    client._send(b"S", b"")
    copybytes = b""
    while True:
        mt, pl = client._recv_msg()
        if mt == "d":
            copybytes += pl
        elif mt == "E":
            raise AssertionError("error: " + pl.decode(errors="replace"))
        elif mt == "Z":
            break
    return copybytes


def test_wire_emits_valid_pgcopy_stream(calcite_backend):
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", backend=calcite_backend)
    time.sleep(0.1)
    try:
        c = MiniPgClient("127.0.0.1", port)
        try:
            data = _extended_copy(
                c, 'COPY (SELECT "dname", "deptno" FROM "SALES"."depts") TO STDOUT (FORMAT binary)'
            )
            assert data[:11] == _PGCOPY_SIG, data[:11].hex()
            assert data[11:19] == struct.pack("!ii", 0, 0)  # flags + ext len
            assert data[-2:] == struct.pack("!h", -1)  # trailer
            # first tuple: 2 fields
            assert struct.unpack("!h", data[19:21])[0] == 2
        finally:
            c.close()
    finally:
        srv.shutdown()


def test_duckdb_attach_and_read_binary_copy(calcite_backend):
    """PGW-005/021 end-to-end: DuckDB ATTACH (TYPE postgres) reads Calcite tables
    via binary COPY, including a large result across multiple CopyData flushes."""
    try:
        import duckdb

        con = duckdb.connect()
        con.execute("LOAD postgres")
    except Exception as exc:  # extension not installed in this env
        pytest.skip(f"duckdb postgres extension unavailable: {exc}")

    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", backend=calcite_backend)
    time.sleep(0.2)
    try:
        con.execute(
            f"ATTACH 'host=127.0.0.1 port={port} dbname=calcite user=analyst' "
            "AS pg (TYPE postgres, READ_ONLY)"
        )
        assert con.execute(
            "SELECT DNAME, DEPTNO FROM pg.SALES.DEPTS ORDER BY DEPTNO"
        ).fetchall() == [("ACCOUNTING", 10), ("RESEARCH", 20), ("SALES", 30), ("OPERATIONS", 40)]
        # large read -> multiple 64 KiB binary-COPY CopyData flushes
        assert con.execute(
            "SELECT count(*) FROM (SELECT e1.EMPNO FROM pg.SALES.EMPS e1, pg.SALES.EMPS e2, "
            "pg.SALES.EMPS e3, pg.SALES.EMPS e4) t"
        ).fetchall() == [(10000,)]
    finally:
        con.close()
        srv.shutdown()


def test_to_regclass_attach_probe_intercepted(calcite_backend):
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", backend=calcite_backend)
    time.sleep(0.1)
    try:
        c = MiniPgClient("127.0.0.1", port)
        try:
            r = c.query("SELECT to_regclass('duckdb_secrets')")
            assert r["error"] is None, r["error"]
            assert r["rows"] == [[None]]
        finally:
            c.close()
    finally:
        srv.shutdown()


# --- the CopyOutResponse format byte comes from the request, not a constant ----


def _extended_copy_full(client: MiniPgClient, copy_sql: str):
    """Drive a COPY over the extended protocol; return (copy_out_body, copy_data)."""
    q = copy_sql.encode() + b"\x00"
    client._send(b"P", b"\x00" + q + struct.pack("!h", 0))
    client._send(b"B", b"\x00\x00" + struct.pack("!hhh", 0, 0, 0))
    client._send(b"D", b"P\x00")
    client._send(b"E", b"\x00" + struct.pack("!i", 0))
    client._send(b"S", b"")
    out_body = None
    copybytes = b""
    error = None
    while True:
        mt, pl = client._recv_msg()
        if mt == "H":
            out_body = pl
        elif mt == "d":
            copybytes += pl
        elif mt == "E":
            error = pl.decode(errors="replace")
        elif mt == "Z":
            break
    return out_body, copybytes, error


@pytest.fixture()
def copy_server(calcite_backend):
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", backend=calcite_backend)
    time.sleep(0.1)
    yield port
    srv.shutdown()


@pytest.mark.parametrize(
    ("fmt", "overall"),
    [("binary", 1), ("text", 0), ("csv", 0)],
)
def test_copy_out_response_format_byte_follows_the_request(copy_server, fmt, overall):
    """PGW-021: the overall-format byte (and every per-column code) is 0 for the
    textual formats and 1 for binary -- it was previously hardcoded to 0."""
    c = MiniPgClient("127.0.0.1", copy_server)
    try:
        body, data, error = _extended_copy_full(
            c, f'COPY (SELECT "dname", "deptno" FROM "SALES"."depts") TO STDOUT (FORMAT {fmt})'
        )
        assert error is None, error
        assert body is not None, "server sent a CopyOutResponse"
        assert body[0] == overall
        ncols = struct.unpack("!h", body[1:3])[0]
        assert ncols == 2
        assert list(struct.unpack("!hh", body[3:7])) == [overall, overall]
        assert data, "server sent CopyData"
    finally:
        c.close()


def test_text_and_csv_copy_bodies_are_pg_shaped(copy_server):
    c = MiniPgClient("127.0.0.1", copy_server)
    try:
        _, text_data, err = _extended_copy_full(
            c, 'COPY (SELECT "dname", "deptno" FROM "SALES"."depts" ORDER BY 2) TO STDOUT'
        )
        assert err is None, err
        assert text_data.startswith(b"ACCOUNTING\t10\n")
        _, csv_data, err = _extended_copy_full(
            c,
            'COPY (SELECT "dname", "deptno" FROM "SALES"."depts" ORDER BY 2) '
            "TO STDOUT WITH (FORMAT csv)",
        )
        assert err is None, err
        assert csv_data.startswith(b"ACCOUNTING,10\r\n") or csv_data.startswith(
            b"ACCOUNTING,10\n"
        ), csv_data[:40]
    finally:
        c.close()


# --- COPY is governed exactly like the SELECT it wraps -------------------------


@pytest.fixture()
def authz_copy_server(calcite_backend):
    grants = RoleGrants.from_dict({"analyst": {"SALES.DEPTS"}, "admin": {"*"}})
    port = _free_port()
    srv = launcher.serve(
        host="127.0.0.1", port=port, auth="none", backend=calcite_backend, authz_grants=grants
    )
    time.sleep(0.1)
    yield port
    srv.shutdown()


def test_copy_export_of_an_ungranted_table_is_denied(authz_copy_server):
    """PGW-045: an export is not a way around the grants that gate SELECT.

    The denial must land before any CopyData, as SQLSTATE 42501 -- the same code
    the SELECT path returns.
    """
    c = MiniPgClient("127.0.0.1", authz_copy_server, user="analyst")
    try:
        body, data, error = _extended_copy_full(
            c, 'COPY (SELECT "empno" FROM "SALES"."emps") TO STDOUT (FORMAT binary)'
        )
        assert error is not None and "42501" in error, error
        assert body is None and data == b"", "nothing was streamed before the denial"
    finally:
        c.close()


def test_copy_export_of_a_granted_table_succeeds_for_the_same_role(authz_copy_server):
    c = MiniPgClient("127.0.0.1", authz_copy_server, user="analyst")
    try:
        body, data, error = _extended_copy_full(
            c, 'COPY (SELECT "deptno" FROM "SALES"."depts") TO STDOUT (FORMAT binary)'
        )
        assert error is None, error
        assert body is not None and data[:11] == _PGCOPY_SIG
    finally:
        c.close()


def test_copy_table_form_is_governed_too(authz_copy_server):
    """``COPY emps TO STDOUT`` becomes ``SELECT * FROM emps`` and is checked there."""
    c = MiniPgClient("127.0.0.1", authz_copy_server, user="analyst")
    try:
        _, _, error = _extended_copy_full(c, 'COPY "SALES"."emps" TO STDOUT')
        assert error is not None and "42501" in error, error
    finally:
        c.close()


def test_copy_from_stdin_is_rejected_as_a_read_surface(copy_server):
    c = MiniPgClient("127.0.0.1", copy_server)
    try:
        _, _, error = _extended_copy_full(c, "COPY depts FROM STDIN")
        assert error is not None and "TO STDOUT" in error, error
    finally:
        c.close()
