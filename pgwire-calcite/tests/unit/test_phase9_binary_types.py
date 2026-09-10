# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One contract for BINARY/VARBINARY columns, end to end (PGW-016/021).

A binary column used to disagree with itself: normalize.py advertised bytea
(OID 17), the backend handed the wire a hex *string*, the type label said
VARCHAR (OID 25 on the wire), and binary COPY refused the string outright. This
suite pins the single contract instead:

  JDBC byte[] / Arrow binary -> Python bytes
                             -> label BLOB -> BVType.BYTES -> OID 17
                             -> RowDescription, Describe, and every COPY format

The fixture column is ``WIDEBIN.BINTYPES.c_bin`` (a model view casting a binary
literal), because the CSV file adapter has no binary column type of its own.
"""

from __future__ import annotations

import struct
import time

import psycopg
import pytest

from buenavista.core import BVType
from pgwire_calcite import launcher, normalize
from pgwire_calcite.binary_copy import advertised_oid, binary_codec, encode_text_copy
from pgwire_calcite.catalog import _trino_to_pg_name, _trino_to_pg_oid
from pgwire_calcite.server import _duckdb_type_to_bvtype, _infer_bvtype

from test_phase0_wire import MiniPgClient, _free_port
from test_phase4_copy import _PGCOPY_SIG, _extended_copy

#: the value the fixture view casts, in every representation the wire uses
_BIN = b"\xde\xad\xbe\xef"
_BIN_QUERY = 'SELECT "id", "c_bin" FROM "WIDEBIN"."bintypes" ORDER BY 1'


# --- pure units: one type table, no JVM, no socket ---------------------------


@pytest.mark.parametrize("sql_type", ["VARBINARY", "BINARY", "varbinary(4)", "VARBINARY NOT NULL"])
def test_binary_sql_types_map_to_bytea_and_the_blob_label(sql_type):
    m = normalize.type_mapping(sql_type)
    assert (m.pg_oid, m.pg_typname, m.duckdb) == (17, "bytea", "BLOB")


def test_nullability_suffix_is_not_part_of_the_type():
    """Calcite's DatabaseMetaData reports "INTEGER NOT NULL"; that is still int4,
    not the unknown-type default of text."""
    assert normalize.type_mapping("INTEGER NOT NULL").pg_oid == 23
    assert normalize.type_mapping("TIMESTAMP(0) NOT NULL").pg_oid == 1114


def test_timestamp_with_local_time_zone_agrees_with_the_wire():
    """The catalog said 1184 while RowDescription said 1114 for the same column;
    the binary layout is identical, so both say 1114 now."""
    m = normalize.type_mapping("TIMESTAMP WITH LOCAL TIME ZONE")
    assert (m.pg_oid, m.duckdb) == (1114, "TIMESTAMP")


def test_blob_label_reaches_the_wire_encoder_as_bytes():
    assert _duckdb_type_to_bvtype("BLOB") is BVType.BYTES
    assert advertised_oid(BVType.BYTES) == 17


def test_inferred_type_of_a_bytes_column_is_bytes_not_text():
    """The opaque-metadata path infers from data; bytes under BVType.TEXT would
    reach str() and be sent as "b'\\xde...'"."""
    assert _infer_bvtype([(None,), (_BIN,)], 0) is BVType.BYTES


def test_catalog_derives_bytea_from_the_same_label():
    assert _trino_to_pg_oid("blob") == 17
    assert _trino_to_pg_name("blob") == "bytea"


def test_every_copy_format_renders_bytes_as_pg_hex_or_raw():
    assert binary_codec(BVType.BYTES)(_BIN) == _BIN  # binary: raw octets
    rendered = b"".join(encode_text_copy(_OneBytesRow()))
    assert rendered == b"\\\\x" + _BIN.hex().encode() + b"\n"  # text: \\x escape


class _OneBytesRow:
    """Minimal buenavista QueryResult carrying a single bytea cell."""

    def column_count(self) -> int:
        return 1

    def column(self, index: int):
        return ("b", BVType.BYTES)

    def rows(self):
        yield (_BIN,)


# --- over the wire: psycopg3 against a live server ---------------------------


@pytest.fixture(scope="module")
def binary_server(widetypes_backend):
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", backend=widetypes_backend)
    time.sleep(0.2)
    yield port
    srv.shutdown()
    srv.server_close()


@pytest.mark.parametrize("binary_format", [False, True])
def test_varbinary_round_trips_as_bytes_in_both_result_formats(binary_server, binary_format):
    with psycopg.connect(
        f"host=127.0.0.1 port={binary_server} user=tester dbname=postgres", autocommit=True
    ) as conn:
        with conn.cursor(binary=binary_format) as cur:
            cur.execute(_BIN_QUERY)
            rows = cur.fetchall()
            # Describe/RowDescription must agree with the value that follows it.
            assert cur.description[1].type_code == 17, cur.description[1]
        assert rows == [(1, _BIN), (2, _BIN), (3, _BIN)]


def test_binary_copy_of_a_bytea_column_decodes_to_the_same_bytes(binary_server):
    client = MiniPgClient("127.0.0.1", binary_server)
    try:
        data = _extended_copy(client, f"COPY ({_BIN_QUERY}) TO STDOUT (FORMAT binary)")
    finally:
        client.close()
    assert data[:11] == _PGCOPY_SIG, data[:11].hex()
    assert _decode_pgcopy(data) == [(1, _BIN), (2, _BIN), (3, _BIN)]


def test_text_copy_of_a_bytea_column_is_pg_hex(binary_server):
    client = MiniPgClient("127.0.0.1", binary_server)
    try:
        data = _extended_copy(client, f"COPY ({_BIN_QUERY}) TO STDOUT (FORMAT text)")
    finally:
        client.close()
    assert data.decode().splitlines() == [f"{i}\t\\\\x{_BIN.hex()}" for i in (1, 2, 3)]


def _decode_pgcopy(data: bytes) -> list[tuple]:
    """Decode a two-column (int4, bytea) PGCOPY binary stream."""
    pos = 19  # signature (11) + flags (4) + header extension length (4)
    out: list[tuple] = []
    while True:
        (nfields,) = struct.unpack("!h", data[pos : pos + 2])
        pos += 2
        if nfields == -1:
            return out
        row = []
        for _ in range(nfields):
            (size,) = struct.unpack("!i", data[pos : pos + 4])
            pos += 4
            body = data[pos : pos + size]
            pos += size
            row.append(body)
        out.append((struct.unpack("!i", row[0])[0], row[1]))
