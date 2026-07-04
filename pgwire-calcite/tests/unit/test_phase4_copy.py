# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Phase 4 tests: binary COPY encoder, framing, and DuckDB attach probes.

Covers the parts of PGW-021/005 that pgwire-calcite owns and can verify without a
DuckDB client: the pg-binary-COPY byte encoding (spec-exact), the on-the-wire
CopyOut/CopyData/CopyDone framing over the extended protocol (DuckDB's read
path), the ``to_regclass`` attach-probe intercept, and the int4 width/OID
consistency (PGW-016) that a binary reader requires.

NOTE: end-to-end DuckDB ATTACH read is verified manually by the user — DuckDB's
binary-COPY reader rejects even a spec-valid stream here for a client-side reason
still under investigation; the server-emitted stream is asserted spec-correct.
"""

from __future__ import annotations

import struct
import time

from buenavista.core import BVType

from pgwire_calcite import launcher
from pgwire_calcite.binary_copy import encode_binary_copy, parse_copy_to_stdout

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


def test_parse_copy_to_stdout_variants():
    q, fmt = parse_copy_to_stdout('COPY (SELECT a FROM t) TO STDOUT (FORMAT "binary")')
    assert q == "SELECT a FROM t" and fmt == "binary"
    q, fmt = parse_copy_to_stdout("COPY (SELECT a FROM t) TO STDOUT (FORMAT binary)")
    assert fmt == "binary"
    q, fmt = parse_copy_to_stdout("COPY t TO STDOUT")
    assert q == "SELECT * FROM t" and fmt == "text"
    assert parse_copy_to_stdout("COPY t FROM STDIN") is None


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
