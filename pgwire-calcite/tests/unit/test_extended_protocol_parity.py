# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The extended query protocol as Provisa's pgwire answers it (REQ-2255 P2).

- A text-format Bind value is decoded by the parameter's declared type, and an array only for
  an array type (or a placeholder the statement casts to one).
- A value is written into the statement as a typed literal, and a ``$N`` inside a string,
  quoted identifier or comment is left alone.
- A message naming a statement or portal the connection does not hold is an ErrorResponse with
  PostgreSQL's SQLSTATE (26000 / 34000), and the connection carries on; closing one is no error.
- A failure inside a transaction is reported by ReadyForQuery as 'E' until the transaction ends.
- The backend pid is an int4; a re-Parse of the same statement with unspecified parameter types
  keeps the types its Describe resolved; a disconnect closes every result the connection holds.
"""

# Requirements: REQ-2255

from __future__ import annotations

import datetime
import decimal
import struct
import time
import uuid

import pytest

from buenavista.postgres import BuenaVistaHandler, BVContext
from pgwire_calcite import launcher
from pgwire_calcite.server import _pg_literal, _substitute_params

from test_phase0_wire import MiniPgClient, _free_port
from test_phase9_wire_fixes import EchoParamsBackend


# --------------------------------------------------------------------------- literals


def test_a_placeholder_inside_a_string_identifier_or_comment_is_left_alone():
    sql = (
        "SELECT $1, '$1 stays', \"$1col\", $10 -- $1 in a comment\n"
        "FROM t /* $2 */ WHERE x = $2 AND y = $$ $1 $$"
    )
    params = ["a", "b", 3, 4, 5, 6, 7, 8, 9, "ten"]
    assert _substitute_params(sql, params) == (
        "SELECT 'a', '$1 stays', \"$1col\", 'ten' -- $1 in a comment\n"
        "FROM t /* $2 */ WHERE x = 'b' AND y = $$ $1 $$"
    )


@pytest.mark.parametrize(
    "value, literal",
    [
        (decimal.Decimal("1.50"), "1.50"),
        (decimal.Decimal("-7"), "-7"),
        (datetime.date(2021, 3, 4), "DATE '2021-03-04'"),
        (datetime.datetime(2021, 3, 4, 12, 34, 56), "TIMESTAMP '2021-03-04 12:34:56'"),
        (
            datetime.datetime(2021, 3, 4, 12, 34, 56, 789000),
            "TIMESTAMP '2021-03-04 12:34:56.789000'",
        ),
        (
            datetime.datetime(2021, 3, 4, 14, 34, 56, tzinfo=datetime.timezone(
                datetime.timedelta(hours=2))),
            "TIMESTAMP '2021-03-04 12:34:56'",
        ),
        (datetime.time(12, 34, 56), "TIME '12:34:56'"),
        (uuid.UUID("12345678-1234-5678-1234-567812345678"),
         "'12345678-1234-5678-1234-567812345678'"),
        (["a", "b c", None, 'q"t', "o'k"], "'{\"a\",\"b c\",NULL,\"q\\\"t\",\"o''k\"}'"),
        ([[1, 2], [3, None]], "'{{\"1\",\"2\"},{\"3\",NULL}}'"),
        ("it's", "'it''s'"),
        (True, "TRUE"),
        (None, "NULL"),
        (42, "42"),
    ],
)
def test_a_bound_value_is_written_as_a_typed_literal(value, literal):
    assert _pg_literal(value) == literal


# --------------------------------------------------------------------------- Bind decoding


def _bind_payload(stmt: bytes, values: list) -> bytes:
    body = b"\x00" + stmt + b"\x00"
    body += struct.pack("!hh", 1, 0)  # one format code: text
    body += struct.pack("!h", len(values))
    for v in values:
        if v is None:
            body += struct.pack("!i", -1)
        else:
            raw = v.encode()
            body += struct.pack("!i", len(raw)) + raw
    body += struct.pack("!hh", 1, 0)
    return body


class _Ctx:
    def __init__(self, sql: str, oids: list):
        self.stmts = {"s": (sql, oids)}
        self.portals: dict = {}

    def add_portal(self, portal, stmt, params, result_formats):
        self.portals[portal] = (stmt, params, result_formats)


class _Handler:
    def __init__(self):
        self.errors: list = []

    def send_bind_complete(self):
        pass

    def send_error(self, exception, ctx=None):
        self.errors.append(exception)


def _bound(sql: str, oids: list, values: list) -> list:
    ctx, handler = _Ctx(sql, oids), _Handler()
    BuenaVistaHandler.handle_bind(handler, ctx, _bind_payload(b"s", values))  # type: ignore[arg-type]
    assert not handler.errors, handler.errors
    return ctx.portals[""][1]


def test_a_text_bind_value_is_decoded_by_its_declared_type():
    got = _bound(
        "SELECT $1, $2, $3, $4, $5, $6, $7",
        [23, 16, 1700, 1082, 1114, 701, 2950],
        ["42", "t", "1.50", "2021-03-04", "2021-03-04 12:34:56", "2.5",
         "12345678-1234-5678-1234-567812345678"],
    )
    assert got == [
        42, True, decimal.Decimal("1.50"), datetime.date(2021, 3, 4),
        datetime.datetime(2021, 3, 4, 12, 34, 56), 2.5,
        uuid.UUID("12345678-1234-5678-1234-567812345678"),
    ]


def test_an_array_is_read_only_for_an_array_type_or_a_cast_to_one():
    got = _bound(
        "SELECT $1, $2, $3::text[], $4",
        [1009, 0, 0, 25],
        ['{a,"b,c",NULL}', '{"k": 1}', "{x,y}", "{not an array}"],
    )
    assert got == [["a", "b,c", None], '{"k": 1}', ["x", "y"], "{not an array}"]


def test_a_bind_naming_an_unknown_statement_is_an_error_not_a_dropped_connection():
    ctx, handler = _Ctx("SELECT 1", []), _Handler()
    BuenaVistaHandler.handle_bind(handler, ctx, _bind_payload(b"nope", []))  # type: ignore[arg-type]
    assert [getattr(e, "sqlstate", None) for e in handler.errors] == ["26000"]


# --------------------------------------------------------------------------- live server


@pytest.fixture()
def server():
    backend = EchoParamsBackend()
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", backend=backend)
    time.sleep(0.1)
    yield port
    srv.shutdown()


def _drain(c: MiniPgClient) -> list:
    """Every message up to and including ReadyForQuery, as (type, body)."""
    out = []
    while True:
        mtype, body = c._recv_msg()
        out.append((mtype, body))
        if mtype == "Z":
            return out


def _sqlstate(body: bytes) -> str:
    for field in body.split(b"\x00"):
        if field[:1] == b"C":
            return field[1:].decode()
    return ""


def test_unknown_portal_and_statement_answer_with_sqlstates_and_the_connection_lives(server):
    c = MiniPgClient("127.0.0.1", server)
    c._send(b"E", b"ghost\x00" + struct.pack("!i", 0))
    c._send(b"S", b"")
    msgs = _drain(c)
    assert [_sqlstate(b) for t, b in msgs if t == "E"] == ["34000"]

    c._send(b"D", b"Sghost\x00")
    c._send(b"S", b"")
    msgs = _drain(c)
    assert [_sqlstate(b) for t, b in msgs if t == "E"] == ["26000"]

    c._send(b"C", b"Sghost\x00")
    c._send(b"C", b"Pghost\x00")
    c._send(b"S", b"")
    assert [t for t, _ in _drain(c)] == ["3", "3", "Z"]

    assert c.query("SELECT 1")["error"] is None
    c.sock.close()


def test_a_failure_in_a_transaction_is_reported_until_it_ends(server):
    c = MiniPgClient("127.0.0.1", server)
    c._send(b"Q", b"BEGIN\x00")
    assert _drain(c)[-1] == ("Z", b"T")
    c._send(b"E", b"ghost\x00" + struct.pack("!i", 0))
    c._send(b"S", b"")
    assert _drain(c)[-1] == ("Z", b"E")
    c._send(b"S", b"")  # a later cycle in the same transaction is still failed
    assert _drain(c)[-1] == ("Z", b"E")
    c._send(b"Q", b"ROLLBACK\x00")
    assert _drain(c)[-1] == ("Z", b"I")
    c.sock.close()


# --------------------------------------------------------------------------- context


class _Session:
    def __init__(self):
        self.process_id = None

    def in_transaction(self):
        return False


def _context() -> BVContext:
    return BVContext(_Session(), None, {})  # type: ignore[arg-type]


def test_the_backend_pid_is_an_int4():
    assert all(0 < _context().process_id < 2**31 for _ in range(200))


def test_a_reparse_with_unspecified_types_keeps_the_described_types():
    ctx = _context()
    ctx.add_statement("", "SELECT $1", [20])
    ctx.add_statement("", "SELECT $1", [0])
    assert ctx.stmts[""] == ("SELECT $1", [20])
    ctx.add_statement("", "SELECT $1 + 1", [0])
    assert ctx.stmts[""] == ("SELECT $1 + 1", [0])


def test_a_disconnect_closes_every_result_the_connection_holds():
    class _Result:
        closed = False

        def close(self):
            self.closed = True

    ctx = _context()
    held = [_Result(), _Result()]
    ctx.result_cache["a"], ctx.result_cache["b"] = held
    ctx.release_results()
    assert all(r.closed for r in held) and not ctx.result_cache
