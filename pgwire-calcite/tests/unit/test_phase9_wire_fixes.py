# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Phase 9 wire-protocol correctness: fixes ported from provisa's pgwire server.

Covers:
- NULL bind parameters in the extended protocol (length -1 carries no bytes).
- Statement-aware multi-statement split for the simple-query protocol.
- Describe parameter-OID inference from inline ``$1::text`` casts.
- NoticeResponse (SQLSTATE 01000) and the post-auth hook.
- The full PG-14 startup ParameterStatus set, sourced from ``_KNOWN_SETTINGS``.
- Monotonic ``txid_current()``.
- ``json_build_object`` -> ``json_object`` rewrite.
"""

from __future__ import annotations

import asyncio
import io
import socket
import struct
import threading
import time

import pytest

from buenavista.postgres import BVBuffer, ServerResponse
from pgwire_calcite import catalog, launcher
from pgwire_calcite.normalize import split_sql_statements
from pgwire_calcite.types import QueryResult

from tests.unit.test_phase0_wire import MiniPgClient, _free_port


# --------------------------------------------------------------------------
# A backend that echoes its bind parameters, so a client can prove what landed.
# --------------------------------------------------------------------------


class EchoParamsBackend:
    """Returns one row holding the repr of each bound parameter."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, list]] = []

    def ready(self) -> bool:
        return True

    def execute_sql(
        self, sql: str, role_id=None, params=None, stream: bool = False, session_key=None, timeout_ms=0
    ):
        del session_key, timeout_ms
        params = list(params or [])
        self.calls.append((sql, params))
        return QueryResult(
            rows=[tuple("NULL" if p is None else str(p) for p in params)] if params else [("",)],
            column_names=[f"p{i}" for i in range(1, len(params) + 1)] or ["p0"],
            column_types=["VARCHAR"] * (len(params) or 1),
        )


@pytest.fixture()
def echo_server():
    backend = EchoParamsBackend()
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", backend=backend)
    # EchoParamsBackend exposes no Calcite metadata, so launcher.serve leaves the
    # catalog intercept off. SHOW / txid_current() are answered by the intercept,
    # so turn it on explicitly rather than letting the echo backend swallow them.
    import pgwire_calcite.server as server_mod

    server_mod.state.catalog_enabled = True
    time.sleep(0.1)
    yield "127.0.0.1", port, backend
    srv.shutdown()


# --------------------------------------------------------------------------
# 1. NULL bind parameters
# --------------------------------------------------------------------------


def _bind_payload(portal: bytes, stmt: bytes, values: list[bytes | None]) -> bytes:
    """Build a Bind message body with text-format parameters (None -> length -1)."""
    body = portal + b"\x00" + stmt + b"\x00"
    body += struct.pack("!h", 1) + struct.pack("!h", 0)  # one format code: text
    body += struct.pack("!h", len(values))
    for v in values:
        if v is None:
            body += struct.pack("!i", -1)
        else:
            body += struct.pack("!i", len(v)) + v
    body += struct.pack("!h", 1) + struct.pack("!h", 0)  # one result format: text
    return body


class _FakeHandler:
    """Just enough of BuenaVistaHandler for handle_bind."""

    def __init__(self):
        self.bind_complete = 0

    def send_bind_complete(self):
        self.bind_complete += 1


def test_null_bind_parameter_does_not_desync_the_stream():
    from buenavista.postgres import BuenaVistaHandler

    class _Ctx:
        def __init__(self):
            self.stmts = {"s1": ("SELECT $1, $2, $3", [])}
            self.portals = {}

        def add_portal(self, portal, stmt, params, result_formats):
            self.portals[portal] = (stmt, params, result_formats)

    ctx = _Ctx()
    handler = _FakeHandler()
    payload = _bind_payload(b"", b"s1", [None, b"x", None])
    BuenaVistaHandler.handle_bind(handler, ctx, payload)  # type: ignore[arg-type]

    stmt, params, result_formats = ctx.portals[""]
    assert params == [None, "x", None], params
    # The result-format list is only parseable if the NULL binds consumed zero bytes.
    assert result_formats == [0], result_formats
    assert handler.bind_complete == 1


def test_psycopg3_null_parameter_round_trip(echo_server):
    psycopg = pytest.importorskip("psycopg")
    host, port, backend = echo_server
    with psycopg.connect(host=host, port=port, user="tester", dbname="postgres") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT %s::text, %s::text", [None, "x"])
            cur.fetchall()
    sql, params = backend.calls[-1]
    assert params == [None, "x"], params


def test_asyncpg_null_parameter_round_trip(echo_server):
    asyncpg = pytest.importorskip("asyncpg")
    host, port, backend = echo_server

    async def _run():
        conn = await asyncpg.connect(
            host=host, port=port, user="tester", database="postgres", statement_cache_size=0
        )
        try:
            await conn.fetch("SELECT $1::text, $2::text", None, "x")
        finally:
            await conn.close()

    asyncio.run(_run())
    sql, params = backend.calls[-1]
    assert params == [None, "x"], params


# --------------------------------------------------------------------------
# 2. Statement-aware multi-statement split
# --------------------------------------------------------------------------


def test_split_keeps_semicolon_inside_a_string_literal():
    assert split_sql_statements("SELECT 'a;b'") == ["SELECT 'a;b'"]
    assert split_sql_statements("SELECT 'a;b'; SELECT 2") == ["SELECT 'a;b'", "SELECT 2"]


def test_split_keeps_semicolon_inside_a_dollar_quoted_body():
    sql = "CREATE FUNCTION f() RETURNS int AS $$ SELECT 1; $$ LANGUAGE sql; SELECT 2"
    assert split_sql_statements(sql) == [
        "CREATE FUNCTION f() RETURNS int AS $$ SELECT 1; $$ LANGUAGE sql",
        "SELECT 2",
    ]


def test_split_keeps_semicolon_inside_a_trailing_comment():
    assert split_sql_statements("SELECT 1 -- trailing ; comment") == [
        "SELECT 1 -- trailing ; comment"
    ]


def test_split_drops_blank_trailing_fragment():
    assert split_sql_statements("SELECT 1;") == ["SELECT 1"]
    assert split_sql_statements("   ") == []


def test_simple_query_with_semicolon_literal_is_one_statement(echo_server):
    host, port, backend = echo_server
    c = MiniPgClient(host, port)
    try:
        r = c.query("SELECT 'a;b'")
        assert r["error"] is None, r["error"]
    finally:
        c.close()
    assert backend.calls[-1][0] == "SELECT 'a;b'", backend.calls[-1][0]


# --------------------------------------------------------------------------
# 3. Describe parameter-OID inference
# --------------------------------------------------------------------------


class ExtPgClient(MiniPgClient):
    """Adds Parse/Describe(statement)/Sync so ParameterDescription can be read."""

    def describe_params(self, sql: str, name: str = "st") -> list[int]:
        parse = name.encode() + b"\x00" + sql.encode() + b"\x00" + struct.pack("!h", 0)
        self._send(b"P", parse)
        self._send(b"D", b"S" + name.encode() + b"\x00")
        self._send(b"S", b"")
        oids: list[int] = []
        error = None
        while True:
            mtype, payload = self._recv_msg()
            if mtype == "t":  # ParameterDescription
                (n,) = struct.unpack("!h", payload[:2])
                oids = list(struct.unpack(f"!{n}i", payload[2 : 2 + 4 * n]))
            elif mtype == "E":
                error = payload.decode(errors="replace")
            elif mtype == "Z":
                break
        if error is not None:
            raise AssertionError(error)
        return oids


def test_describe_infers_param_oids_from_inline_casts(echo_server):
    host, port, _ = echo_server
    c = ExtPgClient(host, port)
    try:
        assert c.describe_params("SELECT $1::text, $2::int4, $3::bool") == [25, 23, 16]
    finally:
        c.close()


def test_describe_defaults_uncast_params_to_int8(echo_server):
    host, port, _ = echo_server
    c = ExtPgClient(host, port)
    try:
        # $2 carries no cast -> int8 (20), never OID 0 (unspecified).
        assert c.describe_params("SELECT $1::text, $2") == [25, 20]
    finally:
        c.close()


# --------------------------------------------------------------------------
# 4. NoticeResponse + post-auth hook
# --------------------------------------------------------------------------


def test_send_pg_notice_emits_notice_response_with_sqlstate_01000():
    from pgwire_calcite.server import CalciteHandler

    class _Sink(io.BytesIO):
        def flush(self):  # BytesIO.flush is a no-op; keep the interface explicit
            pass

    sink = _Sink()
    handler = CalciteHandler.__new__(CalciteHandler)
    handler.wfile = sink  # type: ignore[attr-defined]
    CalciteHandler._send_pg_notice(handler, "heads up")

    raw = sink.getvalue()
    assert raw[0:1] == ServerResponse.NOTICE_RESPONSE
    (length,) = struct.unpack("!i", raw[1:5])
    assert length == len(raw) - 1
    fields = raw[5:]
    assert b"S\x00NOTICE\x00" in fields or b"SNOTICE\x00" in fields
    assert b"01000\x00" in fields
    assert b"heads up\x00" in fields
    assert fields.endswith(b"\x00")


def test_handle_post_auth_hook_is_overridable_and_runs_after_auth(echo_server, monkeypatch):
    from pgwire_calcite.server import CalciteHandler

    seen: list[str] = []
    original = CalciteHandler.handle_post_auth

    def _spy(self, ctx):
        original(self, ctx)
        seen.append("post_auth")
        self._send_pg_notice("connected")

    monkeypatch.setattr(CalciteHandler, "handle_post_auth", _spy)
    host, port, _ = echo_server
    c = MiniPgClient(host, port)
    try:
        assert seen == ["post_auth"], seen
    finally:
        c.close()


# --------------------------------------------------------------------------
# 5. PG-14 startup ParameterStatus set, shared with SHOW/current_setting
# --------------------------------------------------------------------------


_PG14_STARTUP_PARAMS = (
    "server_version",
    "server_encoding",
    "client_encoding",
    "application_name",
    "is_superuser",
    "session_authorization",
    "DateStyle",
    "IntervalStyle",
    "TimeZone",
    "integer_datetimes",
    "standard_conforming_strings",
    "default_transaction_read_only",
    "in_hot_standby",
)


def test_startup_sends_the_full_pg14_parameter_status_set(echo_server):
    host, port, _ = echo_server
    c = MiniPgClient(host, port)
    try:
        missing = [p for p in _PG14_STARTUP_PARAMS if p not in c.params]
        assert not missing, f"missing ParameterStatus: {missing}"
    finally:
        c.close()


@pytest.mark.parametrize(
    "setting", ["default_transaction_read_only", "in_hot_standby", "is_superuser"]
)
def test_show_answers_the_startup_settings_from_the_same_table(echo_server, setting):
    host, port, _ = echo_server
    c = MiniPgClient(host, port)
    try:
        r = c.query(f"SHOW {setting}")
        assert r["error"] is None, r["error"]
        assert r["rows"] == [[catalog._KNOWN_SETTINGS[setting]]], r["rows"]
        assert r["rows"][0][0] != "", f"SHOW {setting} must not answer an empty string"
    finally:
        c.close()


def test_startup_parameter_values_come_from_known_settings(echo_server):
    host, port, _ = echo_server
    c = MiniPgClient(host, port)
    try:
        s = catalog._KNOWN_SETTINGS
        assert c.params["default_transaction_read_only"] == s["default_transaction_read_only"]
        assert c.params["in_hot_standby"] == s["in_hot_standby"]
        assert c.params["session_authorization"] == s["session_authorization"]
        assert c.params["DateStyle"] == s["datestyle"]
    finally:
        c.close()


# --------------------------------------------------------------------------
# 6. Monotonic txid_current()
# --------------------------------------------------------------------------


def test_next_txid_is_monotonic():
    a, b, c = catalog.next_txid(), catalog.next_txid(), catalog.next_txid()
    assert a < b < c


def test_txid_current_advances_between_queries(echo_server):
    host, port, _ = echo_server
    c = MiniPgClient(host, port)
    try:
        first = c.query("SELECT txid_current()")
        second = c.query("SELECT txid_current()")
        assert first["error"] is None and second["error"] is None
        assert int(second["rows"][0][0]) > int(first["rows"][0][0])
    finally:
        c.close()


def test_combined_recovery_txid_probe_returns_a_single_bigint():
    sql = (
        "SELECT CASE WHEN pg_is_in_recovery() THEN NULL "
        "ELSE CAST(txid_current() AS bigint) END AS current_txid"
    )
    r1 = catalog._handle_txid(sql)
    r2 = catalog._handle_txid(sql)
    assert r1.column_names == ["current_txid"]
    assert r1.column_types == ["BIGINT"]
    assert len(r1.rows) == 1 and len(r1.rows[0]) == 1
    assert r2.rows[0][0] > r1.rows[0][0]


def test_txid_probe_ignores_unrelated_sql():
    assert catalog._handle_txid("SELECT pg_is_in_recovery()") is None
    assert catalog._handle_txid("SELECT 1") is None


def test_pg_current_xact_id_alias_still_answers():
    r = catalog._handle_scalar("SELECT pg_current_xact_id()", "tester")
    assert r is not None and isinstance(r.rows[0][0], int)


# --------------------------------------------------------------------------
# 7. json_build_object -> json_object
# --------------------------------------------------------------------------


def test_json_build_object_is_rewritten_to_json_object():
    out = catalog._rewrite_for_duckdb("SELECT json_build_object('a', 1, 'b', 2)")
    assert "json_object(" in out.lower()
    assert "json_build_object" not in out.lower()


def test_json_build_object_rewrite_recurses_into_arguments():
    out = catalog._rewrite_for_duckdb(
        "SELECT json_build_object('inner', json_build_object('a', 1))"
    )
    assert "json_build_object" not in out.lower()
    assert out.lower().count("json_object(") == 2
