# Copyright (c) 2026 Kenneth Stott
# Canary: d4e5f6a7-b8c9-0123-def0-345678901234
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL wire protocol server for pgwire-calcite.

Copied verbatim from provisa's pgwire server (buenavista socketserver handler +
TLS + cleartext auth + catalog intercept + multi-statement simple queries) and
rewired so the execution seam targets Calcite instead of Trino:

- ``provisa.executor.trino.QueryResult``      -> ``pgwire_calcite.types.QueryResult``
- ``provisa.pgwire._pipeline`` (Trino route)  -> ``pgwire_calcite.backend`` (seam)
- ``provisa.api.app.state`` (FastAPI)         -> ``pgwire_calcite.state.ServerState``
- ``provisa.auth.providers.simple``           -> ``state``-carried trust/cleartext auth

The wire-protocol logic (handshake, extended protocol, describe/execute/query)
is unchanged from provisa. Catalog intercept and COPY/DDL are copied but not yet
wired to Calcite (Phases 2 and 4); they are gated behind ``state.catalog_enabled``.
"""
# Requirements: PGW-001, PGW-002, PGW-003, PGW-004, PGW-007

from __future__ import annotations

import asyncio
import datetime
import decimal
import logging
import os
import re
import socketserver
import ssl
import struct
import threading
from typing import Iterator, Optional, Tuple

from buenavista.core import BVType, Connection, QueryResult as BVQueryResult, Session
from buenavista.postgres import (
    BVBuffer,
    BVContext,
    BuenaVistaHandler,
    BuenaVistaServer,
    ServerResponse,
)

from pgwire_calcite.types import QueryResult as TrinoResult

log = logging.getLogger(__name__)

_loop: asyncio.AbstractEventLoop | None = None
_loop_lock = threading.Lock()

_TXN_TAG_RE = re.compile(
    r"^\s*(SET|BEGIN|START\s+TRANSACTION|COMMIT|ROLLBACK|DISCARD|RESET|DEALLOCATE|SAVEPOINT|RELEASE)\b",
    re.IGNORECASE,
)

_COPY_RE = re.compile(r"^\s*COPY\b", re.IGNORECASE)
_DDL_RE = re.compile(
    r"^\s*(CREATE\s+(TABLE|VIEW|INDEX|UNIQUE\s+INDEX|SEQUENCE|SCHEMA)"
    r"|ALTER\s+(TABLE|INDEX|SEQUENCE|VIEW)"
    r"|DROP\s+(TABLE|VIEW|INDEX|SEQUENCE|SCHEMA))\b",
    re.IGNORECASE,
)


state = None  # module-level reference; set by the launcher, replaced by tests via patch()


def _pg_literal(v) -> str:
    """Render a Python value as a safe PG literal string."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, (bytes, bytearray)):
        return "E'\\\\x" + v.hex() + "'"
    if isinstance(v, (list, tuple)):
        return "'{" + ",".join(str(x) for x in v) + "}'"
    s = str(v)
    return "'" + s.replace("'", "''") + "'"


def _substitute_params(sql: str, params: list | None) -> str:
    """Replace $1, $2, ... with literal values (highest index first to avoid $1 matching $10)."""
    if not params:
        return sql
    result = sql
    for i in range(len(params), 0, -1):
        result = result.replace(f"${i}", _pg_literal(params[i - 1]))
    return result


def _tag_from_sql(sql: str) -> str:
    m = _TXN_TAG_RE.match(sql)
    if m:
        return m.group(1).upper().split()[0]
    return ""


_DUCKDB_TYPE_TO_BVTYPE: dict[str, BVType] = {
    "INTEGER[]": BVType.INTEGERARRAY,
    "VARCHAR[]": BVType.STRINGARRAY,
    "BOOLEAN": BVType.BOOL,
    "FLOAT": BVType.FLOAT,
    "DOUBLE": BVType.FLOAT,
    "DECIMAL": BVType.DECIMAL,
    "TIMESTAMP": BVType.TIMESTAMP,
    "DATE": BVType.DATE,
    "TIME": BVType.TIME,
    # int4 (OID 23) must map to the 4-byte BVType so the binary encoding width
    # matches the catalog's reported OID — required for binary COPY / DuckDB
    # (PGW-016/021). SMALLINT/TINYINT also fit int4's 4-byte format safely.
    "INTEGER": BVType.INTEGER,
    "SMALLINT": BVType.INTEGER,
    "TINYINT": BVType.INTEGER,
}
# Wider/unsigned integer types map to 8-byte int8.
_DUCKDB_INT_TYPES = {
    "BIGINT",
    "HUGEINT",
    "UBIGINT",
    "UINTEGER",
    "USMALLINT",
    "UTINYINT",
}


def _duckdb_type_to_bvtype(type_str: str) -> BVType:
    if type_str in _DUCKDB_TYPE_TO_BVTYPE:
        return _DUCKDB_TYPE_TO_BVTYPE[type_str]
    if type_str in _DUCKDB_INT_TYPES:
        return BVType.BIGINT
    return BVType.TEXT


def _infer_bvtype(rows: list[tuple], col_idx: int) -> BVType:
    for row in rows:
        v = row[col_idx] if col_idx < len(row) else None
        if v is None:
            continue
        if isinstance(v, bool):
            return BVType.BOOL
        if isinstance(v, int):
            return BVType.BIGINT
        if isinstance(v, float):
            return BVType.FLOAT
        if isinstance(v, decimal.Decimal):
            return BVType.DECIMAL
        if isinstance(v, datetime.datetime):
            return BVType.TIMESTAMP
        if isinstance(v, datetime.date):
            return BVType.DATE
        if isinstance(v, datetime.time):
            return BVType.TIME
        if isinstance(v, list):
            if v and isinstance(v[0], int):
                return BVType.INTEGERARRAY
            if v and isinstance(v[0], str):
                return BVType.STRINGARRAY
            return BVType.JSON
        if isinstance(v, dict):
            return BVType.JSON
        return BVType.TEXT
    return BVType.TEXT


class CalciteQueryResult(BVQueryResult):
    """Adapts a pgwire_calcite QueryResult (backend or catalog) to buenavista's ABC."""

    def __init__(self, result: TrinoResult, original_sql: str = ""):
        super().__init__()
        self._rows = result.rows
        self._cols = result.column_names
        self._status = _tag_from_sql(original_sql)
        if result.column_types:
            self._types = [
                _duckdb_type_to_bvtype(t) if t else _infer_bvtype(self._rows, i)
                for i, t in enumerate(result.column_types)
            ]
        else:
            self._types = [_infer_bvtype(self._rows, i) for i in range(len(self._cols))]

    def has_results(self) -> bool:
        return len(self._cols) > 0

    def column_count(self) -> int:
        return len(self._cols)

    def column(self, index: int) -> Tuple[str, BVType]:
        return (self._cols[index], self._types[index])

    def rows(self) -> Iterator[list]:
        return iter(self._rows)  # type: ignore[return-value]

    def status(self) -> str:
        return self._status or "OK"


class CalciteSession(Session):  # PGW-002, PGW-003, PGW-004
    def __init__(self) -> None:
        super().__init__()
        self.role_id: str | None = None

    def cursor(self):
        return None

    def close(self):
        pass

    def in_transaction(self) -> bool:
        return False

    def load_df_function(self, table: str):
        del table
        return None

    def execute_sql(self, sql: str, params=None) -> CalciteQueryResult:
        stripped = _substitute_params(sql.strip(), params)

        # Session / transaction commands: accepted and acknowledged, no real
        # transaction isolation (PGW-004). Empty result -> command tag from SQL.
        if _TXN_TAG_RE.match(stripped):
            return CalciteQueryResult(TrinoResult(), stripped)

        if self.role_id is None:
            raise RuntimeError("Not authenticated")

        import pgwire_calcite.server as _m

        _state = _m.state
        if _state is None:
            raise RuntimeError("Server state not initialized")

        # Catalog intercept (information_schema / pg_catalog). Copied from provisa
        # but only wired to Calcite metadata in Phase 2 — gated until then.
        if getattr(_state, "catalog_enabled", False):
            from pgwire_calcite.catalog import answer, classify

            if classify(stripped) == "INTERCEPT":
                result = answer(stripped, self.role_id or "", _state)
                log.debug(
                    "[RESULT] cols=%r rows=%r",
                    result.column_names,
                    result.rows[:3] if result.rows else [],
                )
                return CalciteQueryResult(result, stripped)

        # Per-role authorization (PGW-045): reject out-of-grant relations before
        # execution — enforced on the same grants that filter discovery.
        _grants = getattr(_state, "authz_grants", None)
        if _grants is not None:
            from pgwire_calcite.authz import enforce_query

            enforce_query(_grants, self.role_id or "", stripped)

        # Non-catalog execution seam: Phase 0 StubBackend -> Phase 1 CalciteBackend.
        # stream=True selects the Arrow batch-streaming path at the wire (Phase 3);
        # backends that don't stream ignore the flag and materialize.
        try:
            result = _state.backend.execute_sql(stripped, self.role_id, params, stream=True)
        except PermissionError as exc:
            raise PermissionError(str(exc)) from exc
        except Exception as exc:
            log.warning("[PGWIRE] EXCEPTION sql=%r", stripped[:300], exc_info=True)
            raise RuntimeError(str(exc)) from exc

        return CalciteQueryResult(result, stripped)


class CalciteConnection(Connection):
    def new_session(self) -> CalciteSession:
        return CalciteSession()

    def parameters(self) -> dict[str, str]:
        return {
            # server_version >= 14 clears DuckDB's >=12 gate and DBeaver/DataGrip
            # feature gates (PGW-001).
            "server_version": "14.0",
            "server_encoding": "UTF8",
            "client_encoding": "UTF8",
            "DateStyle": "ISO, MDY",
            "TimeZone": "UTC",
            "integer_datetimes": "on",
            "standard_conforming_strings": "on",
            "IntervalStyle": "postgres",
        }


class CalciteHandler(BuenaVistaHandler):  # PGW-002, PGW-007
    """Extends BuenaVistaHandler with TLS, cleartext auth, and catalog intercept."""

    def _send_pg_error(self, severity: str, sqlstate: str, message: str) -> None:
        buf = BVBuffer()
        for field, value in (
            (b"S", severity),
            (b"V", severity),
            (b"C", sqlstate),
            (b"M", message),
        ):
            buf.write_bytes(field)
            buf.write_string(value)
        buf.write_bytes(b"\x00")
        out = buf.get_value()
        self.wfile.write(struct.pack("!ci", ServerResponse.ERROR_RESPONSE, len(out) + 4))
        self.wfile.write(out)
        self.wfile.flush()

    def handle_startup(self, conn: Connection) -> Optional[BVContext]:  # type: ignore[override]
        msglen = self.r.read_uint32() - 4
        code = self.r.read_uint32()
        if code == 80877103:  # SSL request
            ssl_ctx: ssl.SSLContext | None = getattr(self.server, "ssl_ctx", None)
            if ssl_ctx:
                self.wfile.write(b"S")
                self.wfile.flush()
                self.request = ssl_ctx.wrap_socket(self.request, server_side=True)
                self.rfile = self.request.makefile("rb")
                self.wfile = self.request.makefile("wb", 0)
                self.r = BVBuffer(self.rfile)
            else:
                self.wfile.write(b"N")
                self.wfile.flush()
            return self.handle_startup(conn)
        elif code == 80877102:  # Cancel request
            process_id = self.r.read_uint32()
            secret_key = self.r.read_uint32()
            ctx = self.server.ctxts.get(process_id)  # type: ignore[attr-defined]
            if ctx and ctx.secret_key == secret_key:
                self.server.conn.close_session(ctx.session)  # type: ignore[attr-defined]
                del self.server.ctxts[ctx.process_id]  # type: ignore[attr-defined]
            return None
        elif code == 196608:  # Protocol 3.0
            msg = [x.decode("utf-8") for x in self.r.read_bytes(msglen - 4).split(b"\x00")]
            params = dict(zip(msg[::2], msg[1::2]))
            log.info(
                "[PGWIRE] connect params: %s", {k: v for k, v in params.items() if k != "password"}
            )
            ctx = BVContext(conn.create_session(), None, params)
            # Trust mode: authenticate immediately with no password challenge, so a
            # plain `psql host=… user=… dbname=…` connects like any client. A
            # pluggable provider (Phase 5b) decides via requires_password; else the
            # legacy auth_config/'simple' path applies.
            _st = state
            _prov = getattr(_st, "auth_provider", None) if _st else None
            if _prov is not None:
                trust = not _prov.requires_password
            else:
                _provider = (_st.auth_config or {}).get("provider", "none") if _st else "none"
                trust = _provider == "none" or (_st is not None and not _st.auth_middleware_active)
            if trust:
                role = _prov.authenticate(params.get("user", ""), "") if _prov is not None else params.get("user", "")
                ctx.session.role_id = role  # type: ignore[attr-defined]
                self.send_authentication_ok()
                self.handle_post_auth(ctx)
                return ctx
            # SASL SCRAM-SHA-256 wire exchange (PGW-043): no password on the wire.
            if _prov is not None and getattr(_prov, "wire_mechanism", None) == "SCRAM-SHA-256":
                self._scram = None  # per-connection SCRAM exchange state
                self.send_authentication_sasl(["SCRAM-SHA-256"])
                return ctx
            self.send_auth_request(ctx)
            return ctx
        else:
            raise Exception(f"Unsupported startup message code: {code}")

    def send_auth_request(self, ctx: BVContext) -> None:
        del ctx
        self.wfile.write(struct.pack("!cii", ServerResponse.AUTHENTICATION_REQUEST, 8, 3))
        self.wfile.flush()

    # --- SASL SCRAM-SHA-256 (PGW-043) ----------------------------------------

    def _send_auth_msg(self, code: int, data: bytes = b"") -> None:
        body = struct.pack("!i", code) + data
        self.wfile.write(struct.pack("!ci", ServerResponse.AUTHENTICATION_REQUEST, len(body) + 4))
        self.wfile.write(body)
        self.wfile.flush()

    def send_authentication_sasl(self, mechanisms) -> None:
        data = b"".join(m.encode("utf-8") + b"\x00" for m in mechanisms) + b"\x00"
        self._send_auth_msg(10, data)  # AuthenticationSASL

    def _handle_sasl(self, ctx: BVContext, payload: bytes, provider) -> None:
        username = ctx.params.get("user", "")
        if getattr(self, "_scram", None) is None:
            # SASLInitialResponse: mechanism cstring + int32 len + client-first-message
            idx = payload.index(0)
            rest = payload[idx + 1 :]
            (cflen,) = struct.unpack("!i", rest[:4])
            body = rest[4:] if cflen < 0 else rest[4 : 4 + cflen]
            client_first = body.decode("utf-8")
            verifier = provider.get_verifier(username)
            if verifier is None:
                self._send_pg_error(
                    "FATAL", "28P01", f'password authentication failed for user "{username}"'
                )
                return
            from pgwire_calcite.scram import ScramServerExchange

            self._scram = ScramServerExchange(verifier)
            server_first = self._scram.server_first(client_first)
            self._send_auth_msg(11, server_first.encode("utf-8"))  # SASLContinue
            return
        # SASLResponse: client-final-message
        client_final = payload.rstrip(b"\x00").decode("utf-8")
        ok, server_final = self._scram.verify_final(client_final)
        if not ok:
            self._send_pg_error(
                "FATAL", "28P01", f'password authentication failed for user "{username}"'
            )
            return
        self._send_auth_msg(12, (server_final or "").encode("utf-8"))  # SASLFinal
        ctx.session.role_id = username  # type: ignore[attr-defined]
        self.send_authentication_ok()
        self.handle_post_auth(ctx)

    def handle_md5_password(self, ctx: BVContext, payload: bytes) -> None:
        password = payload.decode("utf-8").rstrip("\x00")
        username = ctx.params.get("user", "")

        import pgwire_calcite.server as _m

        _state = _m.state
        if _state is None:
            self._send_pg_error("FATAL", "28P01", "Server state not initialized")
            return

        # SASL SCRAM-SHA-256 exchange (PGW-043): the 'p' message carries SASL data.
        _prov0 = getattr(_state, "auth_provider", None)
        if _prov0 is not None and getattr(_prov0, "wire_mechanism", None) == "SCRAM-SHA-256":
            self._handle_sasl(ctx, payload, _prov0)
            return

        # Pluggable provider path (Phase 5b): the provider verifies the password
        # (e.g. LocalAccountsProvider against SCRAM-SHA-256 verifiers at rest).
        _prov = getattr(_state, "auth_provider", None)
        if _prov is not None:
            role = _prov.authenticate(username, password)
            if role is None:
                self._send_pg_error(
                    "FATAL", "28P01", f'password authentication failed for user "{username}"'
                )
                return
            ctx.session.role_id = role  # type: ignore[attr-defined]
            self.send_authentication_ok()
            self.handle_post_auth(ctx)
            return

        provider = (_state.auth_config or {}).get("provider", "none")

        if provider == "none" or not _state.auth_middleware_active:
            # Trust mode: username maps directly to role_id, password ignored.
            ctx.session.role_id = username  # type: ignore[attr-defined]
            self.send_authentication_ok()
            self.handle_post_auth(ctx)
            return

        if provider != "simple":
            self._send_pg_error(
                "FATAL",
                "28P01",
                f"pgwire auth requires provider 'none' or 'simple'; configured: {provider!r}",
            )
            return

        if not _state.check_password(username, password):
            self._send_pg_error(
                "FATAL", "28P01", f'password authentication failed for user "{username}"'
            )
            return

        ctx.session.role_id = username  # type: ignore[attr-defined]
        self.send_authentication_ok()
        self.handle_post_auth(ctx)

    def handle_describe(self, ctx: BVContext, payload: bytes) -> None:
        ba = bytearray(payload)
        if ba[0] == ord("P"):
            portal = ba[1 : len(ba) - 1].decode("utf-8")
            stmt_name = ctx.portals.get(portal, (None,))[0] if portal in ctx.portals else None
            portal_sql = ctx.stmts.get(stmt_name, ("",))[0] if stmt_name is not None else ""
            if stmt_name is not None and not portal_sql.strip():
                self.send_no_data()
                return
            # COPY TO STDOUT has no RowDescription — its rows arrive as CopyData at
            # Execute. Describe must return NoData, not run the COPY as a query.
            if portal_sql and _COPY_RE.match(portal_sql):
                self.send_no_data()
                return
        elif ba[0] == ord("S"):
            stmt = ba[1 : len(ba) - 1].decode("utf-8")
            sql = ctx.stmts[stmt][0]
            if not sql.strip():
                self.send_paramter_description([])
                self.send_no_data()
                return
            if _COPY_RE.match(sql):
                self.send_paramter_description([])
                self.send_no_data()
                return
            try:
                # describe_statement executes with $N→NULL (0-row result) but gives us column schema
                query_result = ctx.describe_statement(stmt)
            except Exception as e:
                self.send_error(e, ctx)
                return
            indices = {int(m) for m in re.findall(r"\$(\d+)", sql)}
            if "typeinfo_tree" in sql.lower() and indices:
                # OID 1028 = _oid (oid[]) — asyncpg has a built-in binary codec for this,
                # so it can encode list(typeoids) and we can decode the binary response.
                param_oids = [1028]
                ctx.stmts[stmt] = (sql, param_oids)
            elif "set_config" in sql.lower() and indices:
                # set_config takes TEXT params; OID 25 prevents asyncpg from looping on OID 0
                param_oids = [25] * len(indices)
            else:
                # OID 0 = unspecified: tells client how many params without declaring types
                param_oids = [0] * max(indices) if indices else ctx.stmts[stmt][1]
            self.send_paramter_description(param_oids)
            if query_result.has_results():
                self.send_row_description(query_result)
            else:
                self.send_no_data()
            return
        super().handle_describe(ctx, payload)

    def handle_execute(self, ctx: BVContext, payload: bytes) -> None:
        ba = bytearray(payload)
        portal_idx = ba.index(0)
        portal = ba[:portal_idx].decode("utf-8")
        stmt_name = ctx.portals.get(portal, (None,))[0] if portal in ctx.portals else None
        sql = ctx.stmts.get(stmt_name, ("",))[0] if stmt_name is not None else ""
        if stmt_name is not None and not sql.strip():
            self.wfile.write(struct.pack("!ci", ServerResponse.EMPTY_QUERY_RESPONSE, 4))
            return
        # COPY ... TO STDOUT via the extended protocol (DuckDB's read path): the
        # simple-query COPY branch is in handle_query; do the same here so the
        # portal streams a CopyOut/CopyData/CopyDone response, not a RowDescription.
        if sql and _COPY_RE.match(sql):
            from pgwire_calcite.binary_copy import BinaryCopyHandler

            try:
                nrows = BinaryCopyHandler(self).handle(ctx, sql)  # type: ignore[arg-type]
                self.send_command_complete(f"COPY {nrows}\x00")
            except PermissionError as exc:
                self._send_pg_error("ERROR", "42501", str(exc))
                ctx.mark_error()
            except Exception as exc:
                self._send_pg_error("ERROR", "0A000", str(exc))
                ctx.mark_error()
            return
        super().handle_execute(ctx, payload)

    def handle_query(self, ctx: BVContext, payload: bytes) -> None:
        decoded = payload.decode("utf-8").rstrip("\x00")

        stmts = [s.strip() for s in decoded.split(";") if s.strip()]
        if not stmts:
            self.wfile.write(struct.pack("!ci", ServerResponse.EMPTY_QUERY_RESPONSE, 4))
            self.send_ready_for_query(ctx)
            return

        for stmt in stmts:
            if _COPY_RE.match(stmt):
                from pgwire_calcite.binary_copy import BinaryCopyHandler

                try:
                    nrows = BinaryCopyHandler(self).handle(ctx, stmt)  # type: ignore[arg-type]
                    self.send_command_complete(f"COPY {nrows}\x00")
                except PermissionError as exc:
                    self._send_pg_error("ERROR", "42501", str(exc))
                    ctx.mark_error()
                except Exception as exc:
                    self._send_pg_error("ERROR", "0A000", str(exc))
                    ctx.mark_error()
                break
            if _DDL_RE.match(stmt):
                from pgwire_calcite.ddl_handler import DdlHandler

                try:
                    tag = DdlHandler(self).handle(ctx, stmt)
                    # DDL changed the schema — drop memoized catalog DBs so the next
                    # introspection rebuilds against the new metadata.
                    from pgwire_calcite.catalog import invalidate_catalog_cache

                    invalidate_catalog_cache()
                    self.send_command_complete(f"{tag}\x00")
                except PermissionError as exc:
                    self._send_pg_error("ERROR", "42501", str(exc))
                    ctx.mark_error()
                except Exception as exc:
                    self._send_pg_error("ERROR", "0A000", str(exc))
                    ctx.mark_error()
                break
            try:
                from buenavista.core import Extension

                if req := Extension.check_json(stmt):
                    method = req.get("method")
                    extension = self.server.extensions.get(method)  # type: ignore[attr-defined]
                    if not extension:
                        raise Exception("Unknown method: " + str(method))
                    query_result = extension.apply(req.get("params"), ctx.session)
                else:
                    query_result = ctx.execute_sql(stmt)
            except PermissionError as exc:
                self._send_pg_error("ERROR", "42501", str(exc))
                ctx.mark_error()
                break
            except Exception as exc:
                self.send_error(exc, ctx)
                break

            if not query_result:
                raise Exception("No query result for: " + stmt)

            if query_result.has_results():
                self.send_row_description(query_result)
                row_count = self.send_data_rows(query_result)
                self.send_command_complete("SELECT %d\x00" % row_count)
            else:
                status = query_result.status()
                self.send_command_complete(f"{status}\x00")

        self.send_ready_for_query(ctx)


class CalciteServer(BuenaVistaServer):  # PGW-001
    allow_reuse_address = True

    def __init__(
        self,
        server_address: tuple[str, int],
        conn: CalciteConnection,
        ssl_ctx: ssl.SSLContext | None = None,
    ) -> None:
        socketserver.ThreadingTCPServer.__init__(self, server_address, CalciteHandler)  # type: ignore[arg-type]
        self.conn = conn
        self.rewriter = None
        self.extensions: dict = {}
        self.ctxts: dict = {}
        self.auth = None
        self.ssl_ctx = ssl_ctx

    def verify_request(self, request, client_address) -> bool:
        del request, client_address
        return True


def start_pgwire_server(
    host: str,
    port: int,
    ssl_ctx: ssl.SSLContext | None = None,
    loop: asyncio.AbstractEventLoop | None = None,
) -> CalciteServer:
    """Start the pgwire server in a daemon thread. Returns the server instance.

    ``loop`` is retained for a future async backend but is optional; the Phase 0
    stub and Phase 1 embedded-JDBC backends execute synchronously.
    """
    global _loop
    with _loop_lock:
        _loop = loop

    if os.environ.get("PGWIRE_CALCITE_DEBUG_LOG"):
        _debug_log = os.path.expanduser("~/pgwire_calcite_debug.log")
        _fh = logging.FileHandler(_debug_log)
        _fh.setLevel(logging.DEBUG)
        _fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        logging.getLogger("pgwire_calcite").addHandler(_fh)
        logging.getLogger("pgwire_calcite").setLevel(logging.DEBUG)
        logging.getLogger("buenavista").addHandler(_fh)
        logging.getLogger("buenavista").setLevel(logging.DEBUG)

    conn = CalciteConnection()
    server = CalciteServer((host, port), conn, ssl_ctx=ssl_ctx)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    log.info("[PGWIRE] listening on %s:%d (TLS=%s)", host, port, ssl_ctx is not None)
    return server
