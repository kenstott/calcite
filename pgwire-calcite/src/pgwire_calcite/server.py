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
is unchanged from provisa. The catalog intercept is wired to Calcite metadata and
gated behind ``state.catalog_enabled``; ``COPY ... TO STDOUT`` is served from the
same execution seam. INSERT/UPDATE/DELETE are routed to the backend only when the
server was started with --allow-writes (SQLSTATE 25006 otherwise). DDL has no route here
at all — the schema comes from the Calcite model — so a DDL statement is refused with
SQLSTATE 0A000 naming the statement kind.
"""
# Requirements: PGW-001, PGW-002, PGW-003, PGW-004, PGW-007

from __future__ import annotations

import datetime
import decimal
import logging
import os
import re
import socket
import socketserver
import ssl
import struct
import threading
import time
import weakref
from typing import TYPE_CHECKING, Iterator, Optional, Tuple

from buenavista.core import BVType, Connection, QueryResult as BVQueryResult, Session
from buenavista.postgres import (
    BVBuffer,
    BVContext,
    BuenaVistaHandler,
    BuenaVistaServer,
    ServerResponse,
)

from pgwire_calcite.auth import is_personal_access_token
from pgwire_calcite import metering
from pgwire_calcite.throttle import LockedOut, login_throttle, subject_key, throttled_auth
from pgwire_calcite.types import QueryResult as TrinoResult

if TYPE_CHECKING:
    from pgwire_calcite.scram import ScramServerExchange
    from pgwire_calcite.state import ServerState

log = logging.getLogger(__name__)

# Inline-cast -> PG type OID, for inferring Describe parameter types from `$1::text`.
_CAST_OID = {
    "text": 25,
    "varchar": 25,
    "int": 23,
    "int4": 23,
    "int8": 20,
    "bigint": 20,
    "bool": 16,
    "float8": 701,
}

#: PostgreSQL type OID for the SQL type Calcite infers for a parameter. NUMERIC has no
#: binary decoder in the wire codec, so exact decimals travel as float8.
_SQL_TYPE_OID = {
    "CHAR": 25,
    "VARCHAR": 25,
    "BOOLEAN": 16,
    "TINYINT": 21,
    "SMALLINT": 21,
    "INTEGER": 23,
    "BIGINT": 20,
    "REAL": 701,
    "FLOAT": 701,
    "DOUBLE": 701,
    "DECIMAL": 701,
    "DATE": 1082,
    "TIMESTAMP": 1114,
}

_TXN_TAG_RE = re.compile(
    r"^\s*(SET|BEGIN|START\s+TRANSACTION|COMMIT|ROLLBACK|DISCARD|RESET|DEALLOCATE|SAVEPOINT|RELEASE)\b",
    re.IGNORECASE,
)

_COPY_RE = re.compile(r"^\s*COPY\b", re.IGNORECASE)
# DDL is rejected, never routed: every schema pgwire-calcite serves comes from the
# Calcite model, which no client may change. A DDL statement is therefore answered with
# SQLSTATE 0A000 naming the statement kind instead of being half-executed.
_DDL_RE = re.compile(
    r"^\s*(?P<verb>CREATE|ALTER|DROP)\s+(?:OR\s+REPLACE\s+)?"
    r"(?:TEMP\s+|TEMPORARY\s+)?"
    r"(?P<object>TABLE|MATERIALIZED\s+VIEW|VIEW|UNIQUE\s+INDEX|INDEX|SEQUENCE|SCHEMA)\b",
    re.IGNORECASE,
)


def _current_backend():
    """The installed execution backend, or None before the launcher installs state.

    Sessions constructed directly (tests) run without state and have never handed a
    statement to a backend, so there is nothing to cancel or discard for them.
    """
    return state.backend if state is not None else None


# DML is routed to the backend only when the server was started with --allow-writes.
_DML_RE = re.compile(r"^\s*(INSERT|UPDATE|DELETE)\b", re.IGNORECASE)
_RETURNING_RE = re.compile(r"\bRETURNING\b", re.IGNORECASE)
_TXN_BEGIN_RE = re.compile(r"^\s*(BEGIN|START\s+TRANSACTION)\b", re.IGNORECASE)
_TXN_END_RE = re.compile(r"^\s*(COMMIT|END|ROLLBACK)\b", re.IGNORECASE)
_TXN_ROLLBACK_RE = re.compile(r"^\s*ROLLBACK\b", re.IGNORECASE)


def _dml_statement_kind(sql: str) -> Optional[str]:
    """``"INSERT"``, ``"UPDATE"`` or ``"DELETE"`` for a DML statement; None otherwise."""
    m = _DML_RE.match(sql)
    return m.group(1).upper() if m is not None else None


def _dml_command_tag(kind: str, count: int) -> str:
    """PostgreSQL's CommandComplete tag for a DML statement (INSERT carries an OID of 0)."""
    return f"INSERT 0 {count}" if kind == "INSERT" else f"{kind} {count}"


def _ddl_statement_kind(sql: str) -> Optional[str]:
    """``"CREATE TABLE"`` and friends for a DDL statement; None when it is not DDL."""
    m = _DDL_RE.match(sql)
    if m is None:
        return None
    return f"{m.group('verb').upper()} {' '.join(m.group('object').upper().split())}"


state: Optional["ServerState"] = None  # module-level reference; set by the launcher, replaced by tests via patch()

# Session-command grammar (PGW-051/052). SET/RESET/DISCARD/DEALLOCATE are answered
# by the session itself, not the engine; the catch-all _TXN_TAG_RE above routes
# them here.
_SET_STMT_RE = re.compile(
    r"^\s*SET\s+(?:SESSION\s+|LOCAL\s+)?([A-Za-z_][A-Za-z0-9_.]*)\s*(?:=|\bTO\b)\s*(.+?)\s*;?\s*$",
    re.IGNORECASE,
)
_RESET_STMT_RE = re.compile(r"^\s*RESET\s+(ALL|[A-Za-z_][A-Za-z0-9_.]*)\s*;?\s*$", re.IGNORECASE)
_DISCARD_STMT_RE = re.compile(
    r"^\s*DISCARD\s+(ALL|PLANS|SEQUENCES|TEMP|TEMPORARY)\s*;?\s*$", re.IGNORECASE
)
_DEALLOCATE_STMT_RE = re.compile(
    r"^\s*DEALLOCATE\s+(?:PREPARE\s+)?(ALL|[^\s;]+)\s*;?\s*$", re.IGNORECASE
)

#: PG's `statement_timeout` units. A bare number means milliseconds.
_TIMEOUT_UNIT_MS = {"us": 0.001, "ms": 1, "s": 1000, "min": 60000, "h": 3600000, "d": 86400000}
_TIMEOUT_VALUE_RE = re.compile(r"^(\d+)\s*([a-z]*)$", re.IGNORECASE)


def _parse_statement_timeout(value: str) -> int:
    """Parse a `SET statement_timeout` value into milliseconds (PG semantics)."""
    from pgwire_calcite.backend import InvalidParameterValue

    raw = value.strip().strip("'\"")
    m = _TIMEOUT_VALUE_RE.match(raw)
    if m is None:
        raise InvalidParameterValue(f'invalid value for parameter "statement_timeout": "{value}"')
    unit = (m.group(2) or "ms").lower()
    if unit not in _TIMEOUT_UNIT_MS:
        raise InvalidParameterValue(f'invalid value for parameter "statement_timeout": "{value}"')
    return int(int(m.group(1)) * _TIMEOUT_UNIT_MS[unit])


def _format_statement_timeout(ms: int) -> str:
    """Render milliseconds the way PG's SHOW statement_timeout does."""
    if ms <= 0:
        return "0"
    if ms % 60000 == 0:
        return f"{ms // 60000}min"
    if ms % 1000 == 0:
        return f"{ms // 1000}s"
    return f"{ms}ms"


def _authenticate_credential(provider, username: str, password: str) -> Optional[str]:
    """Validate one credential against *provider*, deciding once whether it presents as a
    bearer secret (a PAT-shaped string, or any provider whose password field always carries a
    token, e.g. OIDC) or a basic password — and never retrying the other interpretation
    (Phase 3 hardening; mirrors provisa's pgwire ``_validate_credential``/REQ-1263 rule).

    pgwire carries no scheme field, so a bearer-shaped secret presented to a provider that
    does not accept bearer credentials is refused outright rather than run through that
    provider's password check.
    """
    bearer = is_personal_access_token(password) or getattr(provider, "accepts_bearer", False)
    if bearer and not getattr(provider, "accepts_bearer", False):
        return None
    return provider.authenticate(username, password)


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
    # bytea (OID 17): the label normalize.py gives BINARY/VARBINARY columns. The
    # values reaching here are Python bytes (calcite_backend._coerce / the Arrow
    # binary consumer), which BVType.BYTES sends as raw bytes in binary format and
    # as PG hex (\x...) in text format.
    "BLOB": BVType.BYTES,
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
        if isinstance(v, (bytes, bytearray, memoryview)):
            return BVType.BYTES
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
    """Adapts a pgwire_calcite QueryResult (backend or catalog) to buenavista's ABC.

    Rows are pulled lazily (PGW-020): a streaming result's batches are drained only
    as buenavista emits DataRow messages, so a large result never fully materializes
    and CommandComplete's count is what buenavista counted on the way out. The wire
    protocol needs column types up front (RowDescription precedes DataRow); those
    come from JDBC ResultSetMetaData. Only when metadata is insufficient (Calcite
    reports ANY/OTHER, surfaced as an empty label by ``normalize.stream_type_label``)
    is exactly ONE batch buffered to infer from data — a bounded peek, never the
    whole result.

    The batch iterator owns the JDBC statement, the Arrow allocator and the backend
    lock. :meth:`close` releases them, and is called on normal completion, on an
    early stop, and by the handler when the client disconnects mid-stream (PGW-022).
    """

    def __init__(self, result: TrinoResult, original_sql: str = "", status: str | None = None):
        super().__init__()
        self._cols = result.column_names
        self._status = status if status is not None else _tag_from_sql(original_sql)
        #: Command-tag prefix for a result that carries rows but is not a SELECT (a DML
        #: statement with RETURNING): "INSERT 0", "UPDATE" or "DELETE". None = "SELECT".
        self.row_tag_prefix: str | None = None
        # Materialized results (catalog intercept, session commands, non-streaming
        # backends) arrive as a single batch; streaming backends hand over a lazy
        # batch iterator instead. Both are consumed the same way below.
        self._batch_iter: Iterator[list] = (
            result.row_batches if result.row_batches is not None else iter([list(result.rows)])
        )
        self._head: list | None = None
        self._closed = False
        # Usage metering (kenstott/calcite#364): sampled as rows are actually
        # streamed to the client, reported once on close(). See metering.py's
        # module doc for why this lives here rather than on the Java side.
        self._meter_sql = original_sql
        self._meter_start = time.monotonic()
        self._sampler = metering.EgressSampler(len(self._cols))
        ctypes = result.column_types
        if not ctypes or any(not t for t in ctypes):
            self._head = next(self._batch_iter, [])
        # The peeked batch (if any) becomes the first pending batch, so the peek costs
        # nothing at the wire: no row is read twice and none is dropped.
        self._pending: list = list(self._head or [])
        self._pending_idx = 0
        if ctypes:
            self._types = [
                _duckdb_type_to_bvtype(t) if t else _infer_bvtype(self._head or [], i)
                for i, t in enumerate(ctypes)
            ]
        else:
            self._types = [_infer_bvtype(self._head or [], i) for i in range(len(self._cols))]

    def has_results(self) -> bool:
        return len(self._cols) > 0

    def column_count(self) -> int:
        return len(self._cols)

    def column(self, index: int) -> Tuple[str, BVType]:
        return (self._cols[index], self._types[index])

    def rows(self) -> Iterator[list]:
        """Yield rows one batch at a time, resumably.

        Position lives on the result, not on this generator, so an Execute row limit
        (portal suspension) can abandon the generator and a later Execute on the same
        portal picks up at the next unsent row instead of replaying from the start.
        Exactly one batch is resident at a time. Exhausting the source releases it
        here; an abandoned stream is released by the session (see CalciteSession).
        """
        while True:
            if self._pending_idx < len(self._pending):
                row = self._pending[self._pending_idx]
                self._pending_idx += 1
                self._sampler.observe(row)
                yield row
                continue
            if self._closed:
                return
            batch = next(self._batch_iter, None)
            if batch is None:  # source exhausted -> its own finally has released
                self.close()
                return
            self._pending, self._pending_idx = batch, 0

    def close(self) -> None:
        """Release the underlying statement, Arrow allocator and backend lock. Idempotent."""
        if self._closed:
            return
        self._closed = True
        duration_ms = int((time.monotonic() - self._meter_start) * 1000)
        self._sampler.finish(self._meter_sql, duration_ms)
        self._head = None
        self._pending, self._pending_idx = [], 0
        close = getattr(self._batch_iter, "close", None)
        if close is not None:
            close()

    def status(self) -> str:
        return self._status or "OK"


def _peer_closed(sock) -> bool:
    """True once the client has closed its end of ``sock``.

    Peeks through the plain socket layer, so it neither consumes bytes nor needs
    the TLS layer (which cannot peek): a pending pipelined message reads as alive,
    an orderly FIN reads as EOF, and a reset raises.
    """
    try:
        return socket.socket.recv(sock, 1, socket.MSG_PEEK | socket.MSG_DONTWAIT) == b""
    except BlockingIOError:
        return False
    except OSError:
        return True


class ClientLink:
    """A statement's view of the client connection it is running for.

    Calling it answers "has the client gone?" (what the backends poll while a statement
    waits for the engine). ``terminate`` ends the connection from the server's side, for
    a client that keeps the engine without reading from it.
    """

    def __init__(self, sock) -> None:
        self._sock = sock

    def __call__(self) -> bool:
        return _peer_closed(self._sock)

    def terminate(self, reason: str) -> None:
        """Shut the connection down so the thread serving it (blocked reading the
        client's next message, or writing rows it does not read) returns and releases
        what it holds. No ErrorResponse is written: that thread may be mid-message, and
        a client that is not reading would not see it."""
        log.warning("[PGWIRE] closing a client connection: %s", reason)
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            # Already disconnected: the serving thread is on its way out by itself.
            pass


#: SQLSTATE for an error that names none: PostgreSQL's internal_error.
_SQLSTATE_INTERNAL_ERROR = "XX000"


def _sqlstate_of(exception) -> str:
    """The SQLSTATE an exception reaches the client with."""
    sqlstate = getattr(exception, "sqlstate", None)
    if sqlstate:
        return sqlstate
    if isinstance(exception, PermissionError):
        return "42501"
    return _SQLSTATE_INTERNAL_ERROR


class CalciteSession(Session):  # PGW-002, PGW-003, PGW-004
    def __init__(self) -> None:
        super().__init__()
        self.role_id: str | None = None
        # The streaming result of the statement currently in flight. A wire session
        # runs one statement at a time, so starting the next one (or ending the
        # session — including on a client disconnect mid-stream) releases the JDBC
        # statement, Arrow allocator and backend lock the previous result held
        # (PGW-022). Portal suspension does not come through here: buenavista
        # replays the cached result without re-executing.
        self._open_result: CalciteQueryResult | None = None
        #: Weak reference to the connection's BVContext, bound at startup so
        #: DISCARD ALL can drop this session's prepared statements and portals
        #: (PGW-052). Weak on purpose: the context already owns the session, and a
        #: strong back-reference would make the pair uncollectable by refcount —
        #: which keeps a suspended portal's Arrow generator (and with it the
        #: backend's connection lock) alive until the cycle collector happens to
        #: run.
        self._ctx_ref = None
        #: Reports whether the requesting client has disconnected; bound by the wire
        #: handler, absent for sessions created outside a socket (tests, tools).
        self.client_gone = None
        #: GUC name -> value as SHOW reports it, for the settings this session SET.
        self.settings: dict[str, str] = {}
        #: Inside BEGIN ... COMMIT/ROLLBACK, and whether a write ran there. The adapters
        #: commit each write as it runs, so a ROLLBACK after one cannot be honoured.
        self._in_transaction = False
        self._wrote_in_transaction = False
        #: SQL text -> resolved parameter type OIDs (see CalciteHandler._resolve_param_oids).
        self.param_oid_cache: dict[str, list] = {}
        self.statement_timeout_ms: int = self._default_statement_timeout_ms()
        self.settings["statement_timeout"] = _format_statement_timeout(self.statement_timeout_ms)

    @property
    def key(self) -> str:
        """Registry key for this session's in-flight statement."""
        return str(self.id)

    @staticmethod
    def _default_statement_timeout_ms() -> int:
        # No fallback: the field is declared on ServerState with a documented
        # default, so a state that exists always carries one. Sessions created
        # before the launcher installs state (direct construction in tests) get 0.
        return int(state.statement_timeout_ms) if state is not None else 0

    def cursor(self):
        return None

    def close(self):
        self._release_open_result()
        backend = _current_backend()
        if backend is not None:
            backend.discard_session(self.key)

    def _release_open_result(self) -> None:
        result, self._open_result = self._open_result, None
        if result is not None:
            result.close()

    def _track(self, result: "CalciteQueryResult") -> "CalciteQueryResult":
        self._open_result = result
        return result

    @property
    def ctx(self):
        """The connection's BVContext while it is alive, else None."""
        return self._ctx_ref() if self._ctx_ref is not None else None

    def _lane(self) -> str:
        """``probe`` for a client that identified itself as the health probe."""
        from pgwire_calcite.backend import LANE_PROBE, LANE_USER, PROBE_APPLICATION_NAME

        ctx = self.ctx
        if ctx is not None and ctx.params.get("application_name") == PROBE_APPLICATION_NAME:
            return LANE_PROBE
        return LANE_USER

    def bind_context(self, ctx) -> None:
        self._ctx_ref = weakref.ref(ctx)

    # --- session commands (SET / RESET / DISCARD / DEALLOCATE), PGW-051/052 ---

    def _reset_settings(self) -> None:
        self.settings = {}
        self.statement_timeout_ms = self._default_statement_timeout_ms()
        self.settings["statement_timeout"] = _format_statement_timeout(self.statement_timeout_ms)

    def _forget_prepared(self) -> None:
        """Close every prepared statement and portal this connection holds."""
        ctx = self.ctx
        if ctx is None:
            return
        ctx.stmts.clear()
        ctx.portals.clear()
        ctx.result_cache.clear()

    def apply_session_command(self, sql: str) -> None:
        """Apply SET/RESET/DISCARD/DEALLOCATE to this session's own state.

        Transaction verbs (BEGIN/COMMIT/...) reach here too and are acknowledged
        without state, matching the existing no-isolation behaviour (PGW-004).
        """
        m = _SET_STMT_RE.match(sql)
        if m is not None:
            name, value = m.group(1).lower(), m.group(2)
            if name == "statement_timeout":
                self.statement_timeout_ms = _parse_statement_timeout(value)
                self.settings[name] = _format_statement_timeout(self.statement_timeout_ms)
            else:
                self.settings[name] = value.strip().strip("'\"")
            return

        m = _RESET_STMT_RE.match(sql)
        if m is not None:
            if m.group(1).upper() == "ALL":
                self._reset_settings()
            else:
                name = m.group(1).lower()
                self.settings.pop(name, None)
                if name == "statement_timeout":
                    self.statement_timeout_ms = self._default_statement_timeout_ms()
                    self.settings[name] = _format_statement_timeout(self.statement_timeout_ms)
            return

        m = _DISCARD_STMT_RE.match(sql)
        if m is not None:
            if m.group(1).upper() == "ALL":
                self._reset_settings()
                self._forget_prepared()
                backend = _current_backend()
                if backend is not None:
                    backend.discard_session(self.key)
            elif m.group(1).upper() == "PLANS":
                self._forget_prepared()
            return

        m = _DEALLOCATE_STMT_RE.match(sql)
        if m is not None:
            if m.group(1).upper() == "ALL":
                self._forget_prepared()
            else:
                ctx = self.ctx
                if ctx is not None:
                    ctx.stmts.pop(m.group(1), None)
            return

    def in_transaction(self) -> bool:
        return False

    def load_df_function(self, table: str):
        del table
        return None

    def execute_sql(self, sql: str, params=None) -> CalciteQueryResult:
        stripped = _substitute_params(sql.strip(), params)

        # Make this session's SET values the ones SHOW / current_setting resolve
        # against for the rest of this statement (PGW-051).
        from pgwire_calcite import catalog as _catalog_settings

        _catalog_settings.publish_session_settings(self.settings)

        # Session / transaction commands: accepted and acknowledged, no real
        # transaction isolation (PGW-004). Empty result -> command tag from SQL.
        self._release_open_result()

        if _TXN_TAG_RE.match(stripped):
            self._track_transaction(stripped)
            self.apply_session_command(stripped)
            return CalciteQueryResult(TrinoResult(), stripped)

        ddl_kind = _ddl_statement_kind(stripped)
        if ddl_kind is not None:
            from pgwire_calcite.backend import PgProtocolError

            raise PgProtocolError(
                "0A000",
                f"{ddl_kind} is not supported: pgwire-calcite serves a read-only "
                "Calcite model; change the model to change the schema",
            )

        if self.role_id is None:
            raise RuntimeError("Not authenticated")

        import pgwire_calcite.server as _m

        _state = _m.state
        if _state is None:
            raise RuntimeError("Server state not initialized")

        dml_kind = _dml_statement_kind(stripped)
        if dml_kind is not None:
            return self._execute_dml(_state, dml_kind, stripped)

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
                return self._track(CalciteQueryResult(result, stripped))

        # information_schema is answered by Calcite, scoped to the role's catalog and reported
        # in PG's type names (info_schema.py): its views are rewritten into derived tables
        # before execution, so authorization below admits them as catalog reads.
        _info_views: frozenset = frozenset()
        if getattr(_state, "catalog_enabled", False):
            from pgwire_calcite import info_schema

            if info_schema.references(stripped):
                stripped = self._rewrite_information_schema(_state, stripped)
                _info_views = frozenset((info_schema.SCHEMA, v) for v in info_schema.VIEWS)

        # Per-role authorization (PGW-045): reject out-of-grant relations before
        # execution — enforced on the same grants that filter discovery.
        _grants = getattr(_state, "authz_grants", None)
        if _grants is not None:
            from pgwire_calcite.authz import enforce_query

            enforce_query(_grants, self.role_id or "", stripped, catalog_reads=_info_views)

        # Usage quota (kenstott/calcite#364): a no-op when ASKAMERICA_API_KEY isn't
        # set (local dev / self-hosted runs). Checked per query, same cadence as
        # askamerica-engine's embedded-mode UsageMetering.StatementHandler; cheap
        # thanks to metering.py's 60s cache. Raises PermissionError, already an
        # established, handled error type on this exact call path (enforce_query
        # above raises the same type for the same reason).
        metering.enforce_quota()

        # Non-catalog execution seam: Phase 0 StubBackend -> Phase 1 CalciteBackend.
        # stream=True selects the Arrow batch-streaming path at the wire (Phase 3);
        # backends that don't stream ignore the flag and materialize.
        from pgwire_calcite.backend import PgProtocolError

        try:
            # backend is declared as object (ServerState carries no Backend protocol type
            # yet); every concrete backend (Stub/Calcite/Bridge) implements execute_sql.
            result = _state.backend.execute_sql(  # type: ignore[attr-defined]
                stripped,
                self.role_id,
                params,
                stream=True,
                session_key=self.key,
                timeout_ms=self.statement_timeout_ms,
                lane=self._lane(),
                client_gone=self.client_gone,
            )
        except PermissionError as exc:
            raise PermissionError(str(exc)) from exc
        except PgProtocolError:
            # Already carries its SQLSTATE (57014 cancel/timeout) — send as-is.
            raise
        except Exception as exc:
            log.warning("[PGWIRE] EXCEPTION sql=%r", stripped[:300], exc_info=True)
            raise RuntimeError(str(exc)) from exc

        return self._track(CalciteQueryResult(result, stripped))


    def _track_transaction(self, sql: str) -> None:
        """Follow BEGIN/COMMIT/ROLLBACK, refusing a ROLLBACK that cannot undo a write."""
        if _TXN_BEGIN_RE.match(sql):
            self._in_transaction = True
            self._wrote_in_transaction = False
            return
        if not _TXN_END_RE.match(sql):
            return
        wrote = self._in_transaction and self._wrote_in_transaction
        # ROLLBACK TO SAVEPOINT stays inside the transaction; everything else ends it.
        to_savepoint = re.match(r"^\s*ROLLBACK\s+(TRANSACTION\s+|WORK\s+)?TO\b", sql, re.IGNORECASE)
        if not to_savepoint:
            self._in_transaction = False
            self._wrote_in_transaction = False
        if wrote and _TXN_ROLLBACK_RE.match(sql):
            from pgwire_calcite.backend import PgProtocolError

            raise PgProtocolError(
                "0A000",
                "ROLLBACK cannot undo this transaction's writes: each INSERT, UPDATE and "
                "DELETE was committed to the data source when it ran",
            )

    def _execute_dml(self, state, kind: str, pg_sql: str) -> CalciteQueryResult:
        """Run one INSERT/UPDATE/DELETE and answer with its CommandComplete tag."""
        from pgwire_calcite.backend import PgProtocolError

        if not getattr(state, "allow_writes", False):
            raise PgProtocolError(
                "25006",
                f"cannot execute {kind}: this pgwire-calcite server is read-only "
                "(it was started without --allow-writes)",
            )
        backend = state.backend
        if not hasattr(backend, "execute_update"):
            raise PgProtocolError(
                "0A000", f"{kind} is not supported by the {type(backend).__name__} backend"
            )
        from pgwire_calcite import returning

        try:
            plan = returning.plan(pg_sql) if _RETURNING_RE.search(pg_sql) else None
        except ValueError as exc:
            raise PgProtocolError("0A000", str(exc)) from exc

        # The same grants that gate reads gate writes: every relation the statement names,
        # its target included, must be granted to the role.
        grants = getattr(state, "authz_grants", None)
        if grants is not None:
            from pgwire_calcite.authz import enforce_query

            enforce_query(grants, self.role_id or "", pg_sql)
        metering.enforce_quota()

        try:
            if plan is None:
                count = backend.execute_update(pg_sql, **self._statement_args())
                result = CalciteQueryResult(
                    TrinoResult(), pg_sql, status=_dml_command_tag(kind, count)
                )
            else:
                result = CalciteQueryResult(self._execute_returning(backend, plan), pg_sql)
                result.row_tag_prefix = "INSERT 0" if kind == "INSERT" else kind
        except (PermissionError, PgProtocolError):
            raise
        except Exception as exc:
            log.warning("[PGWIRE] EXCEPTION sql=%r", pg_sql[:300], exc_info=True)
            raise RuntimeError(str(exc)) from exc
        if self._in_transaction:
            self._wrote_in_transaction = True
        return result

    def _statement_args(self) -> dict:
        return {
            "session_key": self.key,
            "timeout_ms": self.statement_timeout_ms,
            "lane": self._lane(),
            "client_gone": self.client_gone,
        }

    def _select_rows(self, backend, pg_sql: str) -> TrinoResult:
        """Run a read-back SELECT and hold all of its rows; a write's result is bounded by
        the write, and its rows must outlive the statement that produced them."""
        result = backend.execute_sql(pg_sql, self.role_id, None, **self._statement_args())
        rows = [tuple(r) for r in result.iter_rows()]
        return TrinoResult(
            rows=rows, column_names=list(result.column_names), column_types=result.column_types
        )

    def _execute_returning(self, backend, plan) -> TrinoResult:
        """Run a DML statement that has RETURNING; see :mod:`pgwire_calcite.returning`."""
        from pgwire_calcite import returning

        table_ref = (plan.schema, plan.table)
        key = backend.key_column(table_ref, **self._statement_args())
        if plan.kind == "DELETE":
            deleted = self._select_rows(
                backend, returning.select_sql(plan.select_list, plan.table_sql, plan.where_sql)
            )
            backend.execute_update(plan.write_sql, **self._statement_args())
            return deleted
        if plan.kind == "UPDATE":
            matched = self._select_rows(
                backend,
                returning.select_sql(
                    returning.quote_identifier(key), plan.table_sql, plan.where_sql
                ),
            )
            keys = [row[0] for row in matched.rows]
            backend.execute_update(plan.write_sql, **self._statement_args())
        else:
            _, keys = backend.execute_insert(plan.write_sql, table_ref, **self._statement_args())

        # Read the written rows back by key, the key riding along as a last column so the
        # rows can be put in the order the keys were produced; it is dropped again below.
        select_list = (
            f"{plan.select_list}, {returning.quote_identifier(key)} "
            f"AS {returning.quote_identifier(returning.KEY_ALIAS)}"
        )
        conditions = returning.key_conditions(key, keys) or ["1 = 0"]
        names: list = []
        types = None
        by_key: dict = {}
        for condition in conditions:
            chunk = self._select_rows(
                backend, returning.select_sql(select_list, plan.table_sql, condition)
            )
            names, types = chunk.column_names, chunk.column_types
            for row in chunk.rows:
                by_key[str(row[-1])] = row[:-1]
        rows = [by_key[str(k)] for k in keys if str(k) in by_key]
        return TrinoResult(
            rows=rows,
            column_names=names[:-1],
            column_types=types[:-1] if types else None,
        )

    def infer_parameter_oids(self, pg_sql: str, numbers: list) -> dict:
        """The PostgreSQL type OID of each ``$N`` in ``numbers`` that the engine can type.

        A statement the engine does not plan itself (a catalog query answered by the
        intercept, a session command) has no inference; its parameters are left out, and
        the caller describes them as text, which is what catalog look-ups by name bind.
        """
        if not numbers:
            return {}
        import pgwire_calcite.server as _m

        state = _m.state
        backend = getattr(state, "backend", None)
        if backend is None or not hasattr(backend, "parameter_types"):
            return {}
        if getattr(state, "catalog_enabled", False):
            from pgwire_calcite.catalog import classify

            if classify(pg_sql) == "INTERCEPT":
                return {}
        sql = pg_sql
        if _DML_RE.match(sql) and _RETURNING_RE.search(sql):
            from pgwire_calcite import returning

            plan = returning.plan(sql)
            if plan is not None:
                sql = plan.write_sql
        try:
            names = backend.parameter_types(sql, **self._statement_args())
        except Exception as exc:
            # The statement will fail the same way when it is executed, with this error
            # reported to the client then; describing it must not be what breaks it.
            log.warning("[PGWIRE] could not infer parameter types of %r: %s", pg_sql[:200], exc)
            return {}
        return {
            n: _SQL_TYPE_OID.get(str(names[n]).split("(")[0].strip().upper(), 25)
            for n in numbers
            if n in names
        }

    def describe_returning(self, pg_sql: str) -> "CalciteQueryResult | None":
        """The columns of a DML statement's RETURNING clause, without running the write: a
        zero-row SELECT of the same expressions. None when the statement has no RETURNING."""
        from pgwire_calcite import returning
        from pgwire_calcite.backend import PgProtocolError

        if not _RETURNING_RE.search(pg_sql):
            return None
        try:
            plan = returning.plan(pg_sql)
        except ValueError as exc:
            raise PgProtocolError("0A000", str(exc)) from exc
        if plan is None:
            return None
        return self.execute_sql(
            returning.select_sql(plan.select_list, plan.table_sql, "1 = 0")
        )

    def _rewrite_information_schema(self, state, pg_sql: str) -> str:
        """``pg_sql`` with its information_schema views scoped to this role's catalog."""
        from pgwire_calcite import info_schema

        ctx = state.contexts.get(self.role_id or "")
        visible = [(tm.schema_name, tm.table_name) for tm in ctx.tables.values()]

        def columns_of() -> list[str]:
            # Calcite's own information_schema.columns column list, asked once per process: the
            # derived table names every column so it can restate data_type in PG's names.
            cached = getattr(state, "information_schema_columns", None)
            if cached is None:
                probe = state.backend.execute_sql(  # type: ignore[attr-defined]
                    f"SELECT * FROM {info_schema.SCHEMA}.columns WHERE 1 = 0",
                    self.role_id,
                )
                cached = list(probe.column_names)
                state.information_schema_columns = cached
            return cached

        rewritten = info_schema.rewrite(pg_sql, visible, columns_of)
        assert rewritten is not None  # references() said it reads one of the views
        return rewritten


class CalciteConnection(Connection):
    def new_session(self) -> CalciteSession:
        return CalciteSession()

    def parameters(self) -> dict[str, str]:
        # Startup ParameterStatus set. server_version declares PG 14, so we report the
        # full PG-14 hard-wired set (PG protocol §54.2), including the PG-14 additions
        # default_transaction_read_only and in_hot_standby. Values are sourced from
        # _KNOWN_SETTINGS so the handshake and SHOW/current_setting stay consistent
        # (server_version >= 14 also clears DuckDB's >=12 gate and DBeaver/DataGrip
        # feature gates -- PGW-001). Casing follows what PG sends.
        from pgwire_calcite.catalog import _KNOWN_SETTINGS as s

        return {
            "server_version": s["server_version"],
            "server_encoding": s["server_encoding"],
            "client_encoding": s["client_encoding"],
            "application_name": s["application_name"],
            "is_superuser": s["is_superuser"],
            "session_authorization": s["session_authorization"],
            "DateStyle": s["datestyle"],
            "IntervalStyle": s["intervalstyle"],
            "TimeZone": s["timezone"],
            "integer_datetimes": s["integer_datetimes"],
            "standard_conforming_strings": s["standard_conforming_strings"],
            "default_transaction_read_only": s["default_transaction_read_only"],
            "in_hot_standby": s["in_hot_standby"],
        }


class CalciteHandler(BuenaVistaHandler):  # PGW-002, PGW-007
    """Extends BuenaVistaHandler with TLS, cleartext auth, and catalog intercept."""

    #: Set in handle_startup; read by finish(), which socketserver always runs.
    _ctx: BVContext | None = None

    #: Bound on how long an accepted connection has to complete the startup
    #: handshake (SSLRequest negotiation through authentication). Measured live
    #: (2026-09-21): a long-lived pgwire-govdata process accumulated hundreds of
    #: stuck handler threads over a few hours and stopped answering real queries.
    #: Root cause: handle_startup's first read (self.r.read_uint32()) is a plain
    #: blocking socket read with no timeout, so any connection that opens the TCP
    #: socket but never sends a complete startup packet -- a bare liveness probe,
    #: a client that connects then vanishes -- leaves that thread parked forever.
    #: handle() never returns, finish() never runs, and the FD/thread is never
    #: released. Cleared to blocking once the handshake actually completes
    #: (see handle_startup's authenticated-return points) so a real, idle-but-
    #: connected interactive session is never disconnected mid-use.
    _STARTUP_TIMEOUT_SECONDS = 15.0

    #: TCP keepalive tuning for the case _STARTUP_TIMEOUT_SECONDS doesn't cover: a client
    #: that completes the handshake, then vanishes without a clean close (its machine is
    #: killed, its network drops) rather than sending FIN/RST. Once authenticated, the
    #: socket timeout is cleared to blocking forever (see handle_startup's authenticated
    #: returns) precisely so a real idle-but-connected session isn't disconnected -- but
    #: that means a plain read on a silently-dead peer blocks forever too: finish() never
    #: runs, connection_closed() never fires, and CalciteServer's idle-shutdown watcher
    #: (see maybe_start_idle_shutdown_watcher) never sees the connection count reach zero,
    #: so an orphaned singleton server can never self-reap. TCP keepalive makes the kernel
    #: itself probe a silent peer and force a read error if it's actually gone, without
    #: touching genuinely idle-but-alive sessions (which keep answering pings just fine).
    #: ~30s idle + 3 probes * 10s apart = dead peer detected within ~60s worst case.
    _KEEPALIVE_IDLE_SECONDS = 30
    _KEEPALIVE_INTERVAL_SECONDS = 10
    _KEEPALIVE_PROBE_COUNT = 3

    def _enable_tcp_keepalive(self) -> None:
        sock = self.request
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        except OSError:
            return  # not a real TCP socket (e.g. a test double) -- nothing more to do
        # Option names differ by platform (Linux: TCP_KEEPIDLE; macOS: TCP_KEEPALIVE for
        # the same "seconds idle before the first probe" role) -- set whichever exist
        # rather than assuming one platform, and never let a missing/rejected option
        # (an unsupported kernel, a mocked socket in a unit test) abort the connection.
        for opt_name, value in (
            (getattr(socket, "TCP_KEEPIDLE", None), self._KEEPALIVE_IDLE_SECONDS),
            (getattr(socket, "TCP_KEEPALIVE", None), self._KEEPALIVE_IDLE_SECONDS),
            (getattr(socket, "TCP_KEEPINTVL", None), self._KEEPALIVE_INTERVAL_SECONDS),
            (getattr(socket, "TCP_KEEPCNT", None), self._KEEPALIVE_PROBE_COUNT),
        ):
            if opt_name is None:
                continue
            try:
                sock.setsockopt(socket.IPPROTO_TCP, opt_name, value)
            except OSError:
                pass

    def setup(self) -> None:
        super().setup()
        self.request.settimeout(self._STARTUP_TIMEOUT_SECONDS)
        self._enable_tcp_keepalive()
        self.server.connection_opened(self)

    def terminate(self, reason: str) -> None:
        """End this connection from another thread (server shutdown): cancel what it is
        running in the engine, then shut its socket so the thread serving it returns."""
        ctx = self._ctx
        backend = _current_backend()
        if ctx is not None and backend is not None:
            try:
                backend.cancel_session(ctx.session.key, reason)
            except Exception:  # noqa: BLE001 - reported; the socket is shut regardless
                log.exception("[PGWIRE] could not cancel session %s", ctx.session.key)
        ClientLink(self.request).terminate(reason)

    def finish(self) -> None:
        ctx = self._ctx
        self._ctx = None
        try:
            if ctx is not None:
                # buenavista's handle() unregisters the context only on its clean exit.
                # A client that vanishes mid-result breaks the pipe on its error path
                # first, and every such connection then stayed in the server's table
                # (context, prepared statements, portals) for the life of the process.
                self.server.ctxts.pop(ctx.process_id, None)  # type: ignore[attr-defined]
                ctx.session.close()
        finally:
            try:
                super().finish()
            except OSError as gone:
                # Flushing to a client that already disconnected: nothing left to send.
                log.debug("[PGWIRE] connection already closed at finish: %s", gone)
            finally:
                self.server.connection_closed(self)

    # Per-connection SASL SCRAM exchange state; None before the handshake starts and
    # between the SASLInitialResponse and SASLResponse messages is impossible (only ever
    # None or a live exchange) — annotated so static analysis knows verify_final()/
    # server_first() are reached only once it is set.
    _scram: Optional["ScramServerExchange"] = None

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

    def _send_pg_notice(self, message: str) -> None:
        """Send a NoticeResponse (a non-fatal, out-of-band message) — never touches result rows."""
        buf = BVBuffer()
        for field, value in (
            (b"S", "NOTICE"),
            (b"V", "NOTICE"),
            (b"C", "01000"),  # SQLSTATE warning class
            (b"M", message),
        ):
            buf.write_bytes(field)
            buf.write_string(value)
        buf.write_bytes(b"\x00")
        out = buf.get_value()
        self.wfile.write(struct.pack("!ci", ServerResponse.NOTICE_RESPONSE, len(out) + 4))
        self.wfile.write(out)
        self.wfile.flush()

    def handle_post_auth(self, ctx: BVContext) -> None:  # type: ignore[override]
        """Runs once per connection immediately after AuthenticationOk.

        Anything emitted here must be out-of-band (a NoticeResponse via
        ``_send_pg_notice``) so query results are never modified or gated.
        """
        super().handle_post_auth(ctx)
        # The handshake is complete on every authentication path (trust, cleartext,
        # SCRAM): clear the startup timeout here, the one place they all pass through,
        # so an authenticated session that sits idle is not cut off by it.
        self.request.settimeout(None)

    def _send_lockout(self, locked: LockedOut) -> None:
        # A lockout ends the connection, so it is a FATAL ErrorResponse with SQLSTATE 28000
        # (invalid_authorization_specification); ``_send_pg_notice`` is for post-auth messages.
        self._send_pg_error("FATAL", "28000", str(locked))

    def _assert_peer_binding(self, username: str) -> None:
        """Bind the TLS client certificate to the startup packet's user, when configured
        (PGWIRE_CALCITE_MTLS_BIND_PRINCIPAL, Phase 3 hardening).

        A plaintext connection has no peer certificate to inspect; ``mtls_auth`` is None
        unless a client CA was configured, so the check is a no-op there. The socket is the
        wrapped one — ``handle_startup`` replaced ``self.request`` during the SSLRequest
        exchange.
        """
        from pgwire_calcite.mtls import assert_principal_binding

        auth = getattr(self.server, "mtls_auth", None)
        if auth is None or not auth.bind_principal:
            return
        peer_cert = self.request.getpeercert() if isinstance(self.request, ssl.SSLSocket) else None
        assert_principal_binding(auth, peer_cert, username)

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
        elif code == 80877102:  # Cancel request (PGW-050)
            # Arrives on a fresh connection that never authenticates, carrying the
            # (process id, secret key) we handed the target session in
            # BackendKeyData. Cancel that session's in-flight engine statement and
            # leave the session itself open — PG's contract is that a cancelled
            # backend keeps serving, it does not disconnect.
            process_id = self.r.read_uint32()
            secret_key = self.r.read_uint32()
            ctx = self.server.ctxts.get(process_id)  # type: ignore[attr-defined]
            if ctx is not None and ctx.secret_key == secret_key:
                from pgwire_calcite.backend import CANCELED_BY_USER

                # Through the backend seam, not a process-global registry: with the
                # sidecar topology the statement runs in the Calcite child and the
                # cancel has to cross the bridge (PGW-050).
                backend = _current_backend()
                if backend is None:
                    raise RuntimeError("Server state not initialized")
                cancelled = backend.cancel_session(str(ctx.session.id), CANCELED_BY_USER)
                log.info(
                    "[PGWIRE] cancel request for pid=%s: %s",
                    process_id,
                    "statement cancelled" if cancelled else "no statement in flight",
                )
            else:
                log.info("[PGWIRE] cancel request for pid=%s rejected (bad key)", process_id)
            return None
        elif code == 196608:  # Protocol 3.0
            msg = [x.decode("utf-8") for x in self.r.read_bytes(msglen - 4).split(b"\x00")]
            params = dict(zip(msg[::2], msg[1::2]))
            log.info(
                "[PGWIRE] connect params: %s", {k: v for k, v in params.items() if k != "password"}
            )
            ctx = BVContext(conn.create_session(), None, params)
            # Kept for finish(): buenavista's handle() only releases the session on a
            # clean exit, and a client that vanishes mid-stream can break the pipe on
            # its error path before that runs — which would strand the session's open
            # JDBC statement, Arrow allocator and backend lock (PGW-022).
            self._ctx = ctx
            ctx.session.bind_context(ctx)  # type: ignore[attr-defined]
            ctx.session.client_gone = ClientLink(self.request)  # type: ignore[attr-defined]
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
                username = params.get("user", "")
                # Trust mode presents no password, so there is nothing to guess and no
                # throttle to apply here; a misconfigured provider is still answered on
                # the wire rather than dropping the socket (Phase 3 hardening).
                try:
                    role = _prov.authenticate(username, "") if _prov is not None else username
                except ValueError as exc:
                    self._send_pg_error(
                        "FATAL", "28P01", f"pgwire auth provider unavailable: {exc}"
                    )
                    return None
                try:
                    self._assert_peer_binding(username)
                except PermissionError as exc:
                    self._send_pg_error("FATAL", "28000", str(exc))
                    return None
                ctx.session.role_id = role  # type: ignore[attr-defined]
                self.send_authentication_ok()
                self.handle_post_auth(ctx)
                # Handshake complete (ctx.authenticated is now True) -- clear the
                # startup timeout so a real, idle-but-connected session is never cut
                # off waiting on the client's next message. See _STARTUP_TIMEOUT_SECONDS.
                self.request.settimeout(None)
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
            try:
                login_throttle().check(subject_key(username))
            except LockedOut as locked:
                self._send_lockout(locked)
                return
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

            exchange = ScramServerExchange(verifier)
            self._scram = exchange
            server_first = exchange.server_first(client_first)
            self._send_auth_msg(11, server_first.encode("utf-8"))  # SASLContinue
            return
        # SASLResponse: client-final-message. Reached only after the SASLInitialResponse
        # branch above set self._scram to a live exchange; asserted so static analysis
        # doesn't have to infer it across the two separate wire messages.
        exchange = self._scram
        assert exchange is not None
        client_final = payload.rstrip(b"\x00").decode("utf-8")
        subject = subject_key(username)
        try:
            login_throttle().check(subject)
        except LockedOut as locked:
            self._send_lockout(locked)
            return
        ok, server_final = exchange.verify_final(client_final)
        if not ok:
            login_throttle().record_failure(subject)
            self._send_pg_error(
                "FATAL", "28P01", f'password authentication failed for user "{username}"'
            )
            return
        login_throttle().record_success(subject)
        try:
            self._assert_peer_binding(username)
        except PermissionError as exc:
            self._send_pg_error("FATAL", "28000", str(exc))
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
        # Brute-force throttling (Phase 3 hardening) and the bearer-vs-basic decision
        # (PGWIRE_CALCITE_PAT_PREFIX / OIDC) apply here, uniformly, on every provider.
        _prov = getattr(_state, "auth_provider", None)
        if _prov is not None:
            subject = subject_key(username)
            try:
                role = throttled_auth(
                    lambda: _authenticate_credential(_prov, username, password), subject=subject
                )
            except LockedOut as locked:
                self._send_lockout(locked)
                return
            except ValueError as exc:
                self._send_pg_error(
                    "FATAL", "28P01", f"pgwire auth provider unavailable: {exc}"
                )
                return
            if role is None:
                self._send_pg_error(
                    "FATAL", "28P01", f'password authentication failed for user "{username}"'
                )
                return
            try:
                self._assert_peer_binding(username)
            except PermissionError as exc:
                self._send_pg_error("FATAL", "28000", str(exc))
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

        subject = subject_key(username)
        try:
            ok = throttled_auth(
                lambda: username if _state.check_password(username, password) else None,
                subject=subject,
            )
        except LockedOut as locked:
            self._send_lockout(locked)
            return
        if ok is None:
            self._send_pg_error(
                "FATAL", "28P01", f'password authentication failed for user "{username}"'
            )
            return

        try:
            self._assert_peer_binding(username)
        except PermissionError as exc:
            self._send_pg_error("FATAL", "28000", str(exc))
            return

        ctx.session.role_id = username  # type: ignore[attr-defined]
        self.send_authentication_ok()
        self.handle_post_auth(ctx)

    def send_error(self, exception, ctx: Optional[BVContext] = None) -> None:  # type: ignore[override]
        """Emit ErrorResponse with a real SQLSTATE for errors that declare one.

        buenavista's ErrorResponse carries only a message field, which clients
        read as SQLSTATE XX000. Cancellation must be distinguishable (57014) or a
        client cannot tell "you cancelled me" from "the engine broke" (PGW-050).
        """
        sqlstate = _sqlstate_of(exception)
        if sqlstate == _SQLSTATE_INTERNAL_ERROR:
            log.error("[PGWIRE] %s: %s", sqlstate, exception)
        else:
            log.info("[PGWIRE] %s: %s", sqlstate, exception)
        if ctx is not None:
            self._send_pg_error("ERROR", sqlstate, str(exception))
            ctx.mark_error()
            return
        # No context: buenavista's handle() reports the error that is ending the
        # connection. That is FATAL to the client, and very often the error is that the
        # client is already gone -- then there is nobody left to tell.
        try:
            self._send_pg_error("FATAL", sqlstate, str(exception))
        except OSError as gone:
            log.info("[PGWIRE] client disconnected before the error could be sent: %s", gone)

    # --- extended protocol: one error rule for every message ------------------

    def _extended(self, name: str, handler, ctx: BVContext, payload: bytes) -> None:
        """Run one extended-protocol message under PostgreSQL's error rule: a failure is
        answered with an ErrorResponse and every later message is discarded until Sync;
        the connection survives. Only a failure of the connection itself propagates."""
        from pgwire_calcite.backend import PgProtocolError

        if ctx.has_error:
            return
        try:
            handler(ctx, payload)
        except PermissionError as exc:  # an OSError by inheritance, but the client's to see
            self.send_error(exc, ctx)
        except OSError:
            raise
        except (struct.error, IndexError, UnicodeDecodeError) as exc:
            self.send_error(PgProtocolError("08P01", f"malformed {name} message: {exc}"), ctx)
        except Exception as exc:
            self.send_error(exc, ctx)

    def handle_parse(self, ctx: BVContext, payload: bytes) -> None:
        self._extended("Parse", super().handle_parse, ctx, payload)

    def handle_bind(self, ctx: BVContext, payload: bytes) -> None:
        self._extended("Bind", self._bind, ctx, payload)

    def handle_describe(self, ctx: BVContext, payload: bytes) -> None:
        self._extended("Describe", self._describe, ctx, payload)

    def handle_execute(self, ctx: BVContext, payload: bytes) -> None:
        self._extended("Execute", self._execute, ctx, payload)

    def handle_close(self, ctx: BVContext, payload: bytes) -> None:
        self._extended("Close", self._close, ctx, payload)

    def _close(self, ctx: BVContext, payload: bytes) -> None:
        """Close a prepared statement or a portal. Closing one that does not exist is not
        an error (PostgreSQL's rule; a client closes names the server already dropped
        after DISCARD ALL or DEALLOCATE). Closing a portal releases its result now: a
        cursor closed after a partial fetch otherwise kept the engine until the session's
        next statement."""
        from pgwire_calcite.backend import PgProtocolError

        kind, name = payload[0], payload[1:-1].decode("utf-8")
        if kind == ord("S"):
            ctx.stmts.pop(name, None)
        elif kind == ord("P"):
            ctx.portals.pop(name, None)
            cached = ctx.result_cache.pop(name, None)
            if cached is not None:
                cached.close()
        else:
            raise PgProtocolError("08P01", f"invalid Close target type {chr(kind)!r}")
        self.send_close_complete()

    @staticmethod
    def _require_statement(ctx: BVContext, name: str) -> None:
        from pgwire_calcite.backend import PgProtocolError

        if name not in ctx.stmts:
            raise PgProtocolError("26000", f'prepared statement "{name}" does not exist')

    @staticmethod
    def _require_portal(ctx: BVContext, name: str) -> None:
        from pgwire_calcite.backend import PgProtocolError

        if name not in ctx.portals:
            raise PgProtocolError("34000", f'portal "{name}" does not exist')

    def send_data_rows(self, query_result, limit: int = 0) -> int:  # type: ignore[override]
        # Remembered for the CommandComplete that follows: buenavista tags every result
        # that carries rows "SELECT n", which is wrong for a write with RETURNING.
        self._row_tag_prefix = getattr(query_result, "row_tag_prefix", None)
        return super().send_data_rows(query_result, limit)

    def send_command_complete(self, tag: str) -> None:  # type: ignore[override]
        prefix = getattr(self, "_row_tag_prefix", None)
        self._row_tag_prefix = None
        if prefix is not None and tag.startswith("SELECT "):
            tag = prefix + tag[len("SELECT"):]
        super().send_command_complete(tag)

    def _resolve_param_oids(self, ctx: BVContext, stmt: str) -> list:
        """The type OID of each ``$N`` of a prepared statement: declared at Parse, named by
        an inline cast, or inferred by the engine. Remembered per session by SQL text,
        because a client may Parse the same statement again before it binds (asyncpg does,
        with its statement cache off), and Parse forgets what Describe worked out."""
        sql, declared = ctx.stmts[stmt]
        cache = ctx.session.param_oid_cache
        if not declared and sql in cache:
            return cache[sql]
        indices = {int(m) for m in re.findall(r"\$(\d+)", sql)}
        if "typeinfo_tree" in sql.lower() and indices:
            # OID 1028 = _oid (oid[]) — asyncpg has a built-in binary codec for this,
            # so it can encode list(typeoids) and we can decode the binary response.
            param_oids = [1028]
        elif "set_config" in sql.lower() and indices:
            # set_config takes TEXT params; OID 25 prevents asyncpg from looping on OID 0
            param_oids = [25] * len(indices)
        else:
            stored_oids = ctx.stmts[stmt][1]
            if stored_oids:
                param_oids = stored_oids
            elif indices:
                # No Parse-declared OIDs: an inline cast (`$1::text`) names the type;
                # otherwise it is the type Calcite infers from where the parameter is
                # used (`col = $1` takes the column's). OID 0 (unspecified) is never
                # sent: it makes psycopg/asyncpg re-describe forever.
                cast_map = {
                    int(m): _CAST_OID.get(t.lower(), 25)
                    for m, t in re.findall(r"\$(\d+)::(\w+)", sql)
                }
                inferred = ctx.session.infer_parameter_oids(
                    sql, [i for i in sorted(indices) if i not in cast_map]
                )
                param_oids = [
                    cast_map.get(i) or inferred.get(i, 25) for i in range(1, max(indices) + 1)
                ]
            else:
                param_oids = []
        if not declared:
            cache[sql] = param_oids
        return param_oids

    def _bind(self, ctx: BVContext, payload: bytes) -> None:
        # A parameter sent in binary can only be decoded with its type. If this statement's
        # types are not known here (never described, or parsed again since), work them out.
        ba = bytearray(payload)
        portal_end = ba.index(0)
        stmt_end = ba.index(0, portal_end + 1)
        stmt = ba[portal_end + 1 : stmt_end].decode("utf-8")
        self._require_statement(ctx, stmt)
        if stmt in ctx.stmts and not ctx.stmts[stmt][1]:
            (num_formats,) = struct.unpack("!h", ba[stmt_end + 1 : stmt_end + 3])
            formats = struct.unpack(
                f"!{num_formats}h", ba[stmt_end + 3 : stmt_end + 3 + 2 * num_formats]
            )
            sql = ctx.stmts[stmt][0]
            if any(f == 1 for f in formats) and sql.strip() and not _COPY_RE.match(sql):
                try:
                    ctx.stmts[stmt] = (sql, self._resolve_param_oids(ctx, stmt))
                except Exception as e:
                    self.send_error(e, ctx)
                    return
        super().handle_bind(ctx, payload)

    def _describe(self, ctx: BVContext, payload: bytes) -> None:
        ba = bytearray(payload)
        if ba[0] == ord("P"):
            portal = ba[1 : len(ba) - 1].decode("utf-8")
            self._require_portal(ctx, portal)
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
            # A write without RETURNING returns no rows; describing it must not run it. One
            # with RETURNING takes the default path, which runs the portal once and keeps
            # its result for Execute.
            if (
                portal_sql
                and _DML_RE.match(portal_sql)
                and not _RETURNING_RE.search(portal_sql)
            ):
                self.send_no_data()
                return
        elif ba[0] == ord("S"):
            stmt = ba[1 : len(ba) - 1].decode("utf-8")
            self._require_statement(ctx, stmt)
            sql = ctx.stmts[stmt][0]
            if not sql.strip():
                self.send_paramter_description([])
                self.send_no_data()
                return
            if _COPY_RE.match(sql):
                self.send_paramter_description([])
                self.send_no_data()
                return
            param_oids = self._resolve_param_oids(ctx, stmt)
            # Store the resolved OIDs so describe_statement substitutes typed example
            # values instead of executing the SQL with unresolved $N placeholders.
            ctx.stmts[stmt] = (sql, param_oids)
            # A write returns no rows, and describe_statement would run it (with example
            # parameter values) to learn that.
            if _DML_RE.match(sql):
                try:
                    described = ctx.session.describe_returning(_example_sql(sql, param_oids))
                except Exception as e:
                    self.send_error(e, ctx)
                    return
                self.send_paramter_description(param_oids)
                if described is None:
                    self.send_no_data()
                else:
                    try:
                        self.send_row_description(described)
                    finally:
                        described.close()
                return
            try:
                # describe_statement substitutes typed example values for the $N
                # placeholders (0-row result) but gives us the column schema.
                query_result = ctx.describe_statement(stmt)
            except Exception as e:
                self.send_error(e, ctx)
                return
            try:
                self.send_paramter_description(param_oids)
                if query_result.has_results():
                    self.send_row_description(query_result)
                else:
                    self.send_no_data()
            finally:
                # This result is never drained (only column metadata is used), so
                # without an explicit close() the Calcite lock arrow_bridge holds
                # for the underlying batch iterator's whole lifetime is never
                # released, wedging every later statement on this connection
                # (kenstott/govdata-ops#782).
                close = getattr(query_result, "close", None)
                if close is not None:
                    close()
            return
        super().handle_describe(ctx, payload)

    def _execute(self, ctx: BVContext, payload: bytes) -> None:
        ba = bytearray(payload)
        portal_idx = ba.index(0)
        portal = ba[:portal_idx].decode("utf-8")
        self._require_portal(ctx, portal)
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
        from pgwire_calcite.backend import PgProtocolError

        decoded = payload.decode("utf-8").rstrip("\x00")

        # Statement-aware split: a ';' inside a string literal, comment, quoted
        # identifier or dollar-quoted body must NOT mis-split, so the COPY/DDL regex
        # matching below sees identical statement boundaries to what executes
        # (replaces the old naive decoded.split(';')).
        from pgwire_calcite.normalize import split_sql_statements

        stmts = split_sql_statements(decoded)
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
                try:
                    # Rows stream lazily off the engine, so a cancel can land after
                    # RowDescription. PG allows ErrorResponse mid-result-set; the
                    # connection survives and the next query runs (PGW-050).
                    row_count = self.send_data_rows(query_result)
                except PgProtocolError as exc:
                    self.send_error(exc, ctx)
                    break
                self.send_command_complete("SELECT %d\x00" % row_count)
            else:
                status = query_result.status()
                self.send_command_complete(f"{status}\x00")

        # A simple Query is its own synchronisation point: an error in it must not leave
        # the connection discarding the next extended-protocol batch (whose Execute was
        # then skipped without a word, so the client read an empty result).
        ctx.sync()
        self.send_ready_for_query(ctx)


class CalciteServer(BuenaVistaServer):  # PGW-001
    allow_reuse_address = True

    def __init__(
        self,
        server_address: tuple[str, int],
        conn: CalciteConnection,
        ssl_ctx: ssl.SSLContext | None = None,
        mtls_auth=None,
        sock: socket.socket | None = None,
    ) -> None:
        # `sock`, when given, is an already bound+listening socket (see launcher.py's
        # claim_listen_socket()) claimed BEFORE the caller built its backend — closing the
        # port-exhaustion leak where a losing bind race used to build a full CalciteBackend
        # (JVM + ~250 S3/Iceberg connections) first and only discover it lost minutes later.
        # bind_and_activate=False skips TCPServer's own bind/listen so we don't rebind (which
        # would race all over again); we just adopt the socket that already won.
        if sock is not None:
            socketserver.ThreadingTCPServer.__init__(  # type: ignore[arg-type]
                self, server_address, CalciteHandler, bind_and_activate=False
            )
            self.socket.close()
            self.socket = sock
        else:
            socketserver.ThreadingTCPServer.__init__(self, server_address, CalciteHandler)  # type: ignore[arg-type]
        self.conn = conn
        self.rewriter = None
        self.extensions: dict = {}
        self.ctxts: dict = {}
        self.auth = None
        self.ssl_ctx = ssl_ctx
        # Client-certificate policy (PGWIRE_CALCITE_CLIENT_CA, Phase 3 hardening); None means
        # mTLS is off. Read by CalciteHandler._assert_peer_binding.
        self.mtls_auth = mtls_auth
        # Live connection count, for the opt-in idle-shutdown watcher (see
        # maybe_start_idle_shutdown_watcher). Incremented in CalciteHandler.setup(),
        # decremented in CalciteHandler.finish() — both run once per accepted
        # connection, symmetric by construction (socketserver.BaseRequestHandler
        # always calls finish() after setup(), even when handle() raises).
        self._active_connections = 0
        self._active_lock = threading.Lock()
        #: The handlers of the connections that are open, for terminate_sessions().
        self._handlers: set = set()
        # Wall-clock time.monotonic() the count first reached zero, or None while
        # a connection is live. Read/written only under _active_lock.
        self._idle_since: float | None = None

    def verify_request(self, request, client_address) -> bool:
        del request, client_address
        return True

    def handle_error(self, request, client_address) -> None:
        """A connection that ends because its client went away is routine (a client
        process killed mid-result, a driver's socket timeout); it is logged in one line,
        not as the stderr traceback socketserver prints. Anything else keeps the trace."""
        import sys

        exc = sys.exc_info()[1]
        if isinstance(exc, (ConnectionError, TimeoutError, ssl.SSLError)):
            log.info("[PGWIRE] connection from %s ended: %s", client_address, exc)
            return
        log.error("[PGWIRE] connection from %s failed", client_address, exc_info=True)

    def connection_opened(self, handler=None) -> None:
        with self._active_lock:
            self._active_connections += 1
            self._idle_since = None
            if handler is not None:
                self._handlers.add(handler)

    def terminate_sessions(self, reason: str) -> int:
        """End every open connection (server shutdown). Returns how many there were."""
        with self._active_lock:
            handlers = list(self._handlers)
        for handler in handlers:
            handler.terminate(reason)
        return len(handlers)

    def connection_closed(self, handler=None) -> None:
        with self._active_lock:
            self._handlers.discard(handler)
            self._active_connections -= 1
            if self._active_connections <= 0:
                self._active_connections = 0
                self._idle_since = time.monotonic()

    def idle_seconds(self) -> float | None:
        """Seconds since the connection count last reached zero, or None if not idle."""
        with self._active_lock:
            if self._idle_since is None:
                return None
            return time.monotonic() - self._idle_since


def maybe_start_idle_shutdown_watcher(
    server: "CalciteServer", idle_shutdown_seconds: float | None = None
) -> None:
    """Opt-in: exit the process once the server has had zero live connections for a
    configurable grace period.

    Off by default — a standalone/manually-run pgwire-calcite deployment should keep
    serving indefinitely, exactly like it does today. This exists for the
    spawn-if-absent singleton pattern (askamerica-engine and any other thin client that
    spawns pgwire-govdata on demand, per kenstott/calcite#364): with many short-lived
    client processes connecting and disconnecting, the shared server needs to reap
    itself rather than becoming a background process every user has to notice and
    kill by hand. Enabled by setting PGWIRE_CALCITE_IDLE_SHUTDOWN_SECONDS to a positive
    number of seconds; unset or <= 0 leaves the server running forever, unchanged from
    prior behavior.
    """
    if idle_shutdown_seconds is not None:
        # The launcher's --idle-shutdown-seconds: an explicit setting wins over the
        # environment variable, which stays for hosts that already set it.
        grace = float(idle_shutdown_seconds)
    else:
        raw = os.environ.get("PGWIRE_CALCITE_IDLE_SHUTDOWN_SECONDS", "")
        try:
            grace = float(raw)
        except ValueError:
            grace = 0.0
    if grace <= 0:
        return

    def _watch() -> None:
        # A short poll interval keeps the actual shutdown delay close to `grace`
        # without meaningfully increasing idle CPU use.
        interval = min(5.0, grace / 4) or 1.0
        while True:
            time.sleep(interval)
            idle = server.idle_seconds()
            if idle is not None and idle >= grace:
                log.info(
                    "[PGWIRE] idle for %.0fs (limit %.0fs) with zero connections; exiting",
                    idle, grace,
                )
                try:
                    server.shutdown()
                finally:
                    # os._exit rather than sys.exit: this runs on a daemon thread, and
                    # the embedded Calcite child / backend may hold non-daemon threads
                    # or native resources that would otherwise keep the process alive
                    # indefinitely after an idle server was meant to disappear.
                    os._exit(0)

    t = threading.Thread(target=_watch, name="pgwire-idle-shutdown", daemon=True)
    t.start()
    log.info("[PGWIRE] idle-shutdown watcher armed: %.0fs grace period", grace)


def start_pgwire_server(
    host: str,
    port: int,
    ssl_ctx: ssl.SSLContext | None = None,
    mtls_auth=None,
    sock: socket.socket | None = None,
    idle_shutdown_seconds: float | None = None,
) -> CalciteServer:
    """Start the pgwire server in a daemon thread. Returns the server instance.

    Every backend executes synchronously (embedded JDBC, or a socket round-trip to
    the Calcite child), so there is no event loop to hand in. ``mtls_auth`` is a
    ``pgwire_calcite.mtls.ClientAuth`` (or None) already applied to ``ssl_ctx`` by the
    caller; it is carried onto the server for principal-binding checks post-handshake.

    ``sock``, when given, is an already bound+listening socket from
    ``launcher.claim_listen_socket()`` — see ``CalciteServer.__init__`` for why this
    matters (it's what actually closes the pgwire port-exhaustion bug, not just the
    caller checking it built a backend for nothing).
    """
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
    server = CalciteServer((host, port), conn, ssl_ctx=ssl_ctx, mtls_auth=mtls_auth, sock=sock)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    log.info("[PGWIRE] listening on %s:%d (TLS=%s)", host, port, ssl_ctx is not None)
    maybe_start_idle_shutdown_watcher(server, idle_shutdown_seconds)
    return server
