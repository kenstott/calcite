# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Calcite JVM sidecar + bridge (PGW-033 topology, PGW-037 lifecycle decoupling).

Runs Calcite in a SEPARATE recyclable child process (its own heap, its own JVM),
never in the pgwire/supervisor process. The pgwire server talks to it through
``BridgeBackend`` over a local socket that carries Arrow IPC:

    pgwire (Python) --[Calcite SQL]--> Calcite child (JPype+JVM)
    pgwire (Python) <--[Arrow IPC batches]-- Calcite child

Transpile (PG->Calcite, D4) stays on the pgwire side in ``BridgeBackend`` so
PG-only rejects (PGW-018) happen before the child is touched; the child is a pure
Arrow execution service. ``BridgeBackend`` dials the child per query, so
recycling Calcite (Phase-5 supervisor) fails only an in-flight query — idle
pgwire sessions are untouched and the next query connects to the fresh child
(PGW-037). ``ready()`` readiness-gates on the child socket.

Frame protocol (length-prefixed, big-endian u32; len 0 == empty/terminator frame).
A request is one frame; the first byte distinguishes control requests (all start
with NUL, which SQL never does) from a plain SQL execution request:

  request
    empty frame                          liveness ping (does not touch Calcite)
    CATALOG_REQUEST                      ship the catalog model
    CANCEL_REQUEST + JSON                cancel another connection's statement:
                                         {"session_key": ..., "reason": ...}
    EXEC_REQUEST + JSON                  execute: {"sql", "session_key", "timeout_ms"}

  response
    1 status byte (0 ok / 1 err), then
      err: one frame = JSON {"sqlstate": str|null, "message": str}
      ok:  header frame = JSON (per request kind: {names,labels} for exec and
           ping, the catalog model, or {"cancelled": bool}), then for an exec
           request N Arrow-IPC batch frames, a 0-length terminator frame, and a
           trailer frame = JSON {"error": null | {"sqlstate", "message"}}.

The trailer is what makes cancellation work over the bridge: the child streams
batches before it knows whether the statement will finish, so a failure that
lands mid-stream (a CancelRequest from another connection, or the session's
``statement_timeout`` firing in the child's watchdog) has to be reported *after*
rows, with its SQLSTATE intact — 57014 reaches the client as a cancel, not as a
truncated result or a socket error.
"""

from __future__ import annotations

import json
import logging
import socket
import socketserver
import struct
import time
from typing import List, Optional

from pgwire_calcite import arrow_bridge
from pgwire_calcite.backend import PgProtocolError, QueryCanceled
from pgwire_calcite.dialect import transpile_pg_to_calcite
from pgwire_calcite.types import QueryResult

log = logging.getLogger(__name__)

_STATUS_OK = b"\x00"
_STATUS_ERR = b"\x01"

#: Reserved request that asks the child for its catalog model (not real SQL).
CATALOG_REQUEST = "\x00__PGWIRE_CATALOG__"
#: Reserved request that cancels another connection's in-flight statement (PGW-050).
CANCEL_REQUEST = "\x00__PGWIRE_CANCEL__"
#: Reserved prefix for an execution request carrying its session key and timeout.
EXEC_REQUEST = "\x00__PGWIRE_EXEC__"

#: Extra seconds allowed on the socket read deadline beyond the session's
#: statement_timeout: the child enforces the timeout itself and then still has to
#: send the trailer, so the reader must outlive the cancel it asked for.
_TIMEOUT_GRACE_S = 30.0


def _read_exact(reader, n: int) -> Optional[bytes]:
    buf = b""
    while len(buf) < n:
        chunk = reader.read(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


def write_frame(writer, data: bytes) -> None:
    writer.write(struct.pack("!I", len(data)))
    if data:
        writer.write(data)


def read_frame(reader) -> Optional[bytes]:
    hdr = _read_exact(reader, 4)
    if hdr is None:
        return None
    (n,) = struct.unpack("!I", hdr)
    if n == 0:
        return b""
    return _read_exact(reader, n)


def _error_payload(exc: BaseException) -> bytes:
    """Serialize an exception with its SQLSTATE when it has one."""
    sqlstate = exc.sqlstate if isinstance(exc, PgProtocolError) else None
    return json.dumps({"sqlstate": sqlstate, "message": str(exc)}).encode("utf-8")


def _raise_from_payload(raw: bytes) -> None:
    """Re-raise on the pgwire side what the child reported, SQLSTATE preserved."""
    err = json.loads(raw.decode("utf-8"))
    sqlstate, message = err.get("sqlstate"), err.get("message", "")
    if sqlstate == "57014":
        raise QueryCanceled(message)
    if sqlstate:
        raise PgProtocolError(sqlstate, message)
    raise RuntimeError("calcite: " + message)


# --- Calcite child server -----------------------------------------------------


class _ChildHandler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        backend = self.server.backend  # type: ignore[attr-defined]
        while True:
            req = read_frame(self.rfile)
            if req is None:
                break  # client closed
            request = req.decode("utf-8")
            try:
                if request == "":  # liveness ping — do not touch Calcite
                    self._send_control(b'{"names": [], "labels": []}')
                elif request == CATALOG_REQUEST:  # PGW-012 over the bridge
                    self._send_control(self._catalog_payload(backend))
                elif request.startswith(CANCEL_REQUEST):
                    body = json.loads(request[len(CANCEL_REQUEST):] or "{}")
                    self._send_control(self._cancel_payload(body))
                elif request.startswith(EXEC_REQUEST):
                    body = json.loads(request[len(EXEC_REQUEST):])
                    self._execute(backend, body)
                else:
                    raise ValueError(
                        "unknown bridge request: the pgwire side must send a ping, "
                        f"{CATALOG_REQUEST!r}, {CANCEL_REQUEST!r} or {EXEC_REQUEST!r}"
                    )
            except Exception as exc:  # pre-stream failure -> loud, framed, with SQLSTATE
                log.warning("[CALCITE-CHILD] error: %s", exc)
                self.wfile.write(_STATUS_ERR)
                write_frame(self.wfile, _error_payload(exc))
                self.wfile.flush()

    def _send_control(self, payload: bytes) -> None:
        self.wfile.write(_STATUS_OK)
        write_frame(self.wfile, payload)
        write_frame(self.wfile, b"")
        self.wfile.flush()

    @staticmethod
    def _catalog_payload(backend) -> bytes:
        from pgwire_calcite.catalog_populate import build_context, serialize_catalog

        ctx, column_types = build_context(backend.connection)
        return json.dumps(serialize_catalog(ctx, column_types)).encode("utf-8")

    @staticmethod
    def _cancel_payload(body: dict) -> bytes:
        """Cancel the statement the named session is running in THIS process.

        Arrives on its own connection (the child server is threaded), so it is not
        blocked behind the statement it cancels — the same shape as PG's
        CancelRequest, and the same registry the in-process backend uses.
        """
        from pgwire_calcite.calcite_backend import IN_FLIGHT

        cancelled = IN_FLIGHT.cancel(body["session_key"], body["reason"])
        return json.dumps({"cancelled": cancelled}).encode("utf-8")

    def _execute(self, backend, body: dict) -> None:
        from pgwire_calcite.calcite_backend import CancelScope

        scope = CancelScope(body["session_key"], body["timeout_ms"])
        names, labels, ipc = arrow_bridge.stream_ipc_batches(
            backend.connection, backend._lock, body["sql"], cancel_scope=scope
        )
        self.wfile.write(_STATUS_OK)
        write_frame(self.wfile, json.dumps({"names": names, "labels": labels}).encode("utf-8"))
        self.wfile.flush()
        failure: Optional[BaseException] = None
        try:
            for batch in ipc:
                write_frame(self.wfile, batch)
        except Exception as exc:
            # Mid-stream failure (cancel, statement_timeout, engine error): the client
            # is already reading batches, so it is reported in the trailer, not as a
            # status byte that would be misread as a frame length.
            log.info("[CALCITE-CHILD] stream ended early: %s", exc)
            failure = exc
        write_frame(self.wfile, b"")  # terminator
        write_frame(
            self.wfile,
            json.dumps(
                {"error": None if failure is None else json.loads(_error_payload(failure))}
            ).encode("utf-8"),
        )
        self.wfile.flush()


class CalciteChildServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, server_address, backend) -> None:
        super().__init__(server_address, _ChildHandler)
        self.backend = backend


def serve_calcite_child(backend, host: str = "127.0.0.1", port: int = 5533):
    """Start the Calcite child socket server in a daemon thread; return the server."""
    import threading

    srv = CalciteChildServer((host, port), backend)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    log.info("[CALCITE-CHILD] listening on %s:%d", host, port)
    return srv


# --- Bridge backend (pgwire side) --------------------------------------------


class BridgeBackend:
    """Backend that executes via a Calcite child over the socket bridge."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 5533,
        connect_timeout: float = 10.0,
        connect_retries: int = 3,
        connect_backoff: float = 0.2,
        extensions=None,
    ) -> None:
        self._host = host
        self._port = port
        self._extensions = set(extensions or ())
        self._connect_timeout = connect_timeout
        self._retries = connect_retries
        self._backoff = connect_backoff

    @property
    def extensions(self) -> frozenset:
        """Enabled extension surfaces — what pg_extension advertises (PGW-046)."""
        return frozenset(self._extensions)

    def _connect(self) -> socket.socket:
        return socket.create_connection((self._host, self._port), timeout=self._connect_timeout)

    def _connect_with_retry(self) -> socket.socket:
        # Reconnect across a Calcite recycle: the child may be briefly down.
        last: Optional[Exception] = None
        for attempt in range(self._retries):
            try:
                return self._connect()
            except OSError as exc:
                last = exc
                time.sleep(self._backoff * (2 ** attempt))
        raise ConnectionError(f"Calcite child unavailable at {self._host}:{self._port}: {last}")

    def ready(self) -> bool:
        try:
            sock = self._connect()
        except OSError:
            return False
        try:
            w, r = sock.makefile("wb"), sock.makefile("rb")
            write_frame(w, b"")  # ping
            w.flush()
            status = _read_exact(r, 1)
            read_frame(r)  # header
            read_frame(r)  # terminator
            return status == _STATUS_OK
        except OSError:
            return False
        finally:
            sock.close()

    def fetch_catalog(self):
        """Ask the Calcite child for its catalog model (PGW-012 over the bridge).

        Returns (CompilationContext, column_types) that the launcher installs onto
        the server state so discovery works in the sidecar topology.
        """
        from pgwire_calcite.catalog_populate import deserialize_catalog

        data = self._control_request(CATALOG_REQUEST)
        return deserialize_catalog(json.loads(data.decode("utf-8")))

    def cancel_session(self, session_key: str, reason: str) -> bool:
        """Cancel ``session_key``'s in-flight statement inside the Calcite child.

        The statement lives in the child process, so the CancelRequest has to cross
        the bridge on its own connection — the query's connection is busy streaming
        (PGW-050).
        """
        payload = CANCEL_REQUEST + json.dumps({"session_key": session_key, "reason": reason})
        answer = self._control_request(payload)
        return bool(json.loads(answer.decode("utf-8"))["cancelled"])

    def discard_session(self, session_key: str) -> None:
        """Session teardown / DISCARD ALL (PGW-052).

        Nothing to drop on this side: the child registers a session only while it is
        executing, and a session cannot issue DISCARD ALL while its own statement
        runs, so the entry is already gone.
        """
        del session_key

    def _control_request(self, request: str) -> bytes:
        sock = self._connect_with_retry()
        w, r = sock.makefile("wb"), sock.makefile("rb")
        try:
            write_frame(w, request.encode("utf-8"))
            w.flush()
            status = _read_exact(r, 1)
            if status is None:
                raise ConnectionError("Calcite child closed the connection")
            if status == _STATUS_ERR:
                _raise_from_payload(read_frame(r) or b'{"message": ""}')
            data = read_frame(r) or b"{}"
            read_frame(r)  # terminator
            return data
        finally:
            sock.close()

    def execute_sql(
        self,
        sql: str,
        role_id: str,
        params: Optional[list] = None,
        stream: bool = False,
        session_key: Optional[str] = None,
        timeout_ms: int = 0,
    ) -> QueryResult:
        del role_id, params, stream  # params substituted upstream; always streams
        # PG-only rejects happen here (PGW-018); JSON/vector surfaces honored.
        calcite_sql = transpile_pg_to_calcite(
            sql,
            json_enabled=("json" in self._extensions),
            vector_enabled=("vector" in self._extensions),
        )
        sock = self._connect_with_retry()
        # The child owns the statement and enforces the timeout, so this side reads
        # without a deadline unless the session set one — a long gap between batches
        # is a slow query, not a dead socket (PGW-051).
        sock.settimeout(timeout_ms / 1000.0 + _TIMEOUT_GRACE_S if timeout_ms else None)
        w, r = sock.makefile("wb"), sock.makefile("rb")
        request = EXEC_REQUEST + json.dumps(
            {"sql": calcite_sql, "session_key": session_key, "timeout_ms": int(timeout_ms)}
        )
        write_frame(w, request.encode("utf-8"))
        w.flush()

        status = _read_exact(r, 1)
        if status is None:
            sock.close()
            raise ConnectionError("Calcite child closed the connection")
        if status == _STATUS_ERR:
            payload = read_frame(r) or b'{"message": ""}'
            sock.close()
            _raise_from_payload(payload)

        header = read_frame(r) or b"{}"
        h = json.loads(header.decode("utf-8"))
        names: List[str] = h.get("names", [])
        labels: List[str] = h.get("labels", [])

        def _ipc_iter():
            while True:
                b = read_frame(r)
                if b is None or b == b"":
                    break
                yield b
            # Terminator reached: the trailer says whether the stream completed or
            # was cut short (cancel / statement_timeout / engine error).
            trailer = read_frame(r)
            if trailer is None:
                raise ConnectionError("Calcite child closed the connection before the trailer")
            err = json.loads(trailer.decode("utf-8")).get("error")
            if err is not None:
                _raise_from_payload(json.dumps(err).encode("utf-8"))

        def _batches():
            try:
                yield from arrow_bridge.batches_from_ipc(_ipc_iter())
            finally:
                sock.close()

        return QueryResult(column_names=names, column_types=labels, row_batches=_batches())
