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

Frame protocol (length-prefixed, big-endian u32; len 0 == empty/terminator frame):
- request:  one frame = UTF-8 Calcite SQL  (empty frame == liveness ping)
- response: 1 status byte (0 ok / 1 err), then
    ok:  header frame = JSON {names, labels}, then N Arrow-IPC batch frames, then
         a 0-length terminator frame
    err: one frame = UTF-8 message
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
from pgwire_calcite.dialect import transpile_pg_to_calcite
from pgwire_calcite.types import QueryResult

log = logging.getLogger(__name__)

_STATUS_OK = b"\x00"
_STATUS_ERR = b"\x01"


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


# --- Calcite child server -----------------------------------------------------


class _ChildHandler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        backend = self.server.backend  # type: ignore[attr-defined]
        while True:
            req = read_frame(self.rfile)
            if req is None:
                break  # client closed
            sql = req.decode("utf-8")
            try:
                if sql == "":  # liveness ping — do not touch Calcite
                    self.wfile.write(_STATUS_OK)
                    write_frame(self.wfile, b'{"names": [], "labels": []}')
                    write_frame(self.wfile, b"")
                    self.wfile.flush()
                    continue
                names, labels, ipc = arrow_bridge.stream_ipc_batches(
                    backend.connection, backend._lock, sql
                )
                self.wfile.write(_STATUS_OK)
                write_frame(self.wfile, json.dumps({"names": names, "labels": labels}).encode())
                for batch in ipc:
                    write_frame(self.wfile, batch)
                write_frame(self.wfile, b"")  # terminator
                self.wfile.flush()
            except Exception as exc:  # execution error -> loud, framed
                log.warning("[CALCITE-CHILD] error: %s", exc)
                self.wfile.write(_STATUS_ERR)
                write_frame(self.wfile, str(exc).encode("utf-8"))
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
        timeout: float = 120.0,
        connect_retries: int = 3,
        connect_backoff: float = 0.2,
    ) -> None:
        self._host = host
        self._port = port
        self._timeout = timeout
        self._retries = connect_retries
        self._backoff = connect_backoff

    def _connect(self) -> socket.socket:
        return socket.create_connection((self._host, self._port), timeout=self._timeout)

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

    def execute_sql(
        self, sql: str, role_id: str, params: Optional[list] = None, stream: bool = False
    ) -> QueryResult:
        del role_id, params, stream  # params substituted upstream; always streams
        calcite_sql = transpile_pg_to_calcite(sql)  # PG-only rejects happen here (PGW-018)
        sock = self._connect_with_retry()
        w, r = sock.makefile("wb"), sock.makefile("rb")
        write_frame(w, calcite_sql.encode("utf-8"))
        w.flush()

        status = _read_exact(r, 1)
        if status is None:
            sock.close()
            raise ConnectionError("Calcite child closed the connection")
        if status == _STATUS_ERR:
            msg = read_frame(r) or b""
            sock.close()
            raise RuntimeError("calcite: " + msg.decode("utf-8", "replace"))

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

        def _rows():
            try:
                for row in arrow_bridge.rows_from_ipc(_ipc_iter()):
                    yield row
            finally:
                sock.close()

        return QueryResult(rows=_rows(), column_names=names, column_types=labels)
