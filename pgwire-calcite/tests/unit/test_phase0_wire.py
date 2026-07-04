# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Phase 0 exit-gate tests: wire handshake, auth, simple query against StubBackend.

Verifies PGW-001 (handshake, server_version >= 14), PGW-003 (simple query),
PGW-004 (session commands accepted), PGW-007 (trust + cleartext auth) without
Calcite in the loop. Uses a raw socket client for the simple query protocol so
the test has no client-library dependency and exercises the wire bytes directly.
"""

from __future__ import annotations

import socket
import ssl
import struct
import threading
import time

import pytest

from pgwire_calcite import launcher


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class MiniPgClient:
    """Minimal PostgreSQL v3 simple-query client (enough for Phase 0 assertions)."""

    def __init__(
        self, host: str, port: int, user: str = "tester", password: str | None = None,
        use_ssl: bool = False,
    ):
        self.sock = socket.create_connection((host, port), timeout=5)
        if use_ssl:
            # PostgreSQL SSLRequest: length 8, code 80877103; server replies 'S' to proceed.
            self.sock.sendall(struct.pack("!ii", 8, 80877103))
            if self.sock.recv(1) != b"S":
                raise ConnectionError("server declined SSL")
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            self.sock = ctx.wrap_socket(self.sock, server_hostname=host)
        self.params: dict[str, str] = {}
        self._startup(user, password)

    def _send(self, msg_type: bytes, body: bytes) -> None:
        if msg_type:
            self.sock.sendall(msg_type + struct.pack("!i", len(body) + 4) + body)
        else:
            self.sock.sendall(struct.pack("!i", len(body) + 4) + body)

    def _recv_exact(self, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("server closed connection")
            buf += chunk
        return buf

    def _recv_msg(self) -> tuple[str, bytes]:
        mtype = self._recv_exact(1).decode("ascii")
        (length,) = struct.unpack("!i", self._recv_exact(4))
        body = self._recv_exact(length - 4)
        return mtype, body

    def _startup(self, user: str, password: str | None) -> None:
        params = {"user": user, "database": "calcite", "client_encoding": "UTF8"}
        body = struct.pack("!i", 196608)
        for k, v in params.items():
            body += k.encode() + b"\x00" + v.encode() + b"\x00"
        body += b"\x00"
        self._send(b"", body)
        # Handle both auth flows: trust mode sends AuthenticationOk (code 0)
        # directly; the 'simple' provider sends AuthenticationCleartextPassword
        # (code 3), to which we reply with the password. Drain until ReadyForQuery.
        while True:
            mtype, payload = self._recv_msg()
            if mtype == "R":
                (code,) = struct.unpack("!i", payload[:4])
                if code == 3:  # cleartext-password challenge
                    self._send(b"p", (password or "").encode() + b"\x00")
                elif code != 0:
                    raise ConnectionError(f"unexpected auth code {code}")
            elif mtype == "S":
                key, _, val = payload.partition(b"\x00")
                self.params[key.decode()] = val.rstrip(b"\x00").decode()
            elif mtype == "E":
                raise ConnectionError("startup error: " + payload.decode(errors="replace"))
            elif mtype == "Z":
                break

    def query(self, sql: str) -> dict:
        """Run a simple Query; return {'columns', 'rows', 'command_tag', 'error'}."""
        self._send(b"Q", sql.encode() + b"\x00")
        columns: list[str] = []
        rows: list[list] = []
        command_tag = None
        error = None
        while True:
            mtype, payload = self._recv_msg()
            if mtype == "T":  # RowDescription
                (nfields,) = struct.unpack("!h", payload[:2])
                off = 2
                columns = []
                for _ in range(nfields):
                    end = payload.index(b"\x00", off)
                    columns.append(payload[off:end].decode())
                    off = end + 1 + 18  # skip null + 18 bytes of field metadata
            elif mtype == "D":  # DataRow
                (ncols,) = struct.unpack("!h", payload[:2])
                off = 2
                row = []
                for _ in range(ncols):
                    (vlen,) = struct.unpack("!i", payload[off : off + 4])
                    off += 4
                    if vlen == -1:
                        row.append(None)
                    else:
                        row.append(payload[off : off + vlen].decode())
                        off += vlen
                rows.append(row)
            elif mtype == "C":  # CommandComplete
                command_tag = payload.rstrip(b"\x00").decode()
            elif mtype == "E":  # ErrorResponse
                error = payload.decode(errors="replace")
            elif mtype == "Z":  # ReadyForQuery
                break
        return {"columns": columns, "rows": rows, "command_tag": command_tag, "error": error}

    def close(self) -> None:
        try:
            self._send(b"X", b"")
        except OSError:
            pass
        self.sock.close()


@pytest.fixture()
def trust_server():
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none")
    time.sleep(0.1)
    yield "127.0.0.1", port
    srv.shutdown()


@pytest.fixture()
def simple_auth_server():
    port = _free_port()
    srv = launcher.serve(
        host="127.0.0.1", port=port, auth="simple", users={"alice": "s3cret"}
    )
    time.sleep(0.1)
    yield "127.0.0.1", port
    srv.shutdown()


def test_handshake_reports_server_version_ge_14(trust_server):
    host, port = trust_server
    c = MiniPgClient(host, port)
    try:
        assert "server_version" in c.params
        major = int(c.params["server_version"].split(".")[0])
        assert major >= 14, f"server_version {c.params['server_version']} must be >= 14 (PGW-001)"
    finally:
        c.close()


def test_select_one_simple_query(trust_server):
    host, port = trust_server
    c = MiniPgClient(host, port)
    try:
        r = c.query("SELECT 1")
        assert r["error"] is None, r["error"]
        assert r["rows"] == [["1"]], r
        assert r["command_tag"] == "SELECT 1", r["command_tag"]
    finally:
        c.close()


def test_select_version(trust_server):
    host, port = trust_server
    c = MiniPgClient(host, port)
    try:
        r = c.query("SELECT version()")
        assert r["error"] is None, r["error"]
        assert r["rows"] and r["rows"][0][0].startswith("PostgreSQL"), r
    finally:
        c.close()


def test_session_commands_accepted(trust_server):
    host, port = trust_server
    c = MiniPgClient(host, port)
    try:
        for sql, tag in [
            ("SET client_encoding = 'UTF8'", "SET"),
            ("BEGIN", "BEGIN"),
            ("COMMIT", "COMMIT"),
            ("DISCARD ALL", "DISCARD"),
        ]:
            r = c.query(sql)
            assert r["error"] is None, (sql, r["error"])
            assert r["command_tag"] == tag, (sql, r["command_tag"])
    finally:
        c.close()


def test_unsupported_query_fails_loud_not_silent(trust_server):
    host, port = trust_server
    c = MiniPgClient(host, port)
    try:
        r = c.query("SELECT * FROM some_table")
        assert r["error"] is not None, "StubBackend must reject, not silently return empty"
        assert "Phase 1" in r["error"]
    finally:
        c.close()


def test_simple_auth_accepts_correct_password(simple_auth_server):
    host, port = simple_auth_server
    c = MiniPgClient(host, port, user="alice", password="s3cret")
    try:
        assert c.query("SELECT 1")["rows"] == [["1"]]
    finally:
        c.close()


def test_simple_auth_rejects_wrong_password(simple_auth_server):
    host, port = simple_auth_server
    with pytest.raises(ConnectionError) as exc:
        MiniPgClient(host, port, user="alice", password="wrong")
    assert "28P01" in str(exc.value) or "authentication failed" in str(exc.value)
