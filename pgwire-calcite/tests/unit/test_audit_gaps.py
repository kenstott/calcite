# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Coverage for audit-identified gaps on existing code:
- PGW-007  TLS connection (cert/key).
- PGW-003  multi-statement simple query.
- PGW-002/023  extended-protocol portal suspension (Execute row-limit).
- PGW-022  a client disconnect mid-request does not take the server down.
"""

from __future__ import annotations

import datetime
import socket
import struct
import time

import pytest

from pgwire_calcite import launcher
from pgwire_calcite.backend import StubBackend

from test_phase0_wire import MiniPgClient, _free_port


# --- PGW-025: OS-agnostic Java launcher entry point --------------------------

def test_java_launcher_starts_python(tmp_path):
    import pathlib
    import shutil
    import subprocess
    import sys

    if shutil.which("java") is None:
        pytest.skip("java not available")
    jar = pathlib.Path(__file__).resolve().parents[2] / "packaging" / "pgwire-launcher.jar"
    if not jar.exists():
        pytest.skip("pgwire-launcher.jar not built (run packaging/build-launcher.sh)")
    out = subprocess.run(
        ["java", f"-Dpgwire.python={sys.executable}", "-jar", str(jar), "--help"],
        capture_output=True, text=True, timeout=30,
    )
    assert out.returncode == 0, out.stderr
    assert "pgwire-calcite" in out.stdout  # the Java launcher exec'd the Python launcher


# --- PGW-007: TLS ------------------------------------------------------------

def _self_signed_cert(tmp_path):
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    now = datetime.datetime(2026, 1, 1)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name).issuer_name(name).public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now).not_valid_after(now + datetime.timedelta(days=3650))
        .sign(key, hashes.SHA256())
    )
    cert_path = tmp_path / "cert.pem"
    key_path = tmp_path / "key.pem"
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    )
    return str(cert_path), str(key_path)


def test_tls_connection(tmp_path):
    pytest.importorskip("cryptography")
    certfile, keyfile = _self_signed_cert(tmp_path)
    port = _free_port()
    srv = launcher.serve(
        host="127.0.0.1", port=port, auth="none", backend=StubBackend(),
        certfile=certfile, keyfile=keyfile,
    )
    time.sleep(0.1)
    try:
        c = MiniPgClient("127.0.0.1", port, use_ssl=True)
        try:
            assert c.query("SELECT 1")["rows"] == [["1"]]
        finally:
            c.close()
    finally:
        srv.shutdown()


# --- PGW-003: multi-statement simple query -----------------------------------

def test_multi_statement_simple_query():
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", backend=StubBackend())
    time.sleep(0.1)
    try:
        c = MiniPgClient("127.0.0.1", port)
        try:
            c._send(b"Q", b"SELECT 1; SELECT 2\x00")
            tags, rows = [], []
            while True:
                mt, pl = c._recv_msg()
                if mt == "D":
                    (n,) = struct.unpack("!h", pl[:2])
                    off = 2
                    (vlen,) = struct.unpack("!i", pl[off : off + 4])
                    rows.append(pl[off + 4 : off + 4 + vlen].decode())
                elif mt == "C":
                    tags.append(pl.rstrip(b"\x00").decode())
                elif mt == "Z":
                    break
            assert rows == ["1", "2"], rows
            assert len(tags) == 2, tags  # two CommandComplete, one per statement
        finally:
            c.close()
    finally:
        srv.shutdown()


# --- PGW-002/023: extended-protocol portal suspension ------------------------

def test_portal_suspension_row_limit(calcite_backend):
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", backend=calcite_backend)
    time.sleep(0.1)
    try:
        c = MiniPgClient("127.0.0.1", port)
        try:
            sql = b"SELECT ename FROM EMPS ORDER BY ename\x00"
            c._send(b"P", b"\x00" + sql + struct.pack("!h", 0))  # Parse
            c._send(b"B", b"\x00\x00" + struct.pack("!hhh", 0, 0, 0))  # Bind
            c._send(b"E", b"\x00" + struct.pack("!i", 3))  # Execute, max_rows=3
            c._send(b"S", b"")  # Sync
            data, suspended = 0, False
            while True:
                mt, pl = c._recv_msg()
                if mt == "D":
                    data += 1
                elif mt == "s":  # PortalSuspended
                    suspended = True
                elif mt == "Z":
                    break
                elif mt == "E":
                    raise AssertionError("error: " + pl.decode(errors="replace"))
            assert data == 3, f"expected 3 rows before suspend, got {data}"
            assert suspended, "expected PortalSuspended after the row limit"
        finally:
            c.close()
    finally:
        srv.shutdown()


# --- PGW-022: client disconnect does not take the server down ----------------

def test_client_disconnect_mid_request_survives():
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", backend=StubBackend())
    time.sleep(0.1)
    try:
        # connect, send a partial/again-usable request, then abruptly drop the socket
        rogue = socket.create_connection(("127.0.0.1", port), timeout=5)
        rogue.sendall(struct.pack("!ii", 8, 80877103))  # SSLRequest
        rogue.recv(1)
        rogue.close()  # abrupt disconnect mid-handshake
        # the server must still serve a fresh, well-behaved client
        c = MiniPgClient("127.0.0.1", port)
        try:
            assert c.query("SELECT 1")["rows"] == [["1"]]
        finally:
            c.close()
    finally:
        srv.shutdown()
