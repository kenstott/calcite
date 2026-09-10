# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Phase 3 hardening: brute-force throttling, provider-unavailable answers, the PAT/bearer
one-decision rule, opt-in mTLS client-certificate binding, and the no-silent-fallback fix
in authz.py.
"""

from __future__ import annotations

import datetime
import socket
import ssl
import struct
import time

import pytest

from pgwire_calcite import launcher
from pgwire_calcite.auth import (
    AccountStore,
    LocalAccountsProvider,
    OidcProvider,
    PAT_PREFIX,
    TrustProvider,
    is_personal_access_token,
)
from pgwire_calcite.authz import RoleGrants, enforce_query
from pgwire_calcite.server import _authenticate_credential
from pgwire_calcite.throttle import LockedOut, login_throttle, reset_login_throttle, subject_key

from test_phase0_wire import MiniPgClient, _free_port

pytest.importorskip("cryptography")

from cryptography import x509  # noqa: E402
from cryptography.hazmat.primitives import hashes, serialization  # noqa: E402
from cryptography.hazmat.primitives.asymmetric import rsa  # noqa: E402
from cryptography.x509.oid import NameOID  # noqa: E402


@pytest.fixture(autouse=True)
def _isolated_throttle():
    reset_login_throttle()
    yield
    reset_login_throttle()


# --- Item 1: brute-force throttling and lockout ------------------------------


def test_throttle_locks_out_after_max_attempts(monkeypatch, tmp_path):
    store = AccountStore(tmp_path / "accounts.json")
    store.add("alice", "s3cret")
    provider = LocalAccountsProvider(store)
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", auth_provider=provider)
    time.sleep(0.1)
    try:
        for _ in range(login_throttle().max_attempts):
            with pytest.raises(ConnectionError):
                MiniPgClient("127.0.0.1", port, user="alice", password="WRONG")
        # The account is now locked; even the CORRECT password is refused, and the
        # error is distinguishable from a bad password (28000, not 28P01).
        with pytest.raises(ConnectionError) as exc:
            MiniPgClient("127.0.0.1", port, user="alice", password="s3cret")
        assert "28000" in str(exc.value)
    finally:
        srv.shutdown()


def test_throttle_keyed_per_subject(tmp_path):
    store = AccountStore(tmp_path / "accounts.json")
    store.add("alice", "s3cret")
    store.add("bob", "hunter2")
    provider = LocalAccountsProvider(store)
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", auth_provider=provider)
    time.sleep(0.1)
    try:
        for _ in range(login_throttle().max_attempts):
            with pytest.raises(ConnectionError):
                MiniPgClient("127.0.0.1", port, user="alice", password="WRONG")
        # bob is untouched by alice's lockout
        c = MiniPgClient("127.0.0.1", port, user="bob", password="hunter2")
        c.close()
    finally:
        srv.shutdown()


def test_throttle_check_and_record_directly():
    throttle = login_throttle()
    key = subject_key("carol")
    for _ in range(throttle.max_attempts):
        throttle.check(key)
        throttle.record_failure(key)
    with pytest.raises(LockedOut) as exc:
        throttle.check(key)
    assert exc.value.retry_after > 0
    # A correct credential elsewhere clears an unrelated subject, not this one.
    throttle.record_success(subject_key("dave"))
    with pytest.raises(LockedOut):
        throttle.check(key)


# --- Item 2: provider-construction/config failure answered on the wire ------


def test_oidc_missing_key_source_raises_value_error_not_swallowed():
    # No public_key/jwks/jwks_url configured at all: this is a provider misconfiguration,
    # not a judgment about the presented credential, so it must not come back as None.
    provider = OidcProvider(issuer="https://issuer.example", audience="aud")
    with pytest.raises(ValueError):
        provider.authenticate("alice", "not-a-real-token")


def test_oidc_provider_unavailable_answered_as_fatal_on_wire():
    provider = OidcProvider(issuer="https://issuer.example", audience="aud")
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", auth_provider=provider)
    time.sleep(0.1)
    try:
        with pytest.raises(ConnectionError) as exc:
            MiniPgClient("127.0.0.1", port, user="alice", password="anything")
        assert "28P01" in str(exc.value) and "provider unavailable" in str(exc.value)
    finally:
        srv.shutdown()


def test_malformed_token_is_a_rejected_credential_not_a_crash():
    provider = OidcProvider(issuer="https://issuer.example", audience="aud", public_key="x")
    # A public_key IS configured, so key resolution succeeds; decode then fails on the
    # garbage token — that is a bad credential (None), never an unhandled exception.
    assert provider.authenticate("alice", "not-a-jwt") is None


# --- Item 3: PAT/bearer vs. basic-password, decided once, never retried -----


def test_pat_prefix_recognized():
    assert is_personal_access_token(PAT_PREFIX + "abc123")
    assert not is_personal_access_token("hunter2")


def test_pat_shaped_secret_rejected_outright_by_non_bearer_provider(tmp_path):
    store = AccountStore(tmp_path / "accounts.json")
    # The PAT-shaped string happens to also be a valid stored password; a non-bearer
    # provider must still refuse it as a bearer credential rather than retry it as one.
    store.add("alice", PAT_PREFIX + "abc123")
    provider = LocalAccountsProvider(store)
    assert _authenticate_credential(provider, "alice", PAT_PREFIX + "abc123") is None


def test_oidc_provider_always_treated_as_bearer():
    assert OidcProvider(issuer="i", audience="a").accepts_bearer is True
    assert TrustProvider().accepts_bearer is False


# --- Item 4: mTLS client-certificate principal binding ----------------------


def _ca(tmp_path):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test-ca")])
    now = datetime.datetime(2026, 1, 1)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name).issuer_name(name).public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now).not_valid_after(now + datetime.timedelta(days=3650))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    ca_path = tmp_path / "ca.pem"
    ca_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    return key, cert, str(ca_path)


def _leaf(tmp_path, name_stem, common_name, ca_key, ca_cert):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    now = datetime.datetime(2026, 1, 1)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name).issuer_name(ca_cert.subject).public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now).not_valid_after(now + datetime.timedelta(days=3650))
        .sign(ca_key, hashes.SHA256())
    )
    cert_path = tmp_path / f"{name_stem}-cert.pem"
    key_path = tmp_path / f"{name_stem}-key.pem"
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    )
    return str(cert_path), str(key_path)


def _connect_with_client_cert(host, port, user, client_cert, client_key):
    """Raw v3 startup, TLS handshake presenting a client certificate, drain to ReadyForQuery."""
    sock = socket.create_connection((host, port), timeout=5)
    sock.sendall(struct.pack("!ii", 8, 80877103))
    if sock.recv(1) != b"S":
        raise ConnectionError("server declined SSL")
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    ctx.load_cert_chain(certfile=client_cert, keyfile=client_key)
    tls = ctx.wrap_socket(sock, server_hostname=host)

    def send(msg_type: bytes, body: bytes) -> None:
        tls.sendall((msg_type + struct.pack("!i", len(body) + 4) + body) if msg_type else
                    (struct.pack("!i", len(body) + 4) + body))

    def recv_exact(n):
        buf = b""
        while len(buf) < n:
            chunk = tls.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("server closed connection")
            buf += chunk
        return buf

    def recv_msg():
        mtype = recv_exact(1).decode("ascii")
        (length,) = struct.unpack("!i", recv_exact(4))
        return mtype, recv_exact(length - 4)

    params = {"user": user, "database": "calcite", "client_encoding": "UTF8"}
    body = struct.pack("!i", 196608)
    for k, v in params.items():
        body += k.encode() + b"\x00" + v.encode() + b"\x00"
    body += b"\x00"
    send(b"", body)
    while True:
        mtype, payload = recv_msg()
        if mtype == "R":
            (code,) = struct.unpack("!i", payload[:4])
            if code != 0:
                raise ConnectionError(f"unexpected auth code {code}")
        elif mtype == "E":
            raise ConnectionError("startup error: " + payload.decode(errors="replace"))
        elif mtype == "Z":
            break
    tls.close()


@pytest.fixture()
def ca_and_client_certs(tmp_path):
    ca_key, ca_cert, ca_path = _ca(tmp_path)
    server_cert, server_key = _leaf(tmp_path, "server", "localhost", ca_key, ca_cert)
    client_cert, client_key = _leaf(tmp_path, "alice", "alice", ca_key, ca_cert)
    return {
        "ca_path": ca_path,
        "server_cert": server_cert,
        "server_key": server_key,
        "client_cert": client_cert,
        "client_key": client_key,
    }


def test_mtls_bind_principal_accepts_matching_common_name(ca_and_client_certs):
    c = ca_and_client_certs
    port = _free_port()
    srv = launcher.serve(
        host="127.0.0.1", port=port, auth="none",
        certfile=c["server_cert"], keyfile=c["server_key"],
        client_ca=c["ca_path"], mtls_mode="required", mtls_bind_principal=True,
    )
    time.sleep(0.1)
    try:
        _connect_with_client_cert(
            "127.0.0.1", port, "alice", c["client_cert"], c["client_key"]
        )
    finally:
        srv.shutdown()


def test_mtls_bind_principal_rejects_mismatched_common_name(ca_and_client_certs):
    c = ca_and_client_certs
    port = _free_port()
    srv = launcher.serve(
        host="127.0.0.1", port=port, auth="none",
        certfile=c["server_cert"], keyfile=c["server_key"],
        client_ca=c["ca_path"], mtls_mode="required", mtls_bind_principal=True,
    )
    time.sleep(0.1)
    try:
        with pytest.raises(ConnectionError):
            # cert names "alice"; connecting as "mallory" must be refused.
            _connect_with_client_cert(
                "127.0.0.1", port, "mallory", c["client_cert"], c["client_key"]
            )
    finally:
        srv.shutdown()


def test_mtls_required_rejects_no_client_cert(ca_and_client_certs):
    c = ca_and_client_certs
    port = _free_port()
    srv = launcher.serve(
        host="127.0.0.1", port=port, auth="none",
        certfile=c["server_cert"], keyfile=c["server_key"],
        client_ca=c["ca_path"], mtls_mode="required",
    )
    time.sleep(0.1)
    try:
        with pytest.raises((ConnectionError, ssl.SSLError, OSError)):
            MiniPgClient("127.0.0.1", port, use_ssl=True)
    finally:
        srv.shutdown()


def test_mtls_client_ca_without_server_cert_refuses_to_start(tmp_path):
    ca_key, ca_cert, ca_path = _ca(tmp_path)
    with pytest.raises(ValueError):
        launcher.serve(host="127.0.0.1", port=_free_port(), auth="none", client_ca=ca_path)


# --- Item 5: no silent fallback in authz ------------------------------------


def test_authz_enforce_query_denies_unparsable_sql():
    grants = RoleGrants.from_dict({"analyst": {"*"}})
    with pytest.raises(PermissionError):
        enforce_query(grants, "analyst", "SELECT * FROM WHERE ((( not valid sql")
