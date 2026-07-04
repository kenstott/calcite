# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Phase 5b tests: OIDC/Firebase token-as-password auth (PGW-044).

Generates a local RSA keypair, mints RS256 ID tokens, and checks the provider
accepts a valid token and rejects expired / wrong-audience / wrong-issuer / bad-
signature tokens. Verifies end-to-end over the wire (token in the password field).
"""

from __future__ import annotations

import time

import pytest

jwt = pytest.importorskip("jwt")
pytest.importorskip("cryptography")

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from pgwire_calcite import launcher
from pgwire_calcite.auth import OidcProvider

from test_phase0_wire import MiniPgClient, _free_port

ISSUER = "https://issuer.example"
AUDIENCE = "pgwire-calcite"


@pytest.fixture(scope="module")
def keys():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    priv = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    pub = key.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
    ).decode()
    return priv, pub


def _token(priv, **overrides):
    now = int(time.time())
    claims = {
        "iss": ISSUER,
        "aud": AUDIENCE,
        "sub": "alice@example.com",
        "iat": now,
        "exp": now + 300,
    }
    claims.update(overrides)
    return jwt.encode(claims, priv, algorithm="RS256")


def _provider(pub):
    return OidcProvider(issuer=ISSUER, audience=AUDIENCE, public_key=pub)


def test_valid_token_authenticates_as_subject(keys):
    priv, pub = keys
    assert _provider(pub).authenticate("", _token(priv)) == "alice@example.com"


def test_expired_token_rejected(keys):
    priv, pub = keys
    assert _provider(pub).authenticate("", _token(priv, exp=int(time.time()) - 10)) is None


def test_wrong_audience_rejected(keys):
    priv, pub = keys
    assert _provider(pub).authenticate("", _token(priv, aud="someone-else")) is None


def test_wrong_issuer_rejected(keys):
    priv, pub = keys
    assert _provider(pub).authenticate("", _token(priv, iss="https://evil")) is None


def test_bad_signature_rejected(keys):
    priv, pub = keys
    other = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    other_priv = other.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    assert _provider(pub).authenticate("", _token(other_priv)) is None


def test_oidc_over_the_wire(keys):
    priv, pub = keys
    provider = _provider(pub)
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", auth_provider=provider)
    time.sleep(0.1)
    try:
        # token in the password field
        c = MiniPgClient("127.0.0.1", port, user="alice", password=_token(priv))
        try:
            assert c.query("SELECT 1")["rows"] == [["1"]]
        finally:
            c.close()
        # an invalid token is rejected at connect
        with pytest.raises(ConnectionError):
            MiniPgClient("127.0.0.1", port, user="alice", password="not-a-jwt")
    finally:
        srv.shutdown()
