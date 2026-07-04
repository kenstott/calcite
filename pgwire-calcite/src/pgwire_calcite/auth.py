# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Pluggable authentication providers (PGW-042/043).

One provider interface, selected at launch. ``trust`` (localhost default, no
password) and ``local`` (persisted accounts with SCRAM-SHA-256 verifiers at rest)
are implemented here with no external dependencies. The ``oidc`` provider
(token-as-password, PGW-044) and per-role authorization (PGW-045) are separate
follow-ons noted in the plan.

The ``local`` provider stores only a SCRAM-SHA-256 verifier (RFC 5802): a random
salt, an iteration count, and the derived StoredKey/ServerKey — never the
password. Today the wire still presents the password (cleartext over TLS, or on
localhost) and the provider verifies it against the stored verifier via PBKDF2;
the full SASL SCRAM wire exchange is the remaining slice of PGW-043.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import pathlib
from dataclasses import dataclass
from typing import Dict, Optional

_SCRAM_HASH = "sha256"
_DEFAULT_ITERATIONS = 4096


@dataclass(frozen=True)
class ScramVerifier:
    """RFC 5802 verifier: what is safe to store at rest for a password."""

    salt: bytes
    iterations: int
    stored_key: bytes
    server_key: bytes

    @staticmethod
    def create(password: str, iterations: int = _DEFAULT_ITERATIONS, salt: Optional[bytes] = None) -> "ScramVerifier":
        if salt is None:
            salt = os.urandom(16)
        salted = hashlib.pbkdf2_hmac(_SCRAM_HASH, password.encode("utf-8"), salt, iterations)
        client_key = hmac.new(salted, b"Client Key", _SCRAM_HASH).digest()
        stored_key = hashlib.new(_SCRAM_HASH, client_key).digest()
        server_key = hmac.new(salted, b"Server Key", _SCRAM_HASH).digest()
        return ScramVerifier(salt, iterations, stored_key, server_key)

    def verify(self, password: str) -> bool:
        candidate = ScramVerifier.create(password, self.iterations, self.salt)
        # constant-time compare of the stored keys
        return hmac.compare_digest(candidate.stored_key, self.stored_key)

    def to_json(self) -> dict:
        b64 = lambda b: base64.b64encode(b).decode("ascii")  # noqa: E731
        return {
            "mechanism": "SCRAM-SHA-256",
            "salt": b64(self.salt),
            "iterations": self.iterations,
            "stored_key": b64(self.stored_key),
            "server_key": b64(self.server_key),
        }

    @staticmethod
    def from_json(d: dict) -> "ScramVerifier":
        b = lambda s: base64.b64decode(s)  # noqa: E731
        return ScramVerifier(b(d["salt"]), int(d["iterations"]), b(d["stored_key"]), b(d["server_key"]))


class AccountStore:
    """Persisted local accounts: username -> SCRAM verifier (JSON at rest)."""

    def __init__(self, path: pathlib.Path) -> None:
        self.path = pathlib.Path(path)
        self._accounts: Dict[str, ScramVerifier] = {}
        if self.path.exists():
            self.load()

    def load(self) -> None:
        data = json.loads(self.path.read_text(encoding="utf-8"))
        self._accounts = {u: ScramVerifier.from_json(v) for u, v in data.get("accounts", {}).items()}

    def save(self) -> None:
        data = {"accounts": {u: v.to_json() for u, v in self._accounts.items()}}
        self.path.write_text(json.dumps(data, indent=2), encoding="utf-8")

    def add(self, username: str, password: str) -> None:
        self._accounts[username] = ScramVerifier.create(password)
        self.save()

    def remove(self, username: str) -> bool:
        existed = username in self._accounts
        self._accounts.pop(username, None)
        self.save()
        return existed

    def list_users(self):
        return sorted(self._accounts.keys())

    def verify(self, username: str, password: str) -> bool:
        v = self._accounts.get(username)
        return bool(v and v.verify(password))

    def get_verifier(self, username: str) -> Optional[ScramVerifier]:
        return self._accounts.get(username)


class AuthProvider:
    """Contract: authenticate a (username, password) -> role_id, or None."""

    name = "base"

    def authenticate(self, username: str, password: str) -> Optional[str]:
        raise NotImplementedError

    @property
    def requires_password(self) -> bool:
        return True

    @property
    def wire_mechanism(self) -> Optional[str]:
        """Wire auth mechanism: None (trust), 'cleartext', or 'SCRAM-SHA-256'."""
        return "cleartext"


class TrustProvider(AuthProvider):
    """Localhost default: any username authenticates as itself, no password."""

    name = "trust"

    def authenticate(self, username: str, password: str) -> Optional[str]:
        del password
        return username or "calcite"

    @property
    def requires_password(self) -> bool:
        return False

    @property
    def wire_mechanism(self) -> Optional[str]:
        return None


class LocalAccountsProvider(AuthProvider):
    """Persisted accounts verified against SCRAM-SHA-256 verifiers at rest.

    ``scram_wire=True`` selects the full SASL SCRAM-SHA-256 wire exchange (no
    password ever on the wire); otherwise the client sends the cleartext password
    (over TLS/localhost) and it is verified against the stored verifier.
    """

    name = "local"

    def __init__(self, store: AccountStore, scram_wire: bool = False) -> None:
        self._store = store
        self._scram_wire = scram_wire

    def authenticate(self, username: str, password: str) -> Optional[str]:
        return username if self._store.verify(username, password) else None

    def get_verifier(self, username: str):
        return self._store.get_verifier(username)

    @property
    def wire_mechanism(self) -> Optional[str]:
        return "SCRAM-SHA-256" if self._scram_wire else "cleartext"


class OidcProvider(AuthProvider):
    """Token-as-password: the client sends an OIDC ID token (JWT) as the password;
    we verify signature + iss/aud/exp against the issuer's key (PGW-044).

    Issuer-generic (Firebase/Auth0/Google/Entra): configure ``issuer`` + ``audience``
    and a key source — a static PEM ``public_key``, a static ``jwks`` dict, or a
    ``jwks_url`` (fetched + cached). The authenticated role is the ``username_claim``
    (default ``sub``). JWT libs are imported lazily so airgap builds without OIDC
    still load.
    """

    name = "oidc"

    def __init__(
        self,
        issuer: str,
        audience: str,
        public_key: Optional[str] = None,
        jwks: Optional[dict] = None,
        jwks_url: Optional[str] = None,
        username_claim: str = "sub",
        algorithms=("RS256",),
    ) -> None:
        self._issuer = issuer
        self._audience = audience
        self._public_key = public_key
        self._jwks = jwks
        self._jwks_url = jwks_url
        self._username_claim = username_claim
        self._algorithms = list(algorithms)
        self._jwks_client = None

    def _key_for(self, token: str):
        import jwt

        if self._public_key is not None:
            return self._public_key
        if self._jwks is not None:
            kid = jwt.get_unverified_header(token).get("kid")
            keys = self._jwks.get("keys", [])
            for k in keys:
                if k.get("kid") == kid or len(keys) == 1:
                    return jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(k))
            raise ValueError("no matching JWKS key for token kid")
        if self._jwks_url is not None:
            if self._jwks_client is None:
                self._jwks_client = jwt.PyJWKClient(self._jwks_url)
            return self._jwks_client.get_signing_key_from_jwt(token).key
        raise ValueError("OidcProvider needs a public_key, jwks, or jwks_url")

    def authenticate(self, username: str, password: str) -> Optional[str]:
        import jwt

        try:
            claims = jwt.decode(
                password,
                self._key_for(password),
                algorithms=self._algorithms,
                audience=self._audience,
                issuer=self._issuer,
            )
        except Exception:
            return None
        return str(claims.get(self._username_claim) or username or "")


def build_provider(kind: str, store_path: Optional[str] = None, **kwargs) -> AuthProvider:
    if kind == "trust" or kind == "none":
        return TrustProvider()
    if kind == "local":
        if not store_path:
            raise ValueError("--auth local requires an accounts store path")
        return LocalAccountsProvider(AccountStore(pathlib.Path(store_path)))
    if kind == "oidc":
        return OidcProvider(**kwargs)
    raise ValueError(f"unknown auth provider {kind!r}")
