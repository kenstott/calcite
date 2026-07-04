# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Server-side SCRAM-SHA-256 exchange (RFC 5802 / RFC 7677), for PGW-043.

Uses the at-rest ``ScramVerifier`` (salt/iterations/StoredKey/ServerKey) from
auth.py, so the server never needs the plaintext password and never sends it on
the wire — secure even without TLS. This module is the pure crypto exchange;
server.py drives the PostgreSQL SASL message framing around it.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
from typing import Optional, Tuple


def _b64(b: bytes) -> str:
    return base64.b64encode(b).decode("ascii")


def _ub64(s: str) -> bytes:
    return base64.b64decode(s)


def parse_client_first(msg: str) -> Tuple[str, str, str]:
    """Return (client_first_bare, username, client_nonce) from a client-first-message.

    Format: ``<gs2-header>n=<user>,r=<nonce>`` where gs2-header is e.g. ``n,,``.
    """
    fields = msg.split(",")
    # gs2 header is the first two comma fields (cbind-flag, authzid) + trailing comma
    bare = ",".join(fields[2:])
    username = ""
    nonce = ""
    for attr in bare.split(","):
        if attr.startswith("n="):
            username = attr[2:].replace("=2C", ",").replace("=3D", "=")
        elif attr.startswith("r="):
            nonce = attr[2:]
    return bare, username, nonce


class ScramServerExchange:
    """One SCRAM-SHA-256 authentication exchange against a stored verifier."""

    def __init__(self, verifier, server_nonce: Optional[str] = None) -> None:
        self._verifier = verifier
        self._server_nonce = server_nonce or _b64(os.urandom(18))
        self._client_first_bare = ""
        self._server_first = ""

    def server_first(self, client_first: str) -> str:
        bare, _user, client_nonce = parse_client_first(client_first)
        self._client_first_bare = bare
        nonce = client_nonce + self._server_nonce
        self._server_first = (
            f"r={nonce},s={_b64(self._verifier.salt)},i={self._verifier.iterations}"
        )
        return self._server_first

    def verify_final(self, client_final: str) -> Tuple[bool, Optional[str]]:
        """Verify the client-final-message; return (ok, server_final_message)."""
        try:
            without_proof, proof_b64 = client_final.rsplit(",p=", 1)
            proof = _ub64(proof_b64)
        except (ValueError, Exception):
            return False, None

        auth_message = f"{self._client_first_bare},{self._server_first},{without_proof}"
        client_signature = hmac.new(
            self._verifier.stored_key, auth_message.encode("utf-8"), "sha256"
        ).digest()
        client_key = bytes(a ^ b for a, b in zip(proof, client_signature))
        if not hmac.compare_digest(hashlib.sha256(client_key).digest(), self._verifier.stored_key):
            return False, None
        server_signature = hmac.new(
            self._verifier.server_key, auth_message.encode("utf-8"), "sha256"
        ).digest()
        return True, f"v={_b64(server_signature)}"
