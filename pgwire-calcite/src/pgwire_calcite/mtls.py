# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Mutual TLS — client-certificate verification on the pgwire-calcite socket (Phase 3 hardening).

Ported from provisa's ``provisa.security.mtls`` (REQ-1228) and narrowed to pgwire-calcite's one
wire protocol: no gRPC/Flight translation, no per-protocol-vs-node-wide env layering (there is
only one server here). TLS alone authenticates the server; client-certificate verification moves
the first check to the handshake, so a caller without a certificate the deployment's CA signed
never reaches the credential layer at all.

* ``PGWIRE_CALCITE_CLIENT_CA`` — PEM bundle of the CA(s) permitted to sign client certificates.
  Unset (the default) means mTLS is off entirely — opt-in.
* ``PGWIRE_CALCITE_MTLS_MODE`` — ``required`` (default once a CA is configured) or ``optional``.
* ``PGWIRE_CALCITE_MTLS_BIND_PRINCIPAL`` — when true, the certificate's common name must equal
  the username the connection then authenticates as.

Nothing here is inferred. A mode without a CA raises rather than quietly serving unverified
connections, and an unrecognized mode raises rather than being read as the safest neighbour: a
deployment that believes it requires client certificates and does not is worse off than one that
fails to start.
"""

from __future__ import annotations

import os
import ssl
from typing import NamedTuple, Optional

_TRUE = {"1", "true", "yes", "on"}

CLIENT_CA_ENV = "PGWIRE_CALCITE_CLIENT_CA"
MODE_ENV = "PGWIRE_CALCITE_MTLS_MODE"
BIND_PRINCIPAL_ENV = "PGWIRE_CALCITE_MTLS_BIND_PRINCIPAL"


class ClientAuth(NamedTuple):
    """What the pgwire socket must demand of its clients' certificates."""

    ca_path: str
    required: bool
    bind_principal: bool


def resolve_client_auth(
    ca_path: Optional[str] = None,
    mode: Optional[str] = None,
    bind_principal: Optional[bool] = None,
) -> Optional[ClientAuth]:
    """The client-certificate policy, or None when mTLS is off.

    Each argument falls back to its environment variable when not passed explicitly, so a
    launcher CLI flag and the bare environment behave identically. Raises ``ValueError`` on a
    configuration that cannot mean what it says — a verification mode with no CA to verify
    against, or a mode outside the two the design defines.
    """
    if ca_path is None:
        ca_path = os.environ.get(CLIENT_CA_ENV) or None
    if mode is None:
        mode = os.environ.get(MODE_ENV) or None
    if bind_principal is None:
        raw_bind = os.environ.get(BIND_PRINCIPAL_ENV)
        bind_principal = raw_bind.lower() in _TRUE if raw_bind else False

    if ca_path is None:
        if mode is not None:
            raise ValueError(
                f"{MODE_ENV} is set to {mode!r} but no client CA is configured; set "
                f"{CLIENT_CA_ENV} to the PEM bundle that signs client certificates"
            )
        return None
    if not os.path.exists(ca_path):
        raise ValueError(f"client CA bundle {ca_path!r} does not exist")

    # Configuring a CA and no mode means required: naming a trust anchor is the act of
    # deciding client certificates matter; defaulting that to optional would hand back
    # exactly the unverified connections the operator configured the CA to exclude.
    if mode is None:
        required = True
    elif mode == "required":
        required = True
    elif mode == "optional":
        required = False
    else:
        raise ValueError(f"{MODE_ENV}={mode!r} is not one of 'required', 'optional'")

    return ClientAuth(ca_path=ca_path, required=required, bind_principal=bool(bind_principal))


def apply_to_context(ctx: ssl.SSLContext, auth: Optional[ClientAuth]) -> None:
    """Put a client-certificate policy on the server's SSL context."""
    if auth is None:
        return
    ctx.verify_mode = ssl.CERT_REQUIRED if auth.required else ssl.CERT_OPTIONAL
    ctx.load_verify_locations(cafile=auth.ca_path)


def peer_common_name(peer_cert: Optional[dict]) -> Optional[str]:
    """The common name of a verified peer certificate.

    ``getpeercert()`` returns ``{}`` for a connection that presented nothing and ``None``
    before the handshake completes; both mean there is no verified name to report. Only a
    certificate the CA signed reaches here — the ``ssl`` module rejects the rest during the
    handshake — so the name may be trusted once found.
    """
    if not peer_cert:
        return None
    for rdn in peer_cert.get("subject", ()):
        for key, value in rdn:
            if key == "commonName":
                return value
    return None


def assert_principal_binding(
    auth: Optional[ClientAuth], peer_cert: Optional[dict], username: str
) -> None:
    """Refuse a connection whose certificate names someone other than the authenticating user.

    Only under ``PGWIRE_CALCITE_MTLS_BIND_PRINCIPAL``. Without it a client certificate proves
    the caller belongs on the network and the credential proves who they are, which is the usual
    arrangement; with it the two must agree, so a stolen password is useless without that user's
    certificate and a shared service certificate cannot be used to log in as anyone.
    """
    if auth is None or not auth.bind_principal:
        return
    common_name = peer_common_name(peer_cert)
    if common_name is None:
        raise PermissionError(
            "client certificate carries no common name to bind against the authenticating user"
        )
    if common_name != username:
        raise PermissionError(
            f"client certificate names {common_name!r}, which cannot authenticate as {username!r}"
        )
