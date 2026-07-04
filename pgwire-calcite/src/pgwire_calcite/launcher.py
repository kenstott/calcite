# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Minimal launcher for pgwire-calcite.

Replaces provisa's FastAPI application bootstrap (open decision §7): boots a
backend, installs the shared ``ServerState`` onto the wire module, starts the
socketserver, and blocks. No FastAPI ``state``, no async pipeline.

Phase 0 wires the ``StubBackend``. Phase 1 swaps in the ``CalciteBackend`` here
(one line) and seeds the schema registry; Phase 5 replaces this process with the
supervisor + recyclable-child topology.
"""

from __future__ import annotations

import argparse
import logging
import ssl
import sys
import threading

import pgwire_calcite.server as server_mod
from pgwire_calcite.backend import StubBackend
from pgwire_calcite.state import ServerState

log = logging.getLogger(__name__)


def build_state(backend=None, auth: str = "none", users: dict | None = None) -> ServerState:
    """Assemble the ServerState the wire layer reads.

    ``auth='none'`` is trust mode; ``auth='simple'`` enforces cleartext-password
    auth against ``users`` (PGW-007).
    """
    if backend is None:
        backend = StubBackend()
    st = ServerState(backend=backend)
    st.auth_config = {"provider": auth}
    st.auth_middleware_active = auth != "none"
    st.users = dict(users or {})
    return st


def _build_ssl_ctx(certfile: str | None, keyfile: str | None) -> ssl.SSLContext | None:
    if not certfile or not keyfile:
        return None
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(certfile=certfile, keyfile=keyfile)
    return ctx


def serve(
    host: str = "127.0.0.1",
    port: int = 5433,
    auth: str = "none",
    users: dict | None = None,
    certfile: str | None = None,
    keyfile: str | None = None,
    backend=None,
) -> server_mod.CalciteServer:
    """Install state and start the server thread. Returns the server (non-blocking)."""
    server_mod.state = build_state(backend=backend, auth=auth, users=users)
    ssl_ctx = _build_ssl_ctx(certfile, keyfile)
    srv = server_mod.start_pgwire_server(host, port, ssl_ctx=ssl_ctx)
    return srv


def main(argv: list | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pgwire-calcite", description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5433)
    parser.add_argument("--auth", choices=["none", "simple"], default="none")
    parser.add_argument(
        "--user",
        action="append",
        default=[],
        metavar="NAME:PASSWORD",
        help="cleartext user for --auth simple (repeatable)",
    )
    parser.add_argument("--tls-cert", default=None)
    parser.add_argument("--tls-key", default=None)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    users: dict = {}
    for spec in args.user:
        if ":" not in spec:
            parser.error(f"--user expects NAME:PASSWORD, got {spec!r}")
        name, password = spec.split(":", 1)
        users[name] = password

    srv = serve(
        host=args.host,
        port=args.port,
        auth=args.auth,
        users=users,
        certfile=args.tls_cert,
        keyfile=args.tls_key,
    )
    log.info(
        "pgwire-calcite (Phase 0, StubBackend) listening on %s:%d — Ctrl-C to stop",
        args.host,
        args.port,
    )
    stop = threading.Event()
    try:
        stop.wait()
    except KeyboardInterrupt:
        log.info("shutting down")
        srv.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
