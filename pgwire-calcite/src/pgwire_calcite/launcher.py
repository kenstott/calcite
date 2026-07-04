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
    auth_provider=None,
) -> server_mod.CalciteServer:
    """Install state and start the server thread. Returns the server (non-blocking)."""
    server_mod.state = build_state(backend=backend, auth=auth, users=users)
    if auth_provider is not None:
        server_mod.state.auth_provider = auth_provider
    # Populate the catalog intercept from Calcite metadata (PGW-012) when the
    # backend exposes an embedded connection. StubBackend has none -> skipped.
    conn = getattr(backend, "connection", None)
    if conn is not None:
        from pgwire_calcite.catalog_populate import populate_state

        populate_state(conn, server_mod.state)
    ssl_ctx = _build_ssl_ctx(certfile, keyfile)
    srv = server_mod.start_pgwire_server(host, port, ssl_ctx=ssl_ctx)
    return srv


def build_backend(kind: str, model: str | None, jdbc: dict | None = None):
    """Construct the execution backend. 'stub' (Phase 0) or 'calcite' (Phase 1).

    ``jdbc`` mirrors the Calcite/Avatica JDBC connection properties (lex, fun,
    schema, and arbitrary extra props) so the CLI surface matches what a driver
    user already knows.
    """
    if kind == "stub":
        return StubBackend()
    if kind == "calcite":
        from pgwire_calcite.calcite_backend import CalciteBackend

        jdbc = jdbc or {}
        return CalciteBackend(
            model_path=model,
            lex=jdbc.get("lex", "ORACLE"),
            fun=jdbc.get("fun", "standard"),
            default_schema=jdbc.get("schema"),
            extra_props=jdbc.get("extra_props") or {},
        )
    raise ValueError(f"unknown backend {kind!r}")


def main(argv: list | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pgwire-calcite", description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5433)
    parser.add_argument("--backend", choices=["stub", "calcite"], default="stub")
    parser.add_argument("--model", default=None, help="Calcite model JSON path (--backend calcite)")
    # JDBC-mirroring options (same surface as the Calcite/Avatica driver).
    parser.add_argument("--lex", default="ORACLE", help="Calcite lexer policy (JDBC 'lex')")
    parser.add_argument("--fun", default="standard", help="Calcite function library (JDBC 'fun')")
    parser.add_argument("--schema", default=None, help="default schema (JDBC 'schema')")
    parser.add_argument(
        "--jdbc-prop",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help="extra Calcite JDBC connection property (repeatable)",
    )
    parser.add_argument("--auth", choices=["none", "simple", "trust", "local"], default="none")
    parser.add_argument(
        "--auth-store", default=None, help="accounts JSON path for --auth local (Phase 5b)"
    )
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

    extra_props: dict = {}
    for spec in args.jdbc_prop:
        if "=" not in spec:
            parser.error(f"--jdbc-prop expects NAME=VALUE, got {spec!r}")
        name, value = spec.split("=", 1)
        extra_props[name] = value
    jdbc = {"lex": args.lex, "fun": args.fun, "schema": args.schema, "extra_props": extra_props}
    backend = build_backend(args.backend, args.model, jdbc=jdbc)
    auth_provider = None
    if args.auth in ("trust", "local"):
        from pgwire_calcite.auth import build_provider

        auth_provider = build_provider(args.auth, args.auth_store)
    srv = serve(
        host=args.host,
        port=args.port,
        auth=args.auth if args.auth in ("none", "simple") else "none",
        users=users,
        certfile=args.tls_cert,
        keyfile=args.tls_key,
        backend=backend,
        auth_provider=auth_provider,
    )
    log.info(
        "pgwire-calcite (%s backend) listening on %s:%d — Ctrl-C to stop",
        args.backend,
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


def users_main(argv: list | None = None) -> int:
    """`pgwire-calcite-users` — manage local accounts (SCRAM-SHA-256, Phase 5b)."""
    from pgwire_calcite.auth import AccountStore

    parser = argparse.ArgumentParser(prog="pgwire-calcite-users")
    parser.add_argument("--store", required=True, help="accounts JSON path")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_add = sub.add_parser("add")
    p_add.add_argument("username")
    p_add.add_argument("password")
    p_rm = sub.add_parser("rm")
    p_rm.add_argument("username")
    sub.add_parser("list")
    args = parser.parse_args(argv)

    store = AccountStore(args.store)
    if args.cmd == "add":
        store.add(args.username, args.password)
        print(f"added {args.username}")
    elif args.cmd == "rm":
        print(f"removed {args.username}" if store.remove(args.username) else f"no such user {args.username}")
    elif args.cmd == "list":
        for u in store.list_users():
            print(u)
    return 0


if __name__ == "__main__":
    sys.exit(main())
