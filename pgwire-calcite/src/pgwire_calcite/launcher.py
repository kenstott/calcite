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
import os
import signal
import ssl
import sys
import threading

import pgwire_calcite.server as server_mod
from pgwire_calcite.backend import StubBackend
from pgwire_calcite.state import ServerState

log = logging.getLogger(__name__)


def build_state(
    backend=None,
    auth: str = "none",
    users: dict | None = None,
    statement_timeout_ms: int = 0,
) -> ServerState:
    """Assemble the ServerState the wire layer reads.

    ``auth='none'`` is trust mode; ``auth='simple'`` enforces cleartext-password
    auth against ``users`` (PGW-007). ``statement_timeout_ms`` is the server-wide
    default every session starts with; 0 = no timeout, as in PostgreSQL (PGW-051).
    """
    if backend is None:
        backend = StubBackend()
    st = ServerState(backend=backend)
    st.auth_config = {"provider": auth}
    st.auth_middleware_active = auth != "none"
    st.users = dict(users or {})
    st.statement_timeout_ms = int(statement_timeout_ms)
    return st


def _build_ssl_ctx(
    certfile: str | None, keyfile: str | None, mtls_auth=None
) -> ssl.SSLContext | None:
    if not certfile or not keyfile:
        return None
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(certfile=certfile, keyfile=keyfile)
    if mtls_auth is not None:
        from pgwire_calcite.mtls import apply_to_context

        apply_to_context(ctx, mtls_auth)
    return ctx


def serve(
    host: str = "127.0.0.1",
    port: int = 5433,
    auth: str = "none",
    users: dict | None = None,
    certfile: str | None = None,
    keyfile: str | None = None,
    backend: object = None,
    auth_provider=None,
    authz_grants=None,
    database: str = "postgres",
    client_ca: str | None = None,
    mtls_mode: str | None = None,
    mtls_bind_principal: bool | None = None,
    statement_timeout_ms: int = 0,
) -> server_mod.CalciteServer:
    """Install state and start the server thread. Returns the server (non-blocking).

    ``client_ca``/``mtls_mode``/``mtls_bind_principal`` configure opt-in mutual TLS
    (PGWIRE_CALCITE_CLIENT_CA / _MTLS_MODE / _MTLS_BIND_PRINCIPAL when left None); mTLS is
    off unless a CA is configured either way. Only meaningful with ``certfile``/``keyfile``
    also set — mTLS needs a server certificate to negotiate TLS in the first place.
    """
    # Set the catalog/database name reported to clients (current_database, pg_database,
    # information_schema) BEFORE catalog population reads it. Single source of truth,
    # kept in sync with schema_registry.database below.
    from pgwire_calcite import catalog as _catalog

    _catalog.set_database_name(database)
    server_mod.state = build_state(
        backend=backend, auth=auth, users=users, statement_timeout_ms=statement_timeout_ms
    )
    server_mod.state.schema_registry.database = database
    # Keep the GUC the catalog intercept reports in step with the server default,
    # so `SHOW statement_timeout` on a fresh session matches what is enforced.
    _catalog._KNOWN_SETTINGS["statement_timeout"] = server_mod._format_statement_timeout(
        int(statement_timeout_ms)
    )
    if auth_provider is not None:
        server_mod.state.auth_provider = auth_provider
    # Set authz grants before catalog population so discovery is filtered per role.
    if authz_grants is not None:
        server_mod.state.authz_grants = authz_grants
    # Populate the catalog intercept from Calcite metadata (PGW-012). In-process
    # backends expose a JDBC connection; the bridge backend fetches the catalog
    # from the Calcite child over the socket. StubBackend has neither -> skipped.
    conn = getattr(backend, "connection", None)
    if conn is not None:
        from pgwire_calcite.catalog_populate import populate_state

        populate_state(conn, server_mod.state)
    else:
        fetch_catalog = getattr(backend, "fetch_catalog", None)
        if fetch_catalog is not None:
            from pgwire_calcite.catalog_populate import install_catalog

            # A catalog the child cannot deliver is a startup failure: serving without one
            # would answer every client's introspection with an empty schema and hide the
            # child's fault behind a warning nobody reads.
            ctx, column_types = fetch_catalog()
            install_catalog(server_mod.state, ctx, column_types)
    from pgwire_calcite.mtls import resolve_client_auth

    mtls_auth = resolve_client_auth(client_ca, mtls_mode, mtls_bind_principal)
    ssl_ctx = _build_ssl_ctx(certfile, keyfile, mtls_auth=mtls_auth)
    if mtls_auth is not None and ssl_ctx is None:
        # A client CA with no server certificate configured can mean only one thing: TLS
        # itself never gets negotiated, so the mTLS policy could never apply. Refusing to
        # start is better than serving connections the operator believes are verified.
        raise ValueError(
            "mTLS client CA is configured but no --tls-cert/--tls-key (or certfile/keyfile) "
            "was given; a server certificate is required to negotiate TLS at all"
        )
    srv = server_mod.start_pgwire_server(host, port, ssl_ctx=ssl_ctx, mtls_auth=mtls_auth)
    return srv


OWNER_POLL_SECONDS = 1.0


def watch_owner(owner_pid: int, stop: threading.Event) -> threading.Thread:
    """Request shutdown once the process that owns this server is gone (``--owner-pid``).

    An embedding host (Provisa) starts this server through the Java launcher in its own
    session so that stopping it signals the whole tree — which also means the tree does NOT
    die with the host. A host that is SIGKILLed (a test runner's teardown, an OOM kill) runs
    no shutdown hook, and every server it started would keep its port until someone noticed.
    Polling the owner's liveness closes that gap from the child's side: no signal has to be
    delivered for the server to know it is orphaned.
    """

    def _watch() -> None:
        while not stop.wait(OWNER_POLL_SECONDS):
            try:
                os.kill(owner_pid, 0)
            except ProcessLookupError:
                log.info("owner pid %d is gone; shutting down", owner_pid)
                stop.set()
                return
            except PermissionError:
                continue  # alive, but owned by another user: still there

    t = threading.Thread(target=_watch, name="pgwire-owner-watch", daemon=True)
    t.start()
    return t


def install_shutdown_handler(stop: threading.Event) -> None:
    """Make SIGTERM (and SIGINT) request a clean shutdown instead of a hang.

    Must be called AFTER the backend is constructed: starting the embedded
    Calcite JVM (JPype -> ``jpype.startJVM``) installs its own native SIGTERM/
    SIGINT handlers unless ``-Xrs`` is passed, and the JVM's handler otherwise
    wins (last ``signal.signal``/``sigaction`` call for a given signal replaces
    any earlier one at the OS level). The Java launcher's shutdown hook sends
    SIGTERM to this process expecting it to exit and release the listening
    socket promptly (see ``Launcher.java``); without re-installing our own
    handler last, the JVM's handler can swallow the signal and this process
    (and the port) lingers.
    """

    def _handle(signum, frame):  # noqa: ANN001 - signal handler signature
        log.info("received signal %s; shutting down", signum)
        stop.set()

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)


def build_backend(kind: str, model: str | None, jdbc: dict | None = None, calcite_child: str | None = None, extensions=None):
    """Construct the execution backend.

    - 'stub'    (Phase 0) fixed responses;
    - 'calcite' (Phase 1) in-process JPype;
    - 'bridge'  (Phase 5) talk to a separate Calcite JVM child over the socket
                bridge (``calcite_child`` = "host:port").

    ``jdbc`` mirrors the Calcite/Avatica JDBC connection properties.
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
            extensions=extensions,
        )
    if kind == "bridge":
        from pgwire_calcite.sidecar import BridgeBackend

        host, _, port = (calcite_child or "127.0.0.1:5533").partition(":")
        return BridgeBackend(host=host or "127.0.0.1", port=int(port or 5533), extensions=extensions)
    raise ValueError(f"unknown backend {kind!r}")


def main(argv: list | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pgwire-calcite", description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5433)
    parser.add_argument(
        "--owner-pid",
        type=int,
        default=None,
        help="exit when this process is gone (the host that started the server)",
    )
    parser.add_argument(
        "--database",
        default="postgres",
        help="catalog/database name reported to clients (current_database(), "
        "pg_database, information_schema); default the PG-standard 'postgres'. "
        "Set a topic-relevant name per variant, e.g. --database govdata.",
    )
    parser.add_argument("--backend", choices=["stub", "calcite", "bridge"], default="stub")
    parser.add_argument("--model", default=None, help="Calcite model JSON path (--backend calcite)")
    parser.add_argument(
        "--calcite-child",
        default="127.0.0.1:5533",
        help="host:port of the Calcite JVM child (--backend bridge)",
    )
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
    parser.add_argument(
        "--auth", choices=["none", "simple", "trust", "local", "scram"], default="none"
    )
    parser.add_argument(
        "--auth-store", default=None, help="accounts JSON path for --auth local/scram (Phase 5b)"
    )
    parser.add_argument(
        "--user",
        action="append",
        default=[],
        metavar="NAME:PASSWORD",
        help="cleartext user for --auth simple (repeatable)",
    )
    parser.add_argument(
        "--statement-timeout-ms",
        type=int,
        default=0,
        help="server-wide default statement_timeout in milliseconds; 0 = no timeout "
        "(PG default). Sessions override it with SET statement_timeout.",
    )
    parser.add_argument("--tls-cert", default=None)
    parser.add_argument("--tls-key", default=None)
    parser.add_argument(
        "--client-ca",
        default=None,
        help="PEM bundle of CA(s) trusted to sign client certificates; enables mutual TLS "
        "(env PGWIRE_CALCITE_CLIENT_CA). Opt-in; requires --tls-cert/--tls-key.",
    )
    parser.add_argument(
        "--mtls-mode",
        choices=["required", "optional"],
        default=None,
        help="'required' (default once --client-ca is set) or 'optional' "
        "(env PGWIRE_CALCITE_MTLS_MODE)",
    )
    parser.add_argument(
        "--mtls-bind-principal",
        action="store_true",
        default=None,
        help="require the client certificate's common name to equal the startup user "
        "(env PGWIRE_CALCITE_MTLS_BIND_PRINCIPAL)",
    )
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument(
        "--extension",
        action="append",
        default=[],
        help="enable a PG extension surface (repeatable), e.g. json (Phase 8)",
    )
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
    from pgwire_calcite.extensions import resolve as _resolve_ext

    enabled_ext = _resolve_ext(args.extension)
    backend = build_backend(
        args.backend, args.model, jdbc=jdbc, calcite_child=args.calcite_child, extensions=enabled_ext
    )
    auth_provider = None
    if args.auth in ("trust", "local", "scram"):
        from pgwire_calcite.auth import AccountStore, LocalAccountsProvider, TrustProvider

        if args.auth == "trust":
            auth_provider = TrustProvider()
        else:
            if not args.auth_store:
                parser.error(f"--auth {args.auth} requires --auth-store")
            store = AccountStore(args.auth_store)
            auth_provider = LocalAccountsProvider(store, scram_wire=(args.auth == "scram"))
    srv = serve(
        host=args.host,
        port=args.port,
        auth=args.auth if args.auth in ("none", "simple") else "none",
        users=users,
        certfile=args.tls_cert,
        keyfile=args.tls_key,
        backend=backend,
        auth_provider=auth_provider,
        database=args.database,
        client_ca=args.client_ca,
        mtls_mode=args.mtls_mode,
        mtls_bind_principal=args.mtls_bind_principal,
        statement_timeout_ms=args.statement_timeout_ms,
    )
    log.info(
        "pgwire-calcite (%s backend) listening on %s:%d — Ctrl-C to stop",
        args.backend,
        args.host,
        args.port,
    )
    stop = threading.Event()
    # Installed AFTER build_backend()/serve() so this wins over any signal
    # handler the embedded Calcite JVM installed on startup (see
    # install_shutdown_handler's docstring) -- otherwise SIGTERM from the Java
    # launcher's shutdown hook can be swallowed and the listening socket stays
    # bound.
    install_shutdown_handler(stop)
    if args.owner_pid is not None:
        watch_owner(args.owner_pid, stop)
    try:
        stop.wait()
    except KeyboardInterrupt:
        stop.set()
    log.info("shutting down")
    srv.shutdown()
    srv.server_close()
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
