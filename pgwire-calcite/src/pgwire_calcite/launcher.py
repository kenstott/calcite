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
import socket
import ssl
import sys
import threading

import pgwire_calcite.server as server_mod
from pgwire_calcite.backend import StubBackend
from pgwire_calcite.state import ServerState

log = logging.getLogger(__name__)


def claim_listen_socket(host: str, port: int) -> socket.socket | None:
    """Bind AND listen on (host, port) right now, before anything else — the actual fix
    for kenstott/calcite#(pgwire port exhaustion), not just a pre-flight check.

    The bind used to happen only deep inside start_pgwire_server(), AFTER build_backend()
    had already run to completion — and build_backend() embeds a JVM via JPype and cold-
    mounts all 26 govdata schemas from S3/Iceberg metadata, which the SPAWN_TIMEOUT_MILLIS
    comment on the Java caller's side documents as taking *minutes*, not seconds. During
    that whole window nobody had actually claimed the port yet, so every other process
    racing to connect (e.g. many fleet-wide sync workers starting around the same time)
    would each independently conclude "nothing is listening" and spawn its own redundant,
    equally expensive backend build — that's why there were 37 of these processes at once,
    not just two. And whichever ones lost the eventual bind used to leak forever: an
    embedded JVM's own non-daemon threads (the S3 SDK's connection-pool threads) kept the
    OS process alive even past the unhandled OSError from a failed bind. Observed live: 37
    such zombies leaked ~8,300 connections between them, exhausting the host's ephemeral
    port range and breaking every other outbound/loopback connection on the machine
    (unrelated R2 sync, MinIO) for 17+ hours.

    Claiming the real listening socket here, in milliseconds, before build_backend() is
    called at all, closes both problems at once: the kernel's bind is atomic, so only one
    process can ever win it, and every other process finds out within milliseconds of its
    own startup — not minutes into a redundant cold-mount it should never have started.

    Returns the live socket (caller must pass it through to start_pgwire_server(sock=...)
    and must NOT close it) on success, or None if something else already holds the port."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind((host, port))
        sock.listen()
        return sock
    except OSError:
        sock.close()
        return None


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
    sock: socket.socket | None = None,
) -> server_mod.CalciteServer:
    """Install state and start the server thread. Returns the server (non-blocking).

    ``client_ca``/``mtls_mode``/``mtls_bind_principal`` configure opt-in mutual TLS
    (PGWIRE_CALCITE_CLIENT_CA / _MTLS_MODE / _MTLS_BIND_PRINCIPAL when left None); mTLS is
    off unless a CA is configured either way. Only meaningful with ``certfile``/``keyfile``
    also set — mTLS needs a server certificate to negotiate TLS in the first place.

    ``sock``, when given, is an already bound+listening socket from
    ``claim_listen_socket()`` — see that function's docstring for why binding this early,
    before the backend is built, is the actual fix for the pgwire port-exhaustion bug.
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
        from pgwire_calcite.catalog_populate import populate_state_cached

        # Cached when a pre-built (or previously-generated) catalog cache sits next to
        # the model file -- see catalog_populate.catalog_cache_path. A fresh launch with
        # no cache falls back to the original live JDBC/Iceberg walk and writes one.
        model_path = getattr(backend, "_model_path", None)
        populate_state_cached(conn, server_mod.state, model_path)
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
    srv = server_mod.start_pgwire_server(host, port, ssl_ctx=ssl_ctx, mtls_auth=mtls_auth, sock=sock)
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


SHUTDOWN_BUDGET_SECONDS = 10.0
EXIT_SHUTDOWN_STUCK = 3


def close_server(srv, budget: float = SHUTDOWN_BUDGET_SECONDS, exit_fn=os._exit) -> None:
    """Stop ``srv`` and release its port, never waiting on a wedged request thread.

    ``server_close()`` joins every outstanding request thread, so one handler blocked on
    something with no timeout would keep the process and its port alive indefinitely. Once
    shutdown has been requested there is no in-flight work worth preserving, so if the close
    does not finish within ``budget`` the process exits outright (``os._exit`` also ends
    native JVM threads that a normal interpreter exit would wait on).
    """
    done = threading.Event()

    def _close() -> None:
        srv.shutdown()
        srv.server_close()
        done.set()

    threading.Thread(target=_close, name="pgwire-server-close", daemon=True).start()
    if not done.wait(budget):
        log.error(
            "[PGWIRE] server close did not finish within %.1fs (a request thread is stuck); "
            "forcing exit to release the port",
            budget,
        )
        exit_fn(EXIT_SHUTDOWN_STUCK)


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
    parser.add_argument(
        "--max-queue-wait-ms",
        type=int,
        default=120000,
        help="server-wide bound on how long a statement waits for the query engine "
        "behind other statements before failing with a 'server busy' error "
        "(SQLSTATE 57014); 0 = unbounded. Applies even when statement_timeout is 0.",
    )
    parser.add_argument(
        "--max-unfiltered-scan-rows",
        type=int,
        default=0,
        help="reject a SELECT that scans a table estimated above this many rows without "
        "filtering on one of its partition columns (SQLSTATE 54000); 0 = off. "
        "Requires --table-coverage-file.",
    )
    parser.add_argument(
        "--table-coverage-file",
        default=None,
        help="JSON of schema.table -> {row_count, partition_columns}, written by "
        "scripts/export_table_coverage.py; the row estimates --max-unfiltered-scan-rows uses.",
    )
    parser.add_argument(
        "--cancel-grace-ms",
        type=int,
        default=60000,
        help="how long a cancelled statement (statement_timeout or CancelRequest) may "
        "take to return before the server logs a Java thread dump and exits with "
        "status 3 so a fresh server replaces it; 0 = wait forever.",
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

    # Claim the real listening socket BEFORE the expensive backend build — see
    # claim_listen_socket()'s docstring. A losing race must never get as far as building
    # a CalciteBackend (JVM + ~250 S3/Iceberg connections) in the first place, and must
    # find out it lost within milliseconds, not minutes into a redundant cold-mount.
    listen_sock = claim_listen_socket(args.host, args.port)
    if listen_sock is None:
        log.error(
            "[PGWIRE] %s:%d is already in use — another pgwire-calcite instance is "
            "presumably the real listener. Exiting without building a backend.",
            args.host, args.port,
        )
        return 1

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
    from pgwire_calcite.calcite_backend import CalciteBackend, CancelScope, InFlightStatement

    if args.max_unfiltered_scan_rows > 0:
        if not args.table_coverage_file:
            parser.error("--max-unfiltered-scan-rows requires --table-coverage-file")
        from pgwire_calcite.admission import AdmissionPolicy, load_coverage

        CalciteBackend.admission = AdmissionPolicy(
            args.max_unfiltered_scan_rows, load_coverage(args.table_coverage_file)
        )
    CancelScope.max_queue_wait_ms = max(0, args.max_queue_wait_ms)
    InFlightStatement.cancel_grace_ms = max(0, args.cancel_grace_ms)
    try:
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
            sock=listen_sock,
        )
    except OSError:
        # No bind can fail here anymore — claim_listen_socket() already holds the real
        # listening socket, and CalciteServer adopts it rather than rebinding (see its
        # __init__). This is a last-resort safety net for a genuinely unexpected failure,
        # not the normal bind-race path anymore. Kept anyway because the old failure mode
        # (a Python-level exception left the process alive forever, held open by an
        # embedded JVM's own non-daemon threads — the S3 SDK's connection-pool threads in
        # particular) was exactly the port-exhaustion bug this whole change exists to fix:
        # os._exit() guarantees the process actually dies and its fds actually close,
        # regardless of what native JVM threads are doing, rather than trusting a plain
        # exception to be enough.
        log.error(
            "[PGWIRE] unexpected failure starting the server on %s:%d after the listen "
            "socket was already claimed — forcing immediate exit rather than lingering.",
            args.host, args.port,
        )
        os._exit(1)
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
    close_server(srv)
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
