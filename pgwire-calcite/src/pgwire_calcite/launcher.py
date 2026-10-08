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


#: PostgreSQL's SSLRequest, GSSENCRequest and CancelRequest codes, and protocol 3.0.
_SSL_REQUEST = 80877103
_GSSENC_REQUEST = 80877104
_CANCEL_REQUEST = 80877102
_PROTOCOL_3 = 196608
#: Longest startup packet PostgreSQL itself accepts.
_MAX_STARTUP_PACKET = 10000


class StartupResponder:
    """Answers clients that connect while the backend is still being built.

    The listening socket is claimed before the backend exists (``claim_listen_socket``)
    and the backend can take minutes to build, so for that whole time the port accepted
    connections that nothing answered: a client could not tell a server that was starting
    from one that was wedged, and waited out its own timeout to learn nothing. With the
    responder running (--reject-while-starting), each connection gets what PostgreSQL
    sends in the same state -- a FATAL ErrorResponse with SQLSTATE 57P03, "the database
    system is starting up" -- and is closed. It is opt-in because a client that connects
    once and waits for the server to come up depends on the connection being held.

    ``stop()`` must be called before the real server adopts the socket; connections that
    arrive after it wait in the listen backlog for the real server.
    """

    MESSAGE = "the database system is starting up"
    _POLL_S = 0.1
    _CLIENT_TIMEOUT_S = 5.0

    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run, name="pgwire-startup-responder", daemon=True
        )
        self.answered = 0

    def start(self) -> "StartupResponder":
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join()

    def _run(self) -> None:
        # A timed accept, so stop() is noticed; the socket goes back to blocking before
        # the real server adopts it.
        self._sock.settimeout(self._POLL_S)
        try:
            while not self._stop.is_set():
                try:
                    client, _ = self._sock.accept()
                except socket.timeout:
                    continue
                threading.Thread(
                    target=self._answer,
                    args=(client,),
                    name="pgwire-startup-answer",
                    daemon=True,
                ).start()
        finally:
            self._sock.settimeout(None)

    @staticmethod
    def _read(client: socket.socket, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = client.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("client closed the connection during startup")
            buf += chunk
        return buf

    def _answer(self, client: socket.socket) -> None:
        import struct

        try:
            client.settimeout(self._CLIENT_TIMEOUT_S)
            length, code = struct.unpack("!II", self._read(client, 8))
            if code in (_SSL_REQUEST, _GSSENC_REQUEST):
                client.sendall(b"N")  # no encryption for a message that carries no data
                length, code = struct.unpack("!II", self._read(client, 8))
            if code != _PROTOCOL_3:
                # A CancelRequest has nothing to cancel yet and gets no reply by protocol;
                # anything else is not a PostgreSQL client.
                return
            if 8 <= length <= _MAX_STARTUP_PACKET:
                self._read(client, length - 8)
            body = b"".join(
                field + value.encode("utf-8") + b"\x00"
                for field, value in (
                    (b"S", "FATAL"),
                    (b"V", "FATAL"),
                    (b"C", "57P03"),
                    (b"M", self.MESSAGE),
                )
            ) + b"\x00"
            client.sendall(b"E" + struct.pack("!i", len(body) + 4) + body)
            self.answered += 1
        except OSError as exc:
            # The client hung up or sent nothing: there is nobody to answer.
            log.debug("[PGWIRE] startup-time connection ended early: %s", exc)
        finally:
            client.close()


def build_state(
    backend=None,
    auth: str = "none",
    users: dict | None = None,
    statement_timeout_ms: int = 0,
    allow_writes: bool = False,
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
    st.allow_writes = bool(allow_writes)
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
    allow_writes: bool = False,
    sock: socket.socket | None = None,
    startup_responder: "StartupResponder | None" = None,
    idle_shutdown_seconds: float | None = None,
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
        backend=backend,
        auth=auth,
        users=users,
        statement_timeout_ms=statement_timeout_ms,
        allow_writes=allow_writes,
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
    # The catalog is installed and the server can answer: hand the socket over. Whoever
    # connects from here on is served, not told the server is starting.
    if startup_responder is not None:
        startup_responder.stop()
    srv = server_mod.start_pgwire_server(
        host,
        port,
        ssl_ctx=ssl_ctx,
        mtls_auth=mtls_auth,
        sock=sock,
        idle_shutdown_seconds=idle_shutdown_seconds,
    )
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
#: PostgreSQL's wording for a session ended by a server shutdown (SQLSTATE 57P01).
SHUTDOWN_REASON = "terminating connection due to administrator command"


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
        # Stop accepting first, then end the sessions that are connected: an idle client
        # holds its request thread in a read with no timeout, so without this every
        # shutdown with a client connected ran out the budget and was forced.
        terminate = getattr(srv, "terminate_sessions", None)
        if terminate is not None:
            terminate(SHUTDOWN_REASON)
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


EXIT_STARTUP_FAILED = 1


def exit_startup_failed(stage: str, host: str, port: int) -> None:
    """Log the exception being handled and end the process at once.

    Called for a failure after the listening socket was claimed. A plain exception is
    not enough there: the embedded JVM's own non-daemon threads (the S3 SDK's
    connection-pool threads in particular) keep the process alive past it, and a process
    that is alive still holds the port -- accepting connections it will never answer,
    and making every later start lose the bind. ``os._exit`` ends the process and closes
    its descriptors whatever the JVM's threads are doing.
    """
    log.exception(
        "[PGWIRE] startup failed while %s; exiting with status %d so %s:%d is released",
        stage, EXIT_STARTUP_FAILED, host, port,
    )
    logging.shutdown()
    os._exit(EXIT_STARTUP_FAILED)


def build_backend(
    kind: str,
    model: str | None,
    jdbc: dict | None = None,
    calcite_child: str | None = None,
    extensions=None,
    jvm_args: list | None = None,
):
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
            jvm_args=list(jvm_args or []),
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
        "--allow-writes",
        action="store_true",
        help="route INSERT/UPDATE/DELETE to the Calcite model (for adapters with modifiable "
        "tables, e.g. salesforce, sharepoint). Without it the server is read-only. A write "
        "is committed by the adapter when its statement runs: ROLLBACK cannot undo it.",
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
    parser.add_argument(
        "--idle-holder-grace-ms",
        type=int,
        default=30000,
        help="how long a statement waits behind a session that holds the query engine "
        "without using it (its client stopped reading a result, or holds a cursor it does "
        "not fetch from) before that session's connection is closed; 0 = never.",
    )
    parser.add_argument(
        "--reject-while-starting",
        action="store_true",
        help="answer a client that connects before the server can serve with FATAL 57P03 "
        "('the database system is starting up') at once, as PostgreSQL does. Without it "
        "such a connection waits until the server is up. For a client that polls for "
        "readiness and must tell a server that is starting from one that is stuck.",
    )
    parser.add_argument(
        "--idle-shutdown-seconds",
        type=float,
        default=None,
        help="exit once the server has had no client connection for this long; unset or "
        "0 = serve until stopped (env PGWIRE_CALCITE_IDLE_SHUTDOWN_SECONDS when unset). "
        "For a server a client starts on demand and shares.",
    )
    parser.add_argument(
        "--jvm-arg",
        action="append",
        default=[],
        metavar="ARG",
        help="argument for the embedded Calcite JVM (repeatable), e.g. --jvm-arg=-Xmx4g or "
        "--jvm-arg=-Dname=value (--backend calcite).",
    )
    parser.add_argument(
        "--pid-file",
        default=None,
        help="write the pid of the process that holds the listening port to this file "
        "once the port is claimed; removed on a clean shutdown.",
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
    if args.pid_file:
        with open(args.pid_file, "w", encoding="utf-8") as fh:
            fh.write(str(os.getpid()))
    # Opt-in (--reject-while-starting): from here until the server can answer, tell every
    # client that connects that it is starting (SQLSTATE 57P03). Without it a connection
    # made during start-up waits in the listen backlog and is served once the server is
    # up, which is what a client that connects once and waits relies on.
    responder = StartupResponder(listen_sock).start() if args.reject_while_starting else None

    from pgwire_calcite.extensions import resolve as _resolve_ext

    enabled_ext = _resolve_ext(args.extension)
    try:
        backend = build_backend(
            args.backend,
            args.model,
            jdbc=jdbc,
            calcite_child=args.calcite_child,
            extensions=enabled_ext,
            jvm_args=args.jvm_arg,
        )
    except Exception:
        exit_startup_failed("building the backend", args.host, args.port)
        raise  # only reached when exit_startup_failed is replaced (tests)
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
    CancelScope.idle_holder_grace_ms = max(0, args.idle_holder_grace_ms)
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
            allow_writes=args.allow_writes,
            sock=listen_sock,
            startup_responder=responder,
            idle_shutdown_seconds=args.idle_shutdown_seconds,
        )
    except Exception:
        # Any failure here, not only an OSError: the catalog walk, a bad certificate, an
        # mTLS misconfiguration. See exit_startup_failed for why this must be a hard exit.
        exit_startup_failed("starting the server", args.host, args.port)
        raise  # only reached when exit_startup_failed is replaced (tests)
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
    if args.pid_file and os.path.exists(args.pid_file):
        os.remove(args.pid_file)
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
