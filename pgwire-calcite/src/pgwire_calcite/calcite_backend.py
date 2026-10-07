# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Calcite execution backend (Phase 1, PGW-015).

Replaces the Phase-0 StubBackend: transpiles PG->Calcite (dialect.py) and runs
the result against an embedded ``jdbc:calcite:`` connection reached in-process
via JPype (approved topology for Phase 1; Phase 3 moves execution into a
recyclable JVM sidecar with an Arrow bridge). Rows are read via typed JDBC
getters and returned as the backend-neutral ``QueryResult``; the wire layer
encodes them to pg protocol.

The embedded connection is kept warm across queries (PGW-032). Statement
execution is serialized with a lock because a single JDBC Connection is not
thread-safe; concurrency is a Phase 3/5 concern (JVM sidecar + pooling).
"""

from __future__ import annotations

import datetime
import decimal
import logging
import os
import threading
import time
from typing import Callable, List, Optional, Tuple

from pgwire_calcite import normalize
from pgwire_calcite.admission import AdmissionPolicy
from pgwire_calcite.backend import (
    CANCELED_BY_TIMEOUT,
    CANCELED_CLIENT_GONE,
    CANCELED_SERVER_BUSY,
    LANE_PROBE,
    LANE_USER,
    QueryCanceled,
)
from pgwire_calcite.classpath import resolve_classpath
from pgwire_calcite.dialect import transpile_pg_to_calcite
from pgwire_calcite.types import QueryResult

log = logging.getLogger(__name__)

_CALCITE_DRIVER = "org.apache.calcite.jdbc.Driver"


def _attach_current_thread_to_jvm() -> None:
    """Make the calling thread able to call Java.

    Cancellation always arrives on a thread that never executed a query — the
    connection thread serving a CancelRequest, or the statement_timeout watchdog
    timer — so it may not be attached to the JVM yet.
    """
    import jpype

    if not jpype.java.lang.Thread.isAttached():
        jpype.java.lang.Thread.attachAsDaemon()


#: Exit status when a cancelled statement never returns: the process is wedged.
EXIT_STUCK_STATEMENT = 3

#: How long the wedge report waits for the Java thread dump before exiting anyway —
#: the dump itself cannot complete when the JVM is what is stalled.
_THREAD_DUMP_TIMEOUT_S = 10.0


def java_thread_dump() -> str:
    """Every JVM thread's state, held/awaited monitor and full stack."""
    import jpype

    _attach_current_thread_to_jvm()
    mx = jpype.JClass("java.lang.management.ManagementFactory").getThreadMXBean()
    out = []
    for info in mx.dumpAllThreads(True, True):
        head = f'"{info.getThreadName()}" {info.getThreadState()}'
        if info.getLockName() is not None:
            head += f" on {info.getLockName()}"
        if info.getLockOwnerName() is not None:
            head += f" owned by \"{info.getLockOwnerName()}\""
        out.append(head)
        out.extend(f"    at {frame}" for frame in info.getStackTrace())
    return "\n".join(out)


def exit_wedged(reason: str, grace_ms: int) -> None:
    """Log a cancelled statement that never returned, with the Java threads, and exit.

    The statement still holds the shared connection lock, so every later statement
    on this lane can only queue and time out; the process is useless until it is
    replaced. Exiting lets the client that spawned it start a fresh server.
    """
    log.error(
        "A statement cancelled for '%s' has not returned %dms later; it still holds the "
        "shared Calcite connection, so this server can no longer run queries. Exiting "
        "with status %d so a fresh server replaces it.",
        reason, grace_ms, EXIT_STUCK_STATEMENT,
    )
    dump: List[str] = []

    def _dump() -> None:
        try:
            dump.append(java_thread_dump())
        except Exception:  # noqa: BLE001 - reported, then the exit proceeds regardless
            log.exception("Java thread dump failed")

    dumper = threading.Thread(target=_dump, name="pgwire-wedge-dump", daemon=True)
    dumper.start()
    dumper.join(_THREAD_DUMP_TIMEOUT_S)
    if dump:
        log.error("Java threads at the wedge:\n%s", dump[0])
    elif dumper.is_alive():
        log.error(
            "Java thread dump did not complete within %.0fs: the JVM itself is stalled",
            _THREAD_DUMP_TIMEOUT_S,
        )
    logging.shutdown()
    os._exit(EXIT_STUCK_STATEMENT)


def _match_name(name: str, candidates: List[str]) -> Optional[str]:
    """``name`` among ``candidates``: exact, else the one case-insensitive match."""
    if name in candidates:
        return name
    folded = [c for c in candidates if c.lower() == name.lower()]
    return folded[0] if len(folded) == 1 else None


def _plain_key(key):
    """A row key from the JVM as a Python value. JPype's boxed numbers are Python numbers
    already; anything else (a Java String arrives as ``str``) is taken as text."""
    if isinstance(key, bool):
        return str(key)
    if isinstance(key, int):
        return int(key)
    if isinstance(key, float):
        return float(key)
    return str(key)


class InFlightStatement:
    """One session's currently-executing JDBC Statement, cancellable from anywhere.

    ``java.sql.Statement.cancel`` is defined to be called from a *different*
    thread than the one blocked in ``execute``, which is exactly the pgwire
    CancelRequest shape: a second connection asks the server to abort the first
    connection's running query.

    A cancel only asks the engine to stop; work outside the engine's cancel checks
    (seen live: a DuckDB-pushed scan held the lock 8+ minutes past a 30s timeout)
    never returns. Once cancelled, the statement gets ``cancel_grace_ms`` to
    return before the process is declared wedged (``exit_wedged``).
    """

    #: How long a cancelled statement may take to return before the process is
    #: declared wedged, in milliseconds; 0 = wait forever. Set once at startup by
    #: the launcher (--cancel-grace-ms).
    cancel_grace_ms: int = 60000

    def __init__(self, stmt) -> None:
        self._stmt = stmt
        self._lock = threading.Lock()
        self._returned = threading.Event()
        #: PG wording for why this statement was cancelled; None while it runs normally.
        self.reason: Optional[str] = None

    def cancel(self, reason: str) -> bool:
        """Cancel the statement once. Returns False if it was already cancelled."""
        with self._lock:
            if self.reason is not None:
                return False
            self.reason = reason
        if self.cancel_grace_ms:
            # Started before Statement.cancel, which can itself block on a wedged engine.
            threading.Thread(
                target=self._await_return, args=(reason,), name="pgwire-cancel-grace", daemon=True
            ).start()
        _attach_current_thread_to_jvm()
        self._stmt.cancel()
        return True

    def finish(self) -> None:
        """The statement returned (normally, by error, or by cancel) and released the lock."""
        self._returned.set()

    def _await_return(self, reason: str) -> None:
        grace_ms = self.cancel_grace_ms
        if not self._returned.wait(grace_ms / 1000.0):
            exit_wedged(reason, grace_ms)


class InFlightRegistry:
    """session key -> the statement that session is running right now (PGW-050).

    Only one statement per session can be in flight: the wire protocol executes a
    session's statements one at a time, and the backend serializes on its own
    connection lock.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._by_session: dict = {}

    def begin(self, session_key: str, stmt) -> InFlightStatement:
        handle = InFlightStatement(stmt)
        with self._lock:
            self._by_session[session_key] = handle
        return handle

    def end(self, session_key: str, handle: InFlightStatement) -> None:
        handle.finish()
        with self._lock:
            if self._by_session.get(session_key) is handle:
                del self._by_session[session_key]

    def discard(self, session_key: str) -> None:
        """Drop the session's entry outright (DISCARD ALL, session teardown)."""
        with self._lock:
            self._by_session.pop(session_key, None)

    def active_sessions(self) -> set:
        """Session keys with a statement in flight right now."""
        with self._lock:
            return set(self._by_session)

    def cancel(self, session_key: str, reason: str) -> bool:
        """Cancel the session's in-flight statement. False if it has none."""
        with self._lock:
            handle = self._by_session.get(session_key)
        if handle is None:
            return False
        return handle.cancel(reason)


#: Process-wide registry; the wire layer's CancelRequest branch reads it.
IN_FLIGHT = InFlightRegistry()


class CancelScope:
    """Arms cancellation + the statement_timeout watchdog around one execution.

    Calcite's JDBC driver accepts ``Statement.setQueryTimeout`` but does not act
    on it (verified against calcite/avatica: a 1s timeout let a 156s query run to
    completion), so the timeout is enforced here by a watchdog that calls the same
    ``Statement.cancel`` the CancelRequest path uses. ``setQueryTimeout`` is still
    set so a future driver that honors it agrees with us.
    """

    #: How often a queued statement re-checks that its client is still connected.
    _LIVENESS_POLL_S = 0.5

    #: Server-wide bound on the wait for the connection lock, in milliseconds; 0 =
    #: unbounded. Applies to sessions with no statement_timeout too, so a queue of
    #: abandoned or slow scans cannot starve every later statement indefinitely.
    #: Set once at startup by the launcher (--max-queue-wait-ms).
    max_queue_wait_ms: int = 120000

    def __init__(
        self,
        session_key: Optional[str],
        timeout_ms: int,
        client_gone: Optional[Callable[[], bool]] = None,
        max_queue_wait_ms: Optional[int] = None,
    ) -> None:
        self._session_key = session_key
        self._client_gone = client_gone
        if max_queue_wait_ms is not None:
            self.max_queue_wait_ms = max(0, int(max_queue_wait_ms))
        self._timeout_ms = max(0, int(timeout_ms))
        self._handle: Optional[InFlightStatement] = None
        self._timer: Optional[threading.Timer] = None

    def arm(self, stmt) -> None:
        if self._timeout_ms:
            # JDBC takes whole seconds; round up so a sub-second timeout is not
            # reported to the driver as "no timeout".
            stmt.setQueryTimeout(-(-self._timeout_ms // 1000))
        if self._session_key is None:
            self._handle = InFlightStatement(stmt)  # timeout-only: nothing to look up
        else:
            self._handle = IN_FLIGHT.begin(self._session_key, stmt)
        if self._timeout_ms:
            handle = self._handle
            self._timer = threading.Timer(
                self._timeout_ms / 1000.0, handle.cancel, args=(CANCELED_BY_TIMEOUT,)
            )
            self._timer.daemon = True
            self._timer.start()

    def acquire(self, lock) -> None:
        """Take the shared connection lock, charging the wait to statement_timeout and the server-wide queue bound.

        The watchdog only starts once a statement is running, so without this a
        statement queued behind slow ones waits unboundedly and can starve
        every other session on the single shared connection. While queued, a
        statement whose client has disconnected is dropped rather than run to
        completion for nobody.
        """
        now = time.monotonic()
        timeout_deadline = now + self._timeout_ms / 1000.0 if self._timeout_ms else None
        busy_deadline = (
            now + self.max_queue_wait_ms / 1000.0 if self.max_queue_wait_ms > 0 else None
        )
        deadlines = [d for d in (timeout_deadline, busy_deadline) if d is not None]
        deadline = min(deadlines) if deadlines else None
        while True:
            wait = self._LIVENESS_POLL_S if self._client_gone is not None else None
            if deadline is not None:
                remaining = deadline - time.monotonic()
                wait = max(0.0, remaining) if wait is None else min(wait, max(0.0, remaining))
            got = lock.acquire() if wait is None else lock.acquire(timeout=wait)
            if got:
                return
            if self._client_gone is not None and self._client_gone():
                raise QueryCanceled(CANCELED_CLIENT_GONE)
            if deadline is not None and time.monotonic() >= deadline:
                if timeout_deadline is not None and deadline == timeout_deadline:
                    raise QueryCanceled(CANCELED_BY_TIMEOUT)
                raise QueryCanceled(CANCELED_SERVER_BUSY)

    def disarm(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        if self._session_key is not None and self._handle is not None:
            IN_FLIGHT.end(self._session_key, self._handle)
        elif self._handle is not None:
            self._handle.finish()
        self._handle = None

    def raise_if_canceled(self) -> None:
        """Turn an engine-level failure into SQLSTATE 57014 when we caused it."""
        handle = self._handle
        if handle is not None and handle.reason is not None:
            raise QueryCanceled(handle.reason)


class CalciteBackend:
    """Embedded Calcite JDBC backend reached via JPype."""

    #: Rejects unfiltered scans of large tables before they reach the shared connection.
    #: Set once at startup by the launcher; None = no admission control.
    admission: Optional[AdmissionPolicy] = None

    def __init__(
        self,
        model_path: Optional[str] = None,
        classpath: Optional[List[str]] = None,
        lex: str = "ORACLE",
        fun: str = "standard",
        default_schema: Optional[str] = None,
        extra_props: Optional[dict] = None,
        jvm_args: Optional[List[str]] = None,
        batch_size: int = 1024,
        extensions: Optional[set] = None,
    ) -> None:
        self._model_path = model_path
        self._extensions = set(extensions or ())
        self._classpath = resolve_classpath(classpath)
        self._lex = lex
        self._fun = fun
        # PostGIS surface (PGW-048): add Calcite's spatial function library so ST_*
        # resolve. Function names already match PostGIS; only `fun` needs spatial.
        if "postgis" in self._extensions and "spatial" not in (self._fun or "").lower():
            self._fun = f"{self._fun},spatial" if self._fun else "spatial"
        self._default_schema = default_schema
        self._extra_props = dict(extra_props or {})
        self._jvm_args = list(jvm_args or [])
        self._batch_size = int(batch_size)
        self._conn = None
        self._lock = threading.RLock()
        # Reserved lane for health/metadata probes: its own JDBC connection and lock,
        # so a probe never queues behind a user scan on the main connection.
        self._probe_conn = None
        self._probe_lock = threading.RLock()
        self._Types = None
        self._stats_rewriter = None
        self._start_jvm()
        self._connect()

    # --- lifecycle ------------------------------------------------------------

    #: Required for Arrow off-heap memory on Java 17+ (arrow-jdbc / arrow-memory).
    _ARROW_ADD_OPENS = "--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED"
    #: Bound Calcite's planner/metadata caches (PGW-039) so long-running JVMs don't
    #: grow unbounded. These have defaults, but we pin them explicitly.
    _CACHE_BOUNDS = (
        "-Dcalcite.metadata.handler.cache.maximum.size=1000",
        "-Dcalcite.bindable.cache.maxSize=1000",
    )

    def _start_jvm(self) -> None:
        import jpype

        if not jpype.isJVMStarted():
            args = list(self._jvm_args)
            if not any("java.base/java.nio" in a for a in args):
                args.append(self._ARROW_ADD_OPENS)
            for prop in self._CACHE_BOUNDS:
                key = prop.split("=", 1)[0]
                if not any(a.startswith(key) for a in args):
                    args.append(prop)
            jpype.startJVM(*args, classpath=self._classpath, convertStrings=True)
            log.info("[CALCITE] JVM started with %d classpath entries", len(self._classpath))

    def _connect(self) -> None:
        import jpype

        JClass = jpype.JClass
        JClass("java.lang.Class").forName(_CALCITE_DRIVER)
        DriverManager = JClass("java.sql.DriverManager")
        Properties = JClass("java.util.Properties")
        self._Types = JClass("java.sql.Types")

        props = Properties()
        # These MUST agree with normalize.py (single source of truth, PGW-016).
        props.setProperty("lex", self._lex)
        props.setProperty("fun", self._fun)
        props.setProperty("caseSensitive", "false")
        if self._model_path:
            props.setProperty("model", self._model_path)
        if self._default_schema:
            props.setProperty("schema", self._default_schema)
        # Arbitrary JDBC connection properties, mirroring what the Calcite/Avatica
        # driver already accepts (e.g. conformance, timeZone, typeSystem). Applied
        # last so an operator override wins.
        for key, value in self._extra_props.items():
            props.setProperty(str(key), str(value))

        self._stats_rewriter = self._resolve_stats_rewriter(JClass)
        self._conn = DriverManager.getConnection("jdbc:calcite:", props)
        # The model's schema is shared across connections, so this open is cheap.
        self._probe_conn = DriverManager.getConnection("jdbc:calcite:", props)
        log.info("[CALCITE] connected (model=%s, lex=%s)", self._model_path, self._lex)

    @staticmethod
    def _resolve_stats_rewriter(JClass):
        """The file adapter's pre-parse rewrite for corr()/regr_*() (reserved Calcite words).

        The file adapter registers these aggregates under non-reserved ``agg_*`` names and
        relies on this rewrite to reach them; GovDataDriver applies it on the embedded path,
        but this backend opens ``jdbc:calcite:`` directly, so it must apply it itself. The
        class exists only when the file adapter is on the classpath; other models have no
        such aggregates to reach.
        """
        name = "org.apache.calcite.adapter.file.duckdb.DuckDBSqlRewriter"
        try:
            return JClass(name)
        except TypeError:
            log.info("[CALCITE] %s not on classpath; corr()/regr_*() rewrite disabled", name)
            return None

    @property
    def extensions(self) -> frozenset:
        """Enabled extension surfaces — what pg_extension advertises (PGW-046)."""
        return frozenset(self._extensions)

    def function_library(self) -> str:
        """The connection's ``fun`` library list, the source for pg_proc (PGW-051)."""
        return self._fun

    def table_row_count(self, schema: str, table: str) -> int:
        """Exact ``COUNT(*)`` for one table, for pg_class.reltuples (PGW-051).

        Runs on the JDBC connection directly rather than through ``execute_sql`` so no
        PG->Calcite transpile stands between the catalog and the count. Called once per
        table per catalog build (the catalog DB is memoized), never per query.
        """
        if self._conn is None:
            raise RuntimeError("Calcite connection is not open")
        quoted = f'"{schema.replace(chr(34), chr(34) * 2)}"."{table.replace(chr(34), chr(34) * 2)}"'
        with self._lock:
            stmt = self._conn.createStatement()
            try:
                rs = stmt.executeQuery(f"SELECT COUNT(*) FROM {quoted}")
                if not bool(rs.next()):
                    raise RuntimeError(f"COUNT(*) on {quoted} returned no row")
                return int(rs.getLong(1))
            finally:
                stmt.close()

    @property
    def connection(self):
        """The embedded java.sql.Connection (used by catalog_populate, Phase 2)."""
        return self._conn

    def ready(self) -> bool:
        with self._lock:
            return self._conn is not None and not bool(self._conn.isClosed())

    def lane(self, name: str):
        """Return the (connection, lock) pair serving ``name`` (``user`` or ``probe``)."""
        if name == LANE_PROBE:
            return self._probe_conn, self._probe_lock
        if name == LANE_USER:
            return self._conn, self._lock
        raise ValueError(f"unknown execution lane {name!r}")

    def cancel_session(self, session_key: str, reason: str) -> bool:
        """Cancel the statement this session is running in-process (PGW-050)."""
        return IN_FLIGHT.cancel(session_key, reason)

    def discard_session(self, session_key: str) -> None:
        """Drop the session's registry entry — teardown, DISCARD ALL (PGW-052)."""
        IN_FLIGHT.discard(session_key)

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None
        with self._probe_lock:
            if self._probe_conn is not None:
                self._probe_conn.close()
                self._probe_conn = None

    # --- execution ------------------------------------------------------------

    def execute_sql(
        self,
        sql: str,
        role_id: str,
        params: Optional[list] = None,
        stream: bool = False,
        session_key: Optional[str] = None,
        timeout_ms: int = 0,
        lane: str = LANE_USER,
        client_gone: Optional[Callable[[], bool]] = None,
    ) -> QueryResult:
        del role_id, params  # params already substituted upstream (server._substitute_params)
        if self.admission is not None:
            self.admission.check(sql)
        calcite_sql = transpile_pg_to_calcite(
            sql,
            json_enabled=("json" in self._extensions),
            vector_enabled=("vector" in self._extensions),
        )
        if self._stats_rewriter is not None:
            calcite_sql = str(self._stats_rewriter.rewrite(calcite_sql))
        log.debug("[CALCITE] PG=%r -> CALCITE=%r", sql[:200], calcite_sql[:200])
        conn, lock = self.lane(lane)
        if conn is None:
            raise RuntimeError("Calcite connection is not open")
        scope = CancelScope(session_key, timeout_ms, client_gone)
        if stream:
            # Arrow batch-streaming path (PGW-019/020/022): the generator holds
            # the lock + JVM/Arrow resources and releases them when exhausted or
            # closed (client disconnect / LIMIT-few cancels the query).
            from pgwire_calcite import arrow_bridge

            names, labels, batches = arrow_bridge.stream_query_batches(
                conn, lock, calcite_sql, self._batch_size, cancel_scope=scope
            )
            return QueryResult(column_names=names, column_types=labels, row_batches=batches)
        # Materialized path (direct/programmatic use, tests): typed JDBC row reads.
        scope.acquire(lock)
        try:
            stmt = conn.createStatement()
            scope.arm(stmt)
            try:
                has_rs = bool(stmt.execute(calcite_sql))
                if not has_rs:
                    return QueryResult(rows=[], column_names=[], column_types=None)
                rs = stmt.getResultSet()
                return self._read_result(rs)
            except BaseException:
                scope.raise_if_canceled()
                raise
            finally:
                scope.disarm()
                stmt.close()
        finally:
            lock.release()

    def execute_update(
        self,
        sql: str,
        session_key: Optional[str] = None,
        timeout_ms: int = 0,
        lane: str = LANE_USER,
        client_gone: Optional[Callable[[], bool]] = None,
    ) -> int:
        """Execute one INSERT/UPDATE/DELETE and return the number of rows it affected.

        The adapter decides what a write means (the Salesforce and SharePoint adapters send
        it to their service as the statement runs); a table that is not modifiable is
        rejected by Calcite's validator. Cancellation and ``timeout_ms`` behave as for
        :meth:`execute_sql`.
        """
        calcite_sql = transpile_pg_to_calcite(
            sql,
            json_enabled=("json" in self._extensions),
            vector_enabled=("vector" in self._extensions),
        )
        log.debug("[CALCITE] PG=%r -> CALCITE=%r", sql[:200], calcite_sql[:200])
        scope = CancelScope(session_key, timeout_ms, client_gone)
        return self.run_update(calcite_sql, scope, lane)[0]

    def execute_insert(
        self,
        sql: str,
        table_ref: Tuple[str, str],
        session_key: Optional[str] = None,
        timeout_ms: int = 0,
        lane: str = LANE_USER,
        client_gone: Optional[Callable[[], bool]] = None,
    ) -> Tuple[int, list]:
        """Execute one INSERT into ``table_ref`` (schema, table); return its row count and the
        keys of the rows it created, in creation order.

        The keys are read under the same lock as the INSERT, so they are this statement's
        and no other session's. Raises SQLSTATE 0A000 if the table does not report keys.
        """
        calcite_sql = transpile_pg_to_calcite(
            sql,
            json_enabled=("json" in self._extensions),
            vector_enabled=("vector" in self._extensions),
        )
        scope = CancelScope(session_key, timeout_ms, client_gone)
        count, keys = self.run_update(calcite_sql, scope, lane, keys_of=table_ref)
        return count, keys or []

    def key_column(
        self,
        table_ref: Tuple[str, str],
        session_key: Optional[str] = None,
        timeout_ms: int = 0,
        lane: str = LANE_USER,
        client_gone: Optional[Callable[[], bool]] = None,
    ) -> str:
        """Name of the column that identifies a row of ``table_ref`` (schema, table).

        Raises SQLSTATE 0A000 if the table does not name one — it cannot answer RETURNING.
        """
        conn, lock = self.lane(lane)
        if conn is None:
            raise RuntimeError("Calcite connection is not open")
        scope = CancelScope(session_key, timeout_ms, client_gone)
        scope.acquire(lock)
        try:
            return str(self._keyed_table(conn, table_ref).getKeyColumn())
        finally:
            lock.release()

    def run_update(
        self,
        calcite_sql: str,
        scope: "CancelScope",
        lane: str = LANE_USER,
        keys_of: Optional[Tuple[str, str]] = None,
    ) -> Tuple[int, Optional[list]]:
        """Run an already-transpiled INSERT/UPDATE/DELETE under ``scope``.

        Returns its row count and, when ``keys_of`` names the INSERT's target table, the keys
        of the rows it created. The seam the bridge's Calcite child uses: the pgwire side
        transpiles, the child runs.
        """
        conn, lock = self.lane(lane)
        if conn is None:
            raise RuntimeError("Calcite connection is not open")
        scope.acquire(lock)
        try:
            table = None
            if keys_of is not None:
                table = self._keyed_table(conn, tuple(keys_of))
                table.takeInsertedKeys()  # discard keys of earlier inserts nobody asked for
            stmt = conn.createStatement()
            scope.arm(stmt)
            try:
                count = int(stmt.executeUpdate(calcite_sql))
            except BaseException:
                scope.raise_if_canceled()
                raise
            finally:
                scope.disarm()
                stmt.close()
            keys = None if table is None else [_plain_key(k) for k in table.takeInsertedKeys()]
            return count, keys
        finally:
            lock.release()

    def _keyed_table(self, conn, table_ref: Tuple[str, str]):
        """The Calcite table ``table_ref`` names, which must name its key column and report
        the keys it inserts (``getKeyColumn()`` / ``takeInsertedKeys()``, looked up by name —
        the adapters share no interface)."""
        from pgwire_calcite.backend import PgProtocolError

        table = self._lookup_table(conn, table_ref)
        if not (hasattr(table, "getKeyColumn") and hasattr(table, "takeInsertedKeys")):
            raise PgProtocolError(
                "0A000",
                f'RETURNING is not supported on table "{table_ref[1]}": '
                "its adapter does not report row keys",
            )
        return table

    def _lookup_table(self, conn, table_ref: Tuple[str, str]):
        """Resolve (schema, table) in the connection's root schema; names match exactly, or
        case-insensitively when that is unambiguous. An empty schema is the default schema."""
        import jpype

        from pgwire_calcite.backend import PgProtocolError

        schema_name, table_name = table_ref
        calcite = conn.unwrap(jpype.JClass("org.apache.calcite.jdbc.CalciteConnection"))
        root = calcite.getRootSchema()
        if not schema_name:
            schema_name = str(conn.getSchema() or "")
        resolved = _match_name(schema_name, [str(n) for n in root.getSubSchemaNames()])
        schema = root.getSubSchema(resolved) if resolved is not None else None
        if schema is None:
            raise PgProtocolError("3F000", f'schema "{schema_name}" does not exist')
        resolved = _match_name(table_name, [str(n) for n in schema.getTableNames()])
        table = schema.getTable(resolved) if resolved is not None else None
        if table is None:
            raise PgProtocolError("42P01", f'relation "{table_name}" does not exist')
        return table

    def _read_result(self, rs) -> QueryResult:
        md = rs.getMetaData()
        ncols = int(md.getColumnCount())
        names: List[str] = []
        type_names: List[str] = []
        jdbc_types: List[int] = []
        for i in range(1, ncols + 1):
            names.append(normalize.pg_column_label(str(md.getColumnLabel(i))))
            type_names.append(str(md.getColumnTypeName(i)))
            jdbc_types.append(int(md.getColumnType(i)))
        duck_labels = [normalize.duckdb_label(t) for t in type_names]

        rows: List[tuple] = []
        while bool(rs.next()):
            row = tuple(self._coerce(rs, i, jdbc_types[i - 1]) for i in range(1, ncols + 1))
            rows.append(row)
        return QueryResult(rows=rows, column_names=names, column_types=duck_labels)

    def _coerce(self, rs, i: int, jdbc_type: int):
        """Coerce one JDBC cell to a Python value using the column's java.sql.Types.

        Deterministic, type-directed conversion — not a silent fallback. Unknown
        SQL types are read as strings and reported as text (see normalize.py).
        """
        T = self._Types
        if jdbc_type in (int(T.INTEGER), int(T.SMALLINT), int(T.TINYINT)):
            v = rs.getInt(i)
            return None if bool(rs.wasNull()) else int(v)
        if jdbc_type == int(T.BIGINT):
            v = rs.getLong(i)
            return None if bool(rs.wasNull()) else int(v)
        if jdbc_type in (int(T.REAL), int(T.FLOAT), int(T.DOUBLE)):
            v = rs.getDouble(i)
            return None if bool(rs.wasNull()) else float(v)
        if jdbc_type in (int(T.DECIMAL), int(T.NUMERIC)):
            bd = rs.getBigDecimal(i)
            return None if bool(rs.wasNull()) else decimal.Decimal(str(bd))
        if jdbc_type in (int(T.BOOLEAN), int(T.BIT)):
            v = rs.getBoolean(i)
            return None if bool(rs.wasNull()) else bool(v)
        if jdbc_type in (int(T.BINARY), int(T.VARBINARY), int(T.LONGVARBINARY)):
            # bytea (OID 17) is what normalize.py advertises for these; the wire and
            # the COPY codecs require Python bytes, never the hex string getString()
            # would hand back (PGW-016/021).
            v = rs.getBytes(i)
            return None if bool(rs.wasNull()) else bytes(v)
        if jdbc_type == int(T.DATE):
            s = rs.getString(i)
            return None if bool(rs.wasNull()) else datetime.date.fromisoformat(str(s))
        if jdbc_type == int(T.TIME):
            s = rs.getString(i)
            return None if bool(rs.wasNull()) else datetime.time.fromisoformat(str(s))
        if jdbc_type in (int(T.TIMESTAMP), getattr(T, "TIMESTAMP_WITH_TIMEZONE", T.TIMESTAMP)):
            s = rs.getString(i)
            if bool(rs.wasNull()):
                return None
            return datetime.datetime.fromisoformat(str(s).replace(" ", "T"))
        s = rs.getString(i)
        return None if bool(rs.wasNull()) else str(s)
