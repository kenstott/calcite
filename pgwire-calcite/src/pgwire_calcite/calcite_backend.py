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
import threading
from typing import List, Optional

from pgwire_calcite import normalize
from pgwire_calcite.classpath import resolve_classpath
from pgwire_calcite.dialect import transpile_pg_to_calcite
from pgwire_calcite.types import QueryResult

log = logging.getLogger(__name__)

_CALCITE_DRIVER = "org.apache.calcite.jdbc.Driver"


class CalciteBackend:
    """Embedded Calcite JDBC backend reached via JPype."""

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
        self._Types = None
        self._start_jvm()
        self._connect()

    # --- lifecycle ------------------------------------------------------------

    #: Required for Arrow off-heap memory on Java 17+ (arrow-jdbc / arrow-memory).
    _ARROW_ADD_OPENS = "--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED"

    def _start_jvm(self) -> None:
        import jpype

        if not jpype.isJVMStarted():
            args = list(self._jvm_args)
            if not any("java.base/java.nio" in a for a in args):
                args.append(self._ARROW_ADD_OPENS)
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

        self._conn = DriverManager.getConnection("jdbc:calcite:", props)
        log.info("[CALCITE] connected (model=%s, lex=%s)", self._model_path, self._lex)

    @property
    def connection(self):
        """The embedded java.sql.Connection (used by catalog_populate, Phase 2)."""
        return self._conn

    def ready(self) -> bool:
        with self._lock:
            return self._conn is not None and not bool(self._conn.isClosed())

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    # --- execution ------------------------------------------------------------

    def execute_sql(
        self,
        sql: str,
        role_id: str,
        params: Optional[list] = None,
        stream: bool = False,
    ) -> QueryResult:
        del role_id, params  # params already substituted upstream (server._substitute_params)
        calcite_sql = transpile_pg_to_calcite(
            sql,
            json_enabled=("json" in self._extensions),
            vector_enabled=("vector" in self._extensions),
        )
        log.debug("[CALCITE] PG=%r -> CALCITE=%r", sql[:200], calcite_sql[:200])
        if self._conn is None:
            raise RuntimeError("Calcite connection is not open")
        if stream:
            # Arrow batch-streaming path (PGW-019/020/022): the generator holds
            # the lock + JVM/Arrow resources and releases them when exhausted or
            # closed (client disconnect / LIMIT-few cancels the query).
            from pgwire_calcite import arrow_bridge

            names, labels, rows = arrow_bridge.stream_query(
                self._conn, self._lock, calcite_sql, self._batch_size
            )
            return QueryResult(rows=rows, column_names=names, column_types=labels)
        # Materialized path (direct/programmatic use, tests): typed JDBC row reads.
        with self._lock:
            stmt = self._conn.createStatement()
            try:
                has_rs = bool(stmt.execute(calcite_sql))
                if not has_rs:
                    return QueryResult(rows=[], column_names=[], column_types=None)
                rs = stmt.getResultSet()
                return self._read_result(rs)
            finally:
                stmt.close()

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
