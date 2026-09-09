# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Projection of Calcite's operator table into the pgwire SQL catalog (PGW-051).

Calcite's ``SqlOperatorTable`` is the single source of truth for what the server
can execute, so it is also the source of truth for what the catalog advertises:
``pg_proc`` plus ``information_schema.routines`` / ``.parameters``. A client
(psql ``\\df``, DBeaver's *Procedures* node, DataGrip's routine tree) reads those
three surfaces and, until now, found them empty even though every standard SQL
function resolved at execution time.

What is and is not known statically
-----------------------------------
Calcite exposes an operator's **name**, **kind**, **category** and **operand
count range** without a call site. It does NOT expose concrete operand or return
types: those come from a ``SqlReturnTypeInference`` evaluated against a bound
call. So the projection reports the PG pseudo-type ``any`` (oid 2276) for
argument and return types rather than inventing a concrete type — "not known
statically" is the truthful answer, and it is what PG itself reports for a
polymorphic signature.
"""

from __future__ import annotations

import logging
import re
from typing import List, NamedTuple, Set

log = logging.getLogger(__name__)

#: pg_catalog namespace oid — where PostgreSQL keeps its built-in functions.
_PG_CATALOG_NS_OID = 11
#: Starting synthetic oid for projected functions (above every static catalog oid
#: this module's siblings assign: 8001+ pg_catalog tables, 9001+ IS views, 16384+
#: user tables, 20000+ constraints, 90000+ indexes).
_FN_OID_BASE = 100000
#: PG's ``any`` pseudo-type. See the module docstring: Calcite has no statically
#: knowable operand/return type for an unbound operator.
_ANY_TYPE_OID = 2276

#: An operator name a SQL client can actually type. Calcite's table also holds
#: internal operators ($SUM0, $HISTOGRAM), punctuation operators (+, =, ||) and
#: multi-word syntax (``IS NOT NULL``); none of those are pg_proc rows.
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class FunctionEntry(NamedTuple):
    """One projected routine, independent of both Calcite and DuckDB."""

    name: str  # lower-cased, PG convention
    nargs: int  # minimum operand count (variadic operators report their minimum)
    is_aggregate: bool


def harvest_operators(fun: str = "standard") -> List[FunctionEntry]:
    """Read Calcite's operator table(s) into ``FunctionEntry`` rows (PGW-051).

    ``fun`` is the connection's ``fun`` property (see ``CalciteBackend``): the
    comma-separated function-library list. ``standard`` is Calcite's built-in
    ``SqlStdOperatorTable``; any further token (e.g. ``spatial``) names a
    ``SqlLibrary`` whose operators the connection also resolves, so the catalog
    must advertise them too. Requires a started JVM.
    """
    import jpype

    JClass = jpype.JClass
    sql_function = JClass("org.apache.calcite.sql.SqlFunction")
    agg_function = JClass("org.apache.calcite.sql.SqlAggFunction")

    operators = list(JClass("org.apache.calcite.sql.fun.SqlStdOperatorTable").instance()
                     .getOperatorList())
    for library in _extra_libraries(fun):
        operators.extend(library.getOperatorList())

    seen: Set[str] = set()
    entries: List[FunctionEntry] = []
    for op in operators:
        if not isinstance(op, sql_function):
            continue
        name = str(op.getName())
        if not _IDENT_RE.match(name):
            continue
        lowered = name.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        count_range = op.getOperandCountRange()
        entries.append(
            FunctionEntry(
                name=lowered,
                nargs=max(int(count_range.getMin()), 0),
                is_aggregate=isinstance(op, agg_function),
            )
        )
    entries.sort(key=lambda e: e.name)
    log.info("[CATALOG] harvested %d Calcite operators for pg_proc (fun=%s)", len(entries), fun)
    return entries


def _extra_libraries(fun: str):
    """``SqlOperatorTable`` per non-standard ``fun`` token, in declaration order.

    A token that does not name a ``SqlLibrary`` is a connection-configuration
    error — ``CalciteBackend`` would already have failed to resolve it — so the
    ``valueOf`` lookup is left to raise rather than skipping the library and
    silently under-reporting the catalog.
    """
    import jpype

    JClass = jpype.JClass
    tokens = [t.strip().upper() for t in (fun or "").split(",") if t.strip()]
    tokens = [t for t in tokens if t != "STANDARD"]
    if not tokens:
        return []
    sql_library = JClass("org.apache.calcite.sql.fun.SqlLibrary")
    factory = JClass("org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory").INSTANCE
    enum_set = JClass("java.util.EnumSet")
    tables = []
    for token in tokens:
        libs = enum_set.of(sql_library.valueOf(token))
        tables.append(factory.getOperatorTable(libs))
    return tables


def populate_functions(db, entries: List[FunctionEntry], database_name: str) -> None:
    """Insert ``entries`` into ``_pg_proc`` / ``_is_routines`` / ``_is_parameters``.

    Pure with respect to Calcite — the JVM work happened in ``harvest_operators``
    — so the row shapes are unit-testable without a backend.
    """
    proc_rows: List[tuple] = []
    routine_rows: List[tuple] = []
    parameter_rows: List[tuple] = []
    oid = _FN_OID_BASE
    for entry in entries:
        prokind = "a" if entry.is_aggregate else "f"
        proc_rows.append(
            (
                oid,
                entry.name,
                _PG_CATALOG_NS_OID,
                10,  # proowner
                12,  # prolang: internal
                1.0,  # procost
                0.0,  # prorows
                0,  # provariadic
                None,  # prosupport
                prokind,
                False,  # prosecdef
                False,  # proleakproof
                False,  # proisstrict
                False,  # proretset
                "i",  # provolatile: immutable is Calcite's contract for a pure operator
                "s",  # proparallel: safe
                entry.nargs,
                0,  # pronargdefaults
                _ANY_TYPE_OID,
                [_ANY_TYPE_OID] * entry.nargs,
                None,  # proallargtypes
                None,  # proargmodes
                None,  # proargnames
                None,  # proargdefaults
                None,  # protrftypes
                entry.name,  # prosrc
                None,  # probin
                None,  # prosqlbody
                None,  # proconfig
                None,  # proacl
            )
        )
        # information_schema.routines excludes aggregates in PostgreSQL (it lists
        # prokind 'f' and 'p' only); mirror that rather than over-reporting.
        if prokind == "f":
            specific = f"{entry.name}_{oid}"
            routine_rows.append(
                _routine_row(database_name, specific, entry.name)
            )
            for position in range(1, entry.nargs + 1):
                parameter_rows.append(
                    _parameter_row(database_name, specific, position)
                )
        oid += 1

    if proc_rows:
        db.executemany(f"INSERT INTO _pg_proc VALUES ({','.join(['?'] * 30)})", proc_rows)
    if routine_rows:
        db.executemany(
            f"INSERT INTO _is_routines VALUES ({','.join(['?'] * 82)})", routine_rows
        )
    if parameter_rows:
        db.executemany(
            f"INSERT INTO _is_parameters VALUES ({','.join(['?'] * 32)})", parameter_rows
        )


#: Column index -> value for the handful of information_schema.routines columns that
#: carry a value here. Everything else is NULL, exactly as PG reports for a built-in.
_ROUTINE_COLUMN_COUNT = 82
_PARAMETER_COLUMN_COUNT = 32


def _routine_row(database_name: str, specific: str, name: str) -> tuple:
    row: List[object] = [None] * _ROUTINE_COLUMN_COUNT
    row[0] = database_name  # specific_catalog
    row[1] = "pg_catalog"  # specific_schema
    row[2] = specific  # specific_name
    row[3] = database_name  # routine_catalog
    row[4] = "pg_catalog"  # routine_schema
    row[5] = name  # routine_name
    row[6] = "FUNCTION"  # routine_type
    # data_type (index 13) stays NULL: the return type is not known without a bound
    # call (module docstring). type_udt_* below name the pseudo-type instead.
    row[28] = database_name  # type_udt_catalog
    row[29] = "pg_catalog"  # type_udt_schema
    row[30] = "any"  # type_udt_name
    row[36] = "EXTERNAL"  # routine_body
    row[39] = "EXTERNAL"  # external_language
    row[40] = "GENERAL"  # parameter_style
    row[41] = "YES"  # is_deterministic
    row[42] = "READS"  # sql_data_access
    row[49] = "INVOKER"  # security_type
    return tuple(row)


def _parameter_row(database_name: str, specific: str, position: int) -> tuple:
    row: List[object] = [None] * _PARAMETER_COLUMN_COUNT
    row[0] = database_name  # specific_catalog
    row[1] = "pg_catalog"  # specific_schema
    row[2] = specific  # specific_name
    row[3] = position  # ordinal_position
    row[4] = "IN"  # parameter_mode
    row[5] = "NO"  # is_result
    row[6] = "NO"  # as_locator
    # parameter_name (7) NULL: Calcite operands are positional.
    # data_type (8) NULL for the same reason routines.data_type is.
    row[23] = database_name  # udt_catalog
    row[24] = "pg_catalog"  # udt_schema
    row[25] = "any"  # udt_name
    row[30] = str(position)  # dtd_identifier
    return tuple(row)
