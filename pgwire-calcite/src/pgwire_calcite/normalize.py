# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Canonical identifier + type normalization (PGW-016).

This is the ONE place identifier casing/quoting and type<->OID mapping are
defined. It drives BOTH query transpile (Phase 1, via dialect.py) AND catalog /
discovery population (Phase 2, via catalog.py), so discover-then-query clients
(DuckDB/DataGrip) cannot drift — consistency is guaranteed by construction, not
by two code paths agreeing.

Calcite's default lexer is ``Lex.ORACLE``: unquoted identifiers fold to UPPER,
double-quoted identifiers are case-sensitive, and the quote char is ``"``. That
is the behaviour encoded here and asserted against in dialect tests. If a
deployment overrides ``lex``/``fun`` on the Calcite connection, the override must
be reflected here (single source of truth) rather than patched at either caller.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, Optional

# --- Identifier normalization (Calcite Lex.ORACLE) ---------------------------

_UNQUOTED_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def fold_unquoted(identifier: str) -> str:
    """Fold an *unquoted* identifier the way Calcite's Oracle lexer does: UPPER."""
    return identifier.upper()


def needs_quoting(identifier: str) -> bool:
    """True if the identifier must be double-quoted to survive round-trip.

    An identifier needs quoting when it is not a bare word, or when its canonical
    (folded) form differs from the spelling the caller wants preserved.
    """
    if not _UNQUOTED_RE.match(identifier):
        return True
    return identifier != fold_unquoted(identifier)


def quote_identifier(identifier: str) -> str:
    """Return the identifier quoted for Calcite (double quotes, doubled internally)."""
    return '"' + identifier.replace('"', '""') + '"'


def canonical_identifier(identifier: str, was_quoted: bool) -> str:
    """The canonical stored form of an identifier as Calcite would resolve it.

    Unquoted -> folded to UPPER; quoted -> preserved verbatim. This is the form
    the catalog stores so that a later query referencing the same identifier
    resolves to the same object.
    """
    return identifier if was_quoted else fold_unquoted(identifier)


# --- Type <-> PostgreSQL OID mapping -----------------------------------------
#
# The wire layer must present PostgreSQL type OIDs to clients. This table is the
# canonical bridge between Calcite/JDBC SQL type names, PostgreSQL type OIDs, and
# the DuckDB-style type strings the wire encoder (server._duckdb_type_to_bvtype)
# already understands. Phase 2's catalog reuses OID + typname; Phase 1's backend
# reuses ``duckdb`` to label result columns.


@dataclass(frozen=True)
class TypeMapping:
    pg_oid: int
    pg_typname: str
    duckdb: str  # label understood by server._duckdb_type_to_bvtype


# Keyed by an uppercased, size-stripped JDBC/Calcite type name.
_TYPE_TABLE: Dict[str, TypeMapping] = {
    "BOOLEAN": TypeMapping(16, "bool", "BOOLEAN"),
    "TINYINT": TypeMapping(21, "int2", "SMALLINT"),
    "SMALLINT": TypeMapping(21, "int2", "SMALLINT"),
    "INTEGER": TypeMapping(23, "int4", "INTEGER"),
    "INT": TypeMapping(23, "int4", "INTEGER"),
    "BIGINT": TypeMapping(20, "int8", "BIGINT"),
    "REAL": TypeMapping(700, "float4", "FLOAT"),
    "FLOAT": TypeMapping(701, "float8", "DOUBLE"),
    "DOUBLE": TypeMapping(701, "float8", "DOUBLE"),
    "DOUBLE PRECISION": TypeMapping(701, "float8", "DOUBLE"),
    "DECIMAL": TypeMapping(1700, "numeric", "DECIMAL"),
    "NUMERIC": TypeMapping(1700, "numeric", "DECIMAL"),
    "CHAR": TypeMapping(1042, "bpchar", "VARCHAR"),
    "VARCHAR": TypeMapping(1043, "varchar", "VARCHAR"),
    "TEXT": TypeMapping(25, "text", "VARCHAR"),
    "DATE": TypeMapping(1082, "date", "DATE"),
    "TIME": TypeMapping(1083, "time", "TIME"),
    "TIMESTAMP": TypeMapping(1114, "timestamp", "TIMESTAMP"),
    "TIMESTAMP WITH LOCAL TIME ZONE": TypeMapping(1184, "timestamptz", "TIMESTAMP"),
    "VARBINARY": TypeMapping(17, "bytea", "VARCHAR"),
    "BINARY": TypeMapping(17, "bytea", "VARCHAR"),
    "ANY": TypeMapping(25, "text", "VARCHAR"),
}

#: default for unknown types — reported as text, never silently dropped
_DEFAULT_MAPPING = TypeMapping(25, "text", "VARCHAR")


def _strip_type(sql_type: str) -> str:
    """Normalize a JDBC/Calcite type name: upper, drop precision/scale and array []."""
    t = sql_type.strip().upper()
    t = re.sub(r"\s*\(.*\)\s*", "", t)  # DECIMAL(10,2) -> DECIMAL
    t = t.replace("[]", "").strip()
    return t


def type_mapping(sql_type: str) -> TypeMapping:
    """Map a Calcite/JDBC SQL type name to its canonical PG-OID/duckdb mapping."""
    return _TYPE_TABLE.get(_strip_type(sql_type), _DEFAULT_MAPPING)


def duckdb_label(sql_type: str) -> str:
    """DuckDB-style type label for the wire encoder (server side)."""
    return type_mapping(sql_type).duckdb


def pg_oid(sql_type: str) -> int:
    return type_mapping(sql_type).pg_oid


def pg_typname(sql_type: str) -> str:
    return type_mapping(sql_type).pg_typname
