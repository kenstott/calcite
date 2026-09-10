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
    # TINYINT/SMALLINT are advertised as int4, not int2: the wire encoder's
    # narrowest integer BVType is INTEGER (4 bytes), so an int2 OID here would
    # make binary COPY hand a client 4 bytes under a 2-byte type and misalign
    # every following field (PGW-016/021). Widening is lossless.
    "TINYINT": TypeMapping(23, "int4", "INTEGER"),
    "SMALLINT": TypeMapping(23, "int4", "INTEGER"),
    "INTEGER": TypeMapping(23, "int4", "INTEGER"),
    "INT": TypeMapping(23, "int4", "INTEGER"),
    "BIGINT": TypeMapping(20, "int8", "BIGINT"),
    # REAL is advertised as float8 for the same width reason: BVType.FLOAT encodes
    # 8 bytes, so a float4 OID would misalign a binary COPY stream (PGW-016/021).
    "REAL": TypeMapping(701, "float8", "DOUBLE"),
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
    # Calcite's TIMESTAMP WITH LOCAL TIME ZONE reaches the wire as a naive datetime
    # already normalized to the session zone, and the wire encoder has one timestamp
    # BVType (OID 1114). Advertising 1184 in the catalog while sending 1114 in
    # RowDescription made discover-then-query clients see two different types for one
    # column; the binary layout is identical, so both sides say 1114 (PGW-016/021).
    "TIMESTAMP WITH LOCAL TIME ZONE": TypeMapping(1114, "timestamp", "TIMESTAMP"),
    # Binary columns are bytea (OID 17) everywhere: the backend coerces JDBC byte[]
    # to Python bytes, BLOB is the label the wire encoder maps to BVType.BYTES, and
    # the catalog derives atttypid 17 from that same label.
    "VARBINARY": TypeMapping(17, "bytea", "BLOB"),
    "BINARY": TypeMapping(17, "bytea", "BLOB"),
    "LONGVARBINARY": TypeMapping(17, "bytea", "BLOB"),
    "ANY": TypeMapping(25, "text", "VARCHAR"),
}

#: default for unknown types — reported as text, never silently dropped
_DEFAULT_MAPPING = TypeMapping(25, "text", "VARCHAR")


#: Calcite's DatabaseMetaData reports a column's nullability inside TYPE_NAME
#: ("VARBINARY NOT NULL"); the suffix is not part of the type.
_NULLABILITY_SUFFIX_RE = re.compile(r"\s+(NOT\s+NULL|NULL)$")


def _strip_type(sql_type: str) -> str:
    """Normalize a JDBC/Calcite type name: upper, drop precision/scale, [] and nullability."""
    t = sql_type.strip().upper()
    # A space, not "": "TIMESTAMP(0) NOT NULL" must not collapse to "TIMESTAMPNOT NULL".
    t = re.sub(r"\s*\(.*\)\s*", " ", t).strip()  # DECIMAL(10,2) -> DECIMAL
    t = t.replace("[]", "").strip()
    t = _NULLABILITY_SUFFIX_RE.sub("", t).strip()  # VARBINARY NOT NULL -> VARBINARY
    return t


def type_mapping(sql_type: str) -> TypeMapping:
    """Map a Calcite/JDBC SQL type name to its canonical PG-OID/duckdb mapping."""
    return _TYPE_TABLE.get(_strip_type(sql_type), _DEFAULT_MAPPING)


def duckdb_label(sql_type: str) -> str:
    """DuckDB-style type label for the wire encoder (server side)."""
    return type_mapping(sql_type).duckdb


#: JDBC type names that carry no usable type information: Calcite reports these for
#: dynamic/untyped expressions. They are NOT unknown-types-defaulted-to-text — the
#: real pg type has to be read off the data, so the streaming path reports them as
#: "" and the wire layer buffers exactly one batch to infer (PGW-020).
OPAQUE_SQL_TYPES = frozenset({"ANY", "OTHER", "NULL", "JAVA_OBJECT", "STRUCT"})


def is_opaque(sql_type: str) -> bool:
    """True when ResultSetMetaData gives no usable type for this column."""
    return _strip_type(sql_type) in OPAQUE_SQL_TYPES


def stream_type_label(sql_type: str) -> str:
    """Type label for a streamed column: "" when metadata is insufficient."""
    return "" if is_opaque(sql_type) else duckdb_label(sql_type)


def pg_oid(sql_type: str) -> int:
    return type_mapping(sql_type).pg_oid


def pg_typname(sql_type: str) -> str:
    return type_mapping(sql_type).pg_typname


_EXPR_LABEL_RE = re.compile(r"^EXPR\$\d+$")


def pg_column_label(label: str) -> str:
    """Map Calcite's auto-generated expression labels (``EXPR$0``) to the
    PostgreSQL convention (``?column?``), so PG-wire clients see familiar names."""
    return "?column?" if _EXPR_LABEL_RE.match(label or "") else label


# --- Simple-query batch splitting -------------------------------------------


def split_sql_statements(sql: str) -> list[str]:
    """Split a batch into statements on TOP-LEVEL semicolons ONLY, statement-aware.

    Uses sqlglot's tokenizer so a ``;`` inside a string literal, quoted identifier,
    comment, or a dollar-quoted block does NOT mis-split (a naive ``str.split(';')``
    turns ``SELECT 'a;b'`` into two malformed fragments). Original statement text is
    preserved (sliced between top-level semicolon tokens, not re-rendered), so the
    COPY/DDL regex matching in ``handle_query`` sees EXACTLY what executes. Blank
    fragments (a trailing ``;``) are dropped.
    """
    import sqlglot

    tokens = sqlglot.tokenize(sql, read="postgres")
    stmts: list[str] = []
    start = 0
    for tok in tokens:
        if tok.token_type == sqlglot.TokenType.SEMICOLON:
            seg = sql[start : tok.start].strip()
            if seg:
                stmts.append(seg)
            start = tok.end + 1
    tail = sql[start:].strip()
    if tail:
        stmts.append(tail)
    return stmts
