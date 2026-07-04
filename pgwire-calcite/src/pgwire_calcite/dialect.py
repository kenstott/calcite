# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Bidirectional sqlglot Calcite dialect (PGW-017).

Derives from sqlglot's Oracle dialect — Calcite's default lexer is ``Lex.ORACLE``
(unquoted identifiers fold to UPPER, double-quote quoting, case-sensitive when
quoted), which is the §7 evidence for the base choice. Oracle's *type* rendering
(NUMBER/VARCHAR2/CLOB) is NOT what Calcite wants, so the Generator's TYPE_MAPPING
is overridden back to standard-SQL/Calcite names.

``transpile_pg_to_calcite`` is the single query-side entrypoint: parse as
PostgreSQL, run the PG-only transform layer (transforms.py), then generate
Calcite SQL. Identifier/type normalization is owned by normalize.py so that this
path and the Phase 2 catalog path cannot drift (PGW-016).
"""

from __future__ import annotations

from sqlglot import exp, parse_one
from sqlglot.dialects.oracle import Oracle

from pgwire_calcite import transforms

# Re-export so callers depend on one module.
UnsupportedConstruct = transforms.UnsupportedConstruct


class Calcite(Oracle):
    """sqlglot dialect for Apache Calcite (Lex.ORACLE, standard-SQL types)."""

    class Generator(Oracle.Generator):
        # Undo Oracle-specific type spellings; Calcite uses standard SQL names.
        TYPE_MAPPING = {
            **Oracle.Generator.TYPE_MAPPING,
            exp.DataType.Type.TEXT: "VARCHAR",
            exp.DataType.Type.VARCHAR: "VARCHAR",
            exp.DataType.Type.NVARCHAR: "VARCHAR",
            exp.DataType.Type.TINYINT: "TINYINT",
            exp.DataType.Type.SMALLINT: "SMALLINT",
            exp.DataType.Type.INT: "INTEGER",
            exp.DataType.Type.BIGINT: "BIGINT",
            exp.DataType.Type.FLOAT: "REAL",
            exp.DataType.Type.DOUBLE: "DOUBLE",
            exp.DataType.Type.DECIMAL: "DECIMAL",
            exp.DataType.Type.BOOLEAN: "BOOLEAN",
            exp.DataType.Type.VARBINARY: "VARBINARY",
            exp.DataType.Type.BINARY: "BINARY",
            exp.DataType.Type.DATETIME: "TIMESTAMP",
            exp.DataType.Type.TIMESTAMP: "TIMESTAMP",
        }


def transpile_pg_to_calcite(sql: str) -> str:
    """Transpile one PostgreSQL statement to Calcite SQL.

    Raises ``UnsupportedConstruct`` for PG-only constructs with no safe mapping
    (PGW-018); never silently mistranslates.
    """
    tree = parse_one(sql, read="postgres")
    tree = transforms.apply(tree)
    return tree.sql(dialect=Calcite)


def transpile_identifier_case_probe(sql: str) -> str:
    """Helper for tests/inspection: show how identifiers render under Calcite."""
    return parse_one(sql, read="postgres").sql(dialect=Calcite, identify=False)
