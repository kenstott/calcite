# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""information_schema as a PostgreSQL client expects it, answered by Calcite (PGW-012, PGW-045).

information_schema passes through to Calcite's own InformationSchema (catalog.py explains why it is
not intercepted: the in-memory reimplementation counted rows for every table on first touch). What
Calcite answers is not what a PG client may see, though: its own metadata schemas and tables sit
beside the model's, data_type is a Calcite type name (INTEGER, DOUBLE), and nothing is scoped to the
connected role. So each reference to information_schema.{schemata,tables,views,columns} is rewritten
into a derived table over Calcite's view that

  * keeps only the objects of the role's catalog (``state.contexts.get(role)``): the model's
    tables the role is granted, which drops Calcite's internals and enforces PGW-045 in one rule;
  * reports ``data_type`` as PostgreSQL's display name (the same mapping pg_catalog uses).

The rewrite is SQL, not a filter on the answer, so WHERE, ORDER BY, joins and aggregates over these
views (the connectors' ``count(*) ... schemata`` identity probe among them) see only the rows the
role may see, and Calcite still pushes the outer predicates down.
"""

from __future__ import annotations

from typing import Callable, Iterable

import sqlglot
import sqlglot.expressions as exp

from pgwire_calcite.catalog import _PG_TYPE_DISPLAY_NAMES
from pgwire_calcite.normalize import _DEFAULT_MAPPING, _TYPE_TABLE

SCHEMA = "information_schema"
VIEWS = frozenset({"schemata", "tables", "views", "columns"})


def references(pg_sql: str) -> bool:
    """Whether the statement reads one of the rewritten information_schema views."""
    lowered = pg_sql.lower()
    if SCHEMA not in lowered:
        return False
    tree = sqlglot.parse_one(pg_sql, read="postgres")
    return any(_is_view(t) for t in tree.find_all(exp.Table))


def _is_view(t: exp.Table) -> bool:
    return (t.db or "").lower() == SCHEMA and (t.name or "").lower() in VIEWS


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _varchar(value: str) -> str:
    return f"CAST({_literal(value)} AS VARCHAR)"


def _in_list(values: Iterable[str]) -> str:
    items = sorted(set(values))
    return "(" + ", ".join(_literal(v) for v in items) + ")" if items else "(NULL)"


def _pg_type_case(column: str) -> str:
    """``data_type`` as PG's display name. Longest Calcite names first, so DOUBLE PRECISION is not
    read as DOUBLE nor TIMESTAMP WITH LOCAL TIME ZONE as TIMESTAMP; a size suffix (VARCHAR(20)) and
    Calcite's NOT NULL marker are part of the value, so each name also matches with them. Each
    result is a VARCHAR: Calcite types a CASE over bare literals as CHAR of the longest one, which
    padded 'integer' with trailing blanks."""
    ref = f'UPPER("{column}")'
    whens = []
    for name in sorted(_TYPE_TABLE, key=len, reverse=True):
        mapping = _TYPE_TABLE[name]
        display = _PG_TYPE_DISPLAY_NAMES.get(mapping.pg_typname, mapping.pg_typname)
        whens.append(
            f"WHEN {ref} = {_literal(name)} OR {ref} LIKE {_literal(name + '(%')} "
            f"OR {ref} LIKE {_literal(name + ' %')} THEN {_varchar(display)}"
        )
    default = _PG_TYPE_DISPLAY_NAMES.get(_DEFAULT_MAPPING.pg_typname, _DEFAULT_MAPPING.pg_typname)
    return f"CASE {' '.join(whens)} ELSE {_varchar(default)} END"


def _derived(view: str, visible: list[tuple[str, str]], columns_of: Callable[[], list[str]]) -> str:
    schemas = {s for s, _ in visible}
    if view == "schemata":
        return f"SELECT * FROM {SCHEMA}.schemata WHERE schema_name IN {_in_list(schemas)}"
    keyed = _in_list(f"{s}.{t}" for s, t in visible)
    where = f"table_schema IN {_in_list(schemas)} AND table_schema || '.' || table_name IN {keyed}"
    if view != "columns":
        return f"SELECT * FROM {SCHEMA}.{view} WHERE {where}"
    select = ", ".join(
        f'{_pg_type_case(c)} AS "{c}"' if c.lower() == "data_type" else f'"{c}"'
        for c in columns_of()
    )
    return f"SELECT {select} FROM {SCHEMA}.columns WHERE {where}"


def rewrite(
    pg_sql: str, visible: list[tuple[str, str]], columns_of: Callable[[], list[str]]
) -> str | None:
    """``pg_sql`` with every information_schema view it reads replaced by the role-scoped derived
    table, or None when it reads none. ``visible`` is the role's (schema, table) pairs;
    ``columns_of`` yields Calcite's information_schema.columns column names (asked only when the
    statement reads that view)."""
    tree = sqlglot.parse_one(pg_sql, read="postgres")
    targets = [t for t in tree.find_all(exp.Table) if _is_view(t)]
    if not targets:
        return None
    for t in targets:
        view = t.name.lower()
        alias = t.alias or view
        sub = sqlglot.parse_one(_derived(view, visible, columns_of), read="postgres")
        t.replace(exp.Subquery(this=sub, alias=exp.TableAlias(this=exp.to_identifier(alias))))
    return tree.sql(dialect="postgres")
