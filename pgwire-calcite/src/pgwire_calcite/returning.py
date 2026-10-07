# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""INSERT / UPDATE / DELETE ... RETURNING, answered without the engine's help.

Calcite has no RETURNING, so the clause is taken off the statement and the rows are read
back by key around the write:

- INSERT: run it, ask the table which keys it created, select those rows.
- UPDATE: select the keys the WHERE clause matches, run it, select those rows (after the
  write, so the new values are returned even when the update changes a filtered column).
- DELETE: select the rows the WHERE clause matches, then run it.

The table names its key column and reports the keys of the rows it inserts (see
``CalciteBackend.key_column`` / ``execute_insert``); a table that cannot is not RETURNING-capable.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional

#: Alias of the key column appended to an INSERT's read-back, to restore insertion order.
KEY_ALIAS = "__pgwire_key"

#: Keys per read-back SELECT; keeps the IN list well inside an adapter's statement limit.
KEY_CHUNK = 200


@dataclass(frozen=True)
class ReturningPlan:
    """A DML statement with its RETURNING clause taken apart. SQL text is PG dialect."""

    kind: str  # INSERT, UPDATE or DELETE
    write_sql: str  # the statement without RETURNING
    schema: str  # as written; "" when the table is unqualified
    table: str
    table_sql: str  # the table reference, alias included, for a FROM clause
    where_sql: Optional[str]
    select_list: str  # the RETURNING expressions


def plan(pg_sql: str) -> Optional[ReturningPlan]:
    """The plan for a DML statement that has RETURNING; None when it has none.

    Raises ``ValueError`` for a RETURNING statement that cannot be answered by reading rows
    back from its one target table.
    """
    import sqlglot
    import sqlglot.expressions as exp

    tree = sqlglot.parse_one(pg_sql, read="postgres")
    returning = tree.args.get("returning")
    if returning is None:
        return None
    if isinstance(tree, exp.Insert):
        kind = "INSERT"
        target = tree.this.this if isinstance(tree.this, exp.Schema) else tree.this
        if tree.args.get("conflict"):
            raise ValueError("INSERT ... ON CONFLICT ... RETURNING is not supported")
    elif isinstance(tree, exp.Update):
        kind, target = "UPDATE", tree.this
        if tree.args.get("from") or tree.args.get("from_"):
            raise ValueError("UPDATE ... FROM ... RETURNING is not supported")
    elif isinstance(tree, exp.Delete):
        kind, target = "DELETE", tree.this
        if tree.args.get("using"):
            raise ValueError("DELETE ... USING ... RETURNING is not supported")
    else:
        raise ValueError("RETURNING is only supported on INSERT, UPDATE and DELETE")
    if not isinstance(target, exp.Table):
        raise ValueError("RETURNING needs a plain table as the statement's target")

    where = tree.args.get("where")
    select_list = ", ".join(e.sql(dialect="postgres") for e in returning.expressions)
    table_sql = target.sql(dialect="postgres")
    schema, table = target.db or "", target.name
    where_sql = where.this.sql(dialect="postgres") if where is not None else None
    tree.set("returning", None)
    return ReturningPlan(
        kind=kind,
        write_sql=tree.sql(dialect="postgres"),
        schema=schema,
        table=table,
        table_sql=table_sql,
        where_sql=where_sql,
        select_list=select_list,
    )


def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def key_literal(key: object) -> str:
    """A key as a SQL literal: numbers bare, everything else a quoted string."""
    if isinstance(key, bool):
        raise ValueError("a boolean cannot be a row key")
    if isinstance(key, (int, float)):
        return str(key)
    return "'" + str(key).replace("'", "''") + "'"


def select_sql(select_list: str, table_sql: str, condition: Optional[str]) -> str:
    sql = f"SELECT {select_list} FROM {table_sql}"
    return sql if condition is None else f"{sql} WHERE {condition}"


def key_conditions(key_column: str, keys: Iterable[object]) -> List[str]:
    """One ``key IN (...)`` condition per chunk of ``keys``."""
    keys = list(keys)
    column = quote_identifier(key_column)
    return [
        f"{column} IN ({', '.join(key_literal(k) for k in keys[i:i + KEY_CHUNK])})"
        for i in range(0, len(keys), KEY_CHUNK)
    ]
