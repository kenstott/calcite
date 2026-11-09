# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Bulk row insert for the in-memory catalog DuckDB.

DuckDB's ``executemany`` binds, plans and runs the INSERT once per row. The catalog build
inserts tens of thousands of rows that way, which took over a minute on a full corpus and
made the first catalog query (e.g. a primary-key lookup) time out. ``insert_rows`` takes
the same ``INSERT ... VALUES (...)`` statement and rows, loads the rows as one Arrow table,
and runs a single ``INSERT ... SELECT``: every ``?`` becomes the matching Arrow column and
every literal in the VALUES list stays as written.
"""

from __future__ import annotations

import re
from typing import Iterable, Sequence

import pyarrow as pa

_INSERT = re.compile(
    r"^\s*INSERT\s+INTO\s+(?P<target>\w+(?:\s*\([^)]*\))?)\s+VALUES\s*\((?P<values>.*)\)\s*$",
    re.IGNORECASE | re.DOTALL,
)
_SOURCE = "_bulk_insert_rows"


def _select_list(values: str) -> tuple[str, int]:
    """Replaces each ``?`` outside a string literal with ``c<n>``; returns the list and n."""
    out = []
    count = 0
    in_literal = False
    for ch in values:
        if ch == "'":
            in_literal = not in_literal
        if ch == "?" and not in_literal:
            out.append(f"c{count}")
            count += 1
        else:
            out.append(ch)
    return "".join(out), count


def insert_rows(db, sql: str, rows: Iterable[Sequence]) -> None:
    """Inserts ``rows`` with ``sql``, an ``INSERT INTO t [(cols)] VALUES (...)`` statement."""
    m = _INSERT.match(sql)
    if m is None:
        raise ValueError(f"insert_rows needs INSERT INTO <table> VALUES (...), got: {sql}")
    select, width = _select_list(m.group("values"))
    rows = list(rows)
    if not rows:
        return
    for row in rows:
        if len(row) != width:
            raise ValueError(
                f"insert_rows: row has {len(row)} values, statement has {width} placeholders: "
                f"{sql}"
            )
    table = pa.table({f"c{i}": pa.array([row[i] for row in rows]) for i in range(width)})
    db.register(_SOURCE, table)
    try:
        db.execute(f"INSERT INTO {m.group('target')} SELECT {select} FROM {_SOURCE}")
    finally:
        db.unregister(_SOURCE)
