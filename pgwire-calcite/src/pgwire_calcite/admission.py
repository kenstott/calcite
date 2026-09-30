# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admission control for unfiltered scans of large tables.

One statement that scans a very large table with no partition filter holds the single shared
engine connection for minutes, and every later statement queues behind it. This rejects such a
statement up front, with an error naming the table and the columns that would narrow it.

The row counts come from a coverage file (``--table-coverage-file``): a JSON object keyed by
``schema.table`` with ``row_count`` and ``partition_columns``. ``scripts/export_table_coverage.py``
writes it from each govdata schema's ``observedCoverage.rowCount`` — the same measurement
GovDataCatalog serves as ``observed_row_count``. A table absent from the file is not judged.
Off unless ``--max-unfiltered-scan-rows`` is set above zero.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Dict, List, Tuple

from pgwire_calcite.backend import PgProtocolError

#: SQLSTATE 54000 program_limit_exceeded: the statement is valid but over a configured limit.
SQLSTATE_SCAN_TOO_LARGE = "54000"


class ScanTooLarge(PgProtocolError):
    def __init__(self, message: str) -> None:
        super().__init__(SQLSTATE_SCAN_TOO_LARGE, message)


@dataclass(frozen=True)
class TableCoverage:
    row_count: int
    partition_columns: Tuple[str, ...]


def load_coverage(path: str) -> Dict[str, TableCoverage]:
    with open(path, encoding="utf-8") as fh:
        raw = json.load(fh)
    out: Dict[str, TableCoverage] = {}
    for key, entry in raw.items():
        out[key.lower()] = TableCoverage(
            row_count=int(entry["row_count"]),
            partition_columns=tuple(str(c).lower() for c in entry["partition_columns"]),
        )
    return out


class AdmissionPolicy:
    def __init__(self, max_unfiltered_rows: int, coverage: Dict[str, TableCoverage]) -> None:
        self.max_unfiltered_rows = max_unfiltered_rows
        self._coverage = coverage

    def check(self, pg_sql: str) -> None:
        """Raise :class:`ScanTooLarge` if a SELECT scans an over-threshold table unfiltered.

        A statement sqlglot cannot parse is not judged here; the transpile step that follows
        reports it.
        """
        import sqlglot
        import sqlglot.expressions as exp

        try:
            tree = sqlglot.parse_one(pg_sql, read="postgres")
        except Exception:
            return
        for select in tree.find_all(exp.Select):
            if _is_bounded_by_limit(select):
                continue
            filtered = _filter_columns(select)
            for table in _direct_tables(select):
                cov = self._coverage.get(f"{table.db}.{table.name}".lower())
                if cov is None or cov.row_count <= self.max_unfiltered_rows:
                    continue
                if filtered.intersection(cov.partition_columns):
                    continue
                raise ScanTooLarge(
                    f"{table.db}.{table.name} has about {cov.row_count:,} rows and this "
                    f"statement has no filter on its partition columns "
                    f"({', '.join(cov.partition_columns)}); add a WHERE on one of them, or a LIMIT"
                )


def _direct_tables(select) -> List:
    import sqlglot.expressions as exp

    tables = []
    source = select.args.get("from_") or select.args.get("from")
    if source is not None:
        tables.extend(t for t in [source.this] if isinstance(t, exp.Table))
    for join in select.args.get("joins") or []:
        if isinstance(join.this, exp.Table):
            tables.append(join.this)
    return tables


def _filter_columns(select) -> set:
    import sqlglot.expressions as exp

    conditions = []
    where = select.args.get("where")
    if where is not None:
        conditions.append(where)
    for join in select.args.get("joins") or []:
        on = join.args.get("on")
        if on is not None:
            conditions.append(on)
    return {c.name.lower() for cond in conditions for c in cond.find_all(exp.Column)}


def _is_bounded_by_limit(select) -> bool:
    """A LIMIT with no ordering, grouping, or aggregation streams a few rows and stops."""
    import sqlglot.expressions as exp

    if select.args.get("limit") is None:
        return False
    if select.args.get("order") or select.args.get("group") or select.args.get("distinct"):
        return False
    return not any(True for _ in select.find_all(exp.AggFunc))
