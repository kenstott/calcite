# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Shared result type for pgwire-calcite.

Replaces provisa's ``provisa.executor.trino.QueryResult`` with a backend-neutral
dataclass. Both the catalog intercept (Phase 2) and the query backend (Phase 1+)
produce this shape; the wire layer (server.py) adapts it to the buenavista
QueryResult ABC. Field names/semantics are kept identical to the provisa original
so the copied catalog code binds to it without change.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterator, List, Optional


@dataclass
class QueryResult:
    """Result of executing a SQL statement.

    ``column_types`` are optional DuckDB-style type name strings; an absent list, or
    an entry that is ``None``/``""`` (the source's metadata was insufficient), tells
    the wire layer to infer that pg type from the row values.

    ``row_batches`` is the lazy streaming source: when set it is authoritative and
    ``rows`` stays empty. Each item is a list of row tuples (one engine/Arrow batch),
    so a consumer holds at most one batch in memory and can peek the first batch for
    type inference without draining the result (PGW-020). It is single-pass — the
    consumer owns it and must close it to release the underlying statement (PGW-022).
    """

    rows: List[tuple] = field(default_factory=list)
    column_names: List[str] = field(default_factory=list)
    column_types: Optional[List[str]] = None
    row_batches: Optional[Iterator[List[tuple]]] = None

    def iter_rows(self) -> Iterator[tuple]:
        """Flat row view over whichever source this result carries.

        Streaming results expose ``row_batches``; materialized ones expose ``rows``.
        Callers that do not care about batch boundaries read through here — it is
        still single-pass when the source is a stream.
        """
        if self.row_batches is not None:
            for batch in self.row_batches:
                yield from batch
            return
        yield from self.rows

    @classmethod
    def single(cls, column_name: str, value) -> "QueryResult":
        """Convenience for a 1x1 scalar result."""
        return cls(rows=[(value,)], column_names=[column_name])
