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
from typing import List, Optional, Tuple


@dataclass
class QueryResult:
    """Result of executing a SQL statement.

    ``column_types`` are optional DuckDB-style type name strings; when absent the
    wire layer infers pg types from the row values.
    """

    rows: List[tuple] = field(default_factory=list)
    column_names: List[str] = field(default_factory=list)
    column_types: Optional[List[str]] = None

    @classmethod
    def single(cls, column_name: str, value) -> "QueryResult":
        """Convenience for a 1x1 scalar result."""
        return cls(rows=[(value,)], column_names=[column_name])
