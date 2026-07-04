# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Execution backend seam for pgwire-calcite.

This is the single replaceable seam that decouples the wire protocol from the
query engine. It replaces provisa's ``_pipeline.execute_pgwire_sql`` (the Trino
route, D2/D3 rejected).

Phase roadmap for this file:
- Phase 0 (this file): ``StubBackend`` answers a fixed handful of statements so
  the copied wire server can be exercised end-to-end without Calcite. Anything
  else raises, loudly — no silent fallbacks (CLAUDE.md rule 6).
- Phase 1: ``CalciteBackend`` — sqlglot Calcite-dialect transpile + embedded
  ``jdbc:calcite:`` execution, returning the same ``QueryResult`` shape.

The wire layer only depends on the ``Backend`` protocol below, so swapping
implementations is a one-line change in the launcher.
"""

from __future__ import annotations

import re
from typing import Optional, Protocol, runtime_checkable

from pgwire_calcite.types import QueryResult


@runtime_checkable
class Backend(Protocol):
    """Contract the wire server executes non-catalog statements against."""

    def execute_sql(
        self, sql: str, role_id: str, params: Optional[list] = None, stream: bool = False
    ) -> QueryResult:
        ...

    def ready(self) -> bool:
        """Liveness/readiness gate (Phase 5 readiness-gating hook)."""
        ...


class BackendError(RuntimeError):
    """Backend could not execute the statement. Never swallowed silently."""


_SELECT_ONE_RE = re.compile(r"^\s*SELECT\s+1\s*;?\s*$", re.IGNORECASE)
_SELECT_LITERAL_RE = re.compile(r"^\s*SELECT\s+(\d+)\s*;?\s*$", re.IGNORECASE)
_SELECT_VERSION_RE = re.compile(r"^\s*SELECT\s+version\s*\(\s*\)\s*;?\s*$", re.IGNORECASE)


class StubBackend:
    """Phase-0 stand-in backend.

    Answers only the statements the Phase-0 exit gate exercises (``SELECT 1``,
    ``SELECT <int>``, ``SELECT version()``). Every other statement raises
    ``BackendError`` so that "not yet implemented" can never masquerade as a
    working query result.
    """

    #: Reported by SELECT version() and used to satisfy DuckDB's >=12 / clients' >=14 gate.
    SERVER_VERSION = "14.0 (pgwire-calcite stub backend, Phase 0)"

    def ready(self) -> bool:
        return True

    def execute_sql(
        self, sql: str, role_id: str, params: Optional[list] = None, stream: bool = False
    ) -> QueryResult:
        del role_id, params, stream  # stub is always materialized
        stripped = sql.strip().rstrip(";").strip()
        if _SELECT_ONE_RE.match(sql):
            return QueryResult(rows=[(1,)], column_names=["?column?"], column_types=["INTEGER"])
        m = _SELECT_LITERAL_RE.match(sql)
        if m:
            return QueryResult(
                rows=[(int(m.group(1)),)], column_names=["?column?"], column_types=["INTEGER"]
            )
        if _SELECT_VERSION_RE.match(sql):
            return QueryResult(
                rows=[("PostgreSQL " + self.SERVER_VERSION,)],
                column_names=["version"],
                column_types=["VARCHAR"],
            )
        raise BackendError(
            "StubBackend (Phase 0) executes only SELECT 1 / SELECT <int> / SELECT version(); "
            "real query execution arrives in Phase 1 (CalciteBackend). Rejected: " + stripped[:200]
        )
