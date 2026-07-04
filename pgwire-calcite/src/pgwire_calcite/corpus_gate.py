# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Calcite-upgrade regression gate (PGW-041).

Replays the client corpus (the actual query/introspection SQL DuckDB/DBeaver/
DataGrip emit) against a running server and reports pass/fail. A Calcite version
bump is adopted ONLY if this gate passes on our paths — community fixes on our
schedule, verified, never blind. ``gate()`` takes a ``query_fn`` so it is usable
from tests (MiniPgClient) or a CLI (a real pg client).
"""

from __future__ import annotations

import dataclasses
import json
import pathlib
from typing import Callable, List, Optional

# query_fn(sql) -> dict(columns=[...], rows=[[...]], error=str|None)
QueryFn = Callable[[str], dict]


@dataclasses.dataclass
class CorpusEntry:
    client: str
    sql: str
    min_phase: int
    expect: Optional[dict]
    path: pathlib.Path


@dataclasses.dataclass
class GateFailure:
    entry: CorpusEntry
    reason: str


@dataclasses.dataclass
class GateResult:
    passed: bool
    ran: int
    failures: List[GateFailure]

    def report(self) -> str:
        if self.passed:
            return f"CORPUS GATE PASS: {self.ran} entries"
        lines = [f"CORPUS GATE FAIL: {len(self.failures)}/{self.ran} regressed"]
        for f in self.failures:
            lines.append(f"  - {f.entry.path.name}: {f.reason}")
        return "\n".join(lines)


def load_corpus(root: pathlib.Path) -> List[CorpusEntry]:
    out: List[CorpusEntry] = []
    for path in sorted(pathlib.Path(root).rglob("*.json")):
        d = json.loads(path.read_text(encoding="utf-8"))
        expect = d.get("expect")
        out.append(
            CorpusEntry(
                client=d.get("client", "?"),
                sql=d["sql"],
                min_phase=int((expect or {}).get("min_phase", 0)),
                expect=expect,
                path=path,
            )
        )
    return out


def gate(query_fn: QueryFn, phase: int, root: pathlib.Path) -> GateResult:
    """Run every corpus entry gated at <= ``phase``; return a GateResult."""
    entries = [e for e in load_corpus(root) if e.min_phase <= phase]
    failures: List[GateFailure] = []
    for e in entries:
        result = query_fn(e.sql)
        if result.get("error"):
            failures.append(GateFailure(e, f"error: {result['error'][:120]}"))
            continue
        exp = e.expect or {}
        if "columns" in exp and result.get("columns") != exp["columns"]:
            failures.append(GateFailure(e, f"columns {result.get('columns')} != {exp['columns']}"))
            continue
        if "rows" in exp and result.get("rows") != exp["rows"]:
            failures.append(GateFailure(e, f"rows {result.get('rows')} != {exp['rows']}"))
            continue
        if exp.get("min_phase", 0) >= 2 and not result.get("columns"):
            failures.append(GateFailure(e, "introspection probe returned no columns"))
    return GateResult(passed=not failures, ran=len(entries), failures=failures)
