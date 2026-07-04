# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Regression corpus harness (PGW-041).

The corpus is the actual query/introspection SQL that the target clients
(DuckDB, DBeaver, DataGrip) emit against a Postgres endpoint. It is the shared
asset that (a) gates Calcite version upgrades and every later phase, and (b)
mitigates the D1 two-copies drift risk by living as data, not duplicated code.

Corpus layout
-------------
    tests/corpus/<client>/<probe-name>.json

Each entry is JSON::

    {
      "client":   "dbeaver" | "datagrip" | "duckdb" | "psql",
      "phase":    "connect" | "introspect" | "query" | "copy",
      "sql":      "<the exact SQL the client sent>",
      "captured": true | false,            # from real client traffic vs. hand-seeded
      "expect": {                          # optional, filled in as phases land
        "answered_by": "backend" | "catalog" | "session",
        "min_phase":   0..7,               # phase at which this must pass
        "columns":     ["..."],            # optional expected column names
        "rows":        [[...]]             # optional expected rows
      }
    }

Capturing real traffic
----------------------
Point the client at a reference PostgreSQL with statement logging on
(``log_statement = 'all'``), perform the action (connect / expand schema tree /
draw ER diagram / ``ATTACH``), then extract the statements into entries with
``captured: true``. Until a client's traffic is captured, ``captured: false``
seed entries record the *known* probes so the harness structure is exercised.
"""

from __future__ import annotations

import dataclasses
import json
import pathlib
from typing import List, Optional

CORPUS_ROOT = pathlib.Path(__file__).parent


@dataclasses.dataclass
class CorpusEntry:
    client: str
    phase: str
    sql: str
    captured: bool
    path: pathlib.Path
    expect: Optional[dict] = None

    @property
    def min_phase(self) -> int:
        return int((self.expect or {}).get("min_phase", 0))


def load_corpus(root: pathlib.Path = CORPUS_ROOT) -> List[CorpusEntry]:
    """Load every corpus entry under ``root`` (recursively)."""
    entries: List[CorpusEntry] = []
    for path in sorted(root.rglob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        entries.append(
            CorpusEntry(
                client=data["client"],
                phase=data["phase"],
                sql=data["sql"],
                captured=bool(data.get("captured", False)),
                expect=data.get("expect"),
                path=path,
            )
        )
    return entries


def entries_for_phase(phase: int, root: pathlib.Path = CORPUS_ROOT) -> List[CorpusEntry]:
    """Entries whose ``min_phase`` <= the given phase (i.e. must pass by now)."""
    return [e for e in load_corpus(root) if e.min_phase <= phase]


def summarize(root: pathlib.Path = CORPUS_ROOT) -> dict:
    """Coverage summary: counts by client, capture status, and gating phase."""
    entries = load_corpus(root)
    by_client: dict = {}
    captured = 0
    for e in entries:
        by_client[e.client] = by_client.get(e.client, 0) + 1
        captured += 1 if e.captured else 0
    return {
        "total": len(entries),
        "captured": captured,
        "seeded": len(entries) - captured,
        "by_client": by_client,
    }


if __name__ == "__main__":
    print(json.dumps(summarize(), indent=2))
