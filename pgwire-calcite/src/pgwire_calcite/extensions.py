# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Pluggable PostgreSQL extension surfaces (PGW-046).

Each surface: (a) advertises itself in ``pg_extension`` so clients see it
"installed", (b) may add type/OID mappings (normalize.py), and (c) maps its
operators/functions in the transform layer to a Calcite function / adapter / the
DuckDB engine — or rejects them explicitly (convert-or-reject-loudly, PGW-018).
Surfaces are opt-in per deployment; nothing here claims a capability it does not
implement (e.g. no vector-index acceleration).

Implemented: **json** (PGW-049) — ``->``/``->>`` -> Calcite JSON_QUERY/JSON_VALUE,
wired in transforms.py / dialect.py. Registered-but-follow-on: **postgis**
(ST_* -> Calcite spatial, needs ``fun=spatial``) and **vector** (pgvector ops ->
the file adapter's DuckDB engine). Advertising a follow-on surface here does NOT
enable operators it can't yet lower — that stays a loud reject.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Set


@dataclass(frozen=True)
class ExtensionSurface:
    name: str  # pg_extension.extname
    version: str
    comment: str
    implemented: bool  # True = operators lower today; False = advertised, follow-on


#: Known surfaces. Enable per deployment; only ``implemented`` ones lower operators.
REGISTRY: Dict[str, ExtensionSurface] = {
    "json": ExtensionSurface("json", "1.0", "JSON operators -> Calcite JSON_VALUE/JSON_QUERY", True),
    "postgis": ExtensionSurface("postgis", "3.0", "ST_* -> Calcite spatial (fun=spatial)", False),
    "vector": ExtensionSurface("vector", "0.7", "pgvector ops -> DuckDB array distance", False),
    "pg_trgm": ExtensionSurface("pg_trgm", "1.6", "trigram similarity", False),
}


def resolve(names) -> Set[str]:
    """Validate requested extension names against the registry; return the set."""
    out: Set[str] = set()
    for n in names or ():
        key = n.lower()
        if key not in REGISTRY:
            raise ValueError(f"unknown extension surface {n!r}; known: {sorted(REGISTRY)}")
        out.add(key)
    return out


def pg_extension_rows(enabled: Set[str]) -> List[tuple]:
    """Rows for a ``pg_extension`` catalog table: (oid, extname, extversion, comment).

    Advertises every enabled surface so a client's ``\\dx`` / pg_extension probe
    sees it installed (PGW-046).
    """
    rows: List[tuple] = []
    oid = 90000
    for name in sorted(enabled):
        surf = REGISTRY[name]
        rows.append((oid, surf.name, surf.version, surf.comment))
        oid += 1
    return rows
