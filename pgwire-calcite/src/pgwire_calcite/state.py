# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Minimal server state / schema registry for pgwire-calcite.

Replaces provisa's FastAPI ``provisa.api.app.state`` (open decision §7: the copy
needs a minimal launcher rather than dragging in provisa's FastAPI ``state``).

The wire layer (server.py) reads exactly three things from here today:
- ``backend``            — the execution seam (Phase 0 stub; Phase 1 Calcite)
- ``auth_config``        — ``{"provider": "none"|"simple", "users": {...}}``
- ``auth_middleware_active`` — whether cleartext-password auth is enforced

``schema_registry`` is the durable, re-providable source of truth the supervisor
hands to a restarted child (PGW-038); in Phase 0 it is an empty placeholder that
Phase 2 populates from Calcite metadata to drive the catalog intercept.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class SchemaRegistry:
    """Durable, rebuildable description of what the backend exposes.

    Phase 2 fills this from Calcite metadata (schemas/tables/columns + keys and
    referential constraints). Kept separate from any child process so a recycled
    child can be re-seeded from it (PGW-038).
    """

    #: schema name -> {table name -> list[(column_name, duckdb_type)]}
    tables: Dict[str, Dict[str, list]] = field(default_factory=dict)
    database: str = "calcite"


@dataclass
class ServerState:
    """The single object the wire layer and (Phase 2) catalog intercept read."""

    backend: object  # Backend protocol (pgwire_calcite.backend.Backend)
    schema_registry: SchemaRegistry = field(default_factory=SchemaRegistry)
    auth_config: Dict[str, object] = field(default_factory=lambda: {"provider": "none"})
    auth_middleware_active: bool = False
    #: Flipped on by catalog_populate.populate_state once the intercept is wired
    #: to Calcite metadata (Phase 2).
    catalog_enabled: bool = False
    #: role_id -> plaintext password, for the "simple" cleartext provider.
    users: Dict[str, str] = field(default_factory=dict)
    roles: Dict[str, object] = field(default_factory=dict)
    #: Catalog model consumed by catalog.py: contexts.get(role_id) -> CompilationContext,
    #: and schema_build_cache = {"column_types": {...}, "tables": [], "domains": []}.
    #: Populated from Calcite metadata (Phase 2); None until then.
    contexts: object = None
    schema_build_cache: object = None

    def check_password(self, username: str, password: str) -> bool:
        """Cleartext-password check for provider='simple'. No silent default."""
        if username not in self.users:
            return False
        return self.users[username] == password
