# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Per-role authorization: schema/table visibility (PGW-045).

Enforced in BOTH discovery and execution by construction (same spirit as
PGW-016): the catalog intercept is fed a per-role *filtered* CompilationContext
(so a role only discovers its granted objects), and non-catalog queries are
checked against the same grants before execution (out-of-grant access -> denied,
loud, SQLSTATE 42501).

Grants are ``role -> {"schema.table", ...}`` or ``{"*"}`` for all. Authz is
opt-in: with no policy, nothing is filtered or denied (trust/localhost default).
Unknown roles are denied by default when a policy IS set — never silently allowed.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Set, Tuple

from pgwire_calcite.compiler.sql_gen import CompilationContext

ALL = "*"


@dataclass
class RoleGrants:
    grants: Dict[str, Set[str]]

    @staticmethod
    def from_dict(d: dict) -> "RoleGrants":
        return RoleGrants({role: set(objs) for role, objs in d.items()})

    def allows(self, role: str, schema: str, table: str) -> bool:
        g = self.grants.get(role)
        if not g:
            return False  # unknown role under an active policy -> deny (no silent allow)
        if ALL in g:
            return True
        table_l = (table or "").lower()
        schema_l = (schema or "").lower()
        for key in g:
            kl = key.lower()
            if "." in kl:
                ks, kt = kl.split(".", 1)
                if kt == table_l and (not schema_l or ks == schema_l):
                    return True
            elif kl == table_l:
                return True
        return False


class AuthzContexts:
    """A ``state.contexts`` that returns a per-role FILTERED CompilationContext,
    so discovery only exposes the role's granted objects (PGW-045)."""

    def __init__(self, base_ctx: CompilationContext, grants: RoleGrants) -> None:
        self._base = base_ctx
        self._grants = grants
        self._cache: Dict[str, CompilationContext] = {}

    def get(self, role_id, default=None):
        role = role_id or ""
        if role not in self._cache:
            self._cache[role] = _filter_ctx(self._base, self._grants, role)
        return self._cache[role]


def _filter_ctx(base: CompilationContext, grants: RoleGrants, role: str) -> CompilationContext:
    kept = {
        tn: tm
        for tn, tm in base.tables.items()
        if grants.allows(role, tm.schema_name, tm.table_name)
    }
    kept_ids = {tm.table_id for tm in kept.values()}
    kept_types = {tm.type_name for tm in kept.values()}
    return CompilationContext(
        tables=kept,
        pk_columns={tid: c for tid, c in base.pk_columns.items() if tid in kept_ids},
        joins={
            k: j
            for k, j in base.joins.items()
            if k[0] in kept_types and j.target.table_id in kept_ids
        },
        physical_to_sql=base.physical_to_sql,
        virtual_columns=base.virtual_columns,
    )


def referenced_tables(pg_sql: str) -> List[Tuple[str, str]]:
    """(schema, table) pairs referenced by a PG query; [] if it can't be parsed."""
    import sqlglot
    import sqlglot.expressions as exp

    try:
        tree = sqlglot.parse_one(pg_sql, read="postgres")
    except Exception:
        return []
    out: List[Tuple[str, str]] = []
    for t in tree.find_all(exp.Table):
        out.append((t.db or "", t.name or ""))
    return out


def enforce_query(grants: RoleGrants, role: str, pg_sql: str) -> None:
    """Raise PermissionError if the query references any out-of-grant table."""
    for schema, table in referenced_tables(pg_sql):
        if not table:
            continue
        if not grants.allows(role, schema, table):
            raise PermissionError(f'permission denied for relation "{table}"')
