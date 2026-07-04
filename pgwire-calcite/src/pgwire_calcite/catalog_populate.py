# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Populate the catalog model from Calcite metadata (PGW-012).

Builds a ``CompilationContext`` (the model catalog.py consumes) from the embedded
Calcite connection's JDBC ``DatabaseMetaData``: schemas, tables, columns, primary
keys, and referential (foreign) keys. Column types flow through the shared
``normalize`` module so discovery and query execution use the SAME type/identifier
mapping (PGW-016).

Per PGW-013 the catalog invents nothing: when an adapter declares no keys,
getPrimaryKeys/getImportedKeys return nothing and we report empty — real rows
where keys exist, well-formed empties where they don't. Constraint-less adapters
simply have no PK/FK entries; clients (DataGrip) add virtual FKs on their side.
"""

from __future__ import annotations

import logging
from typing import Dict, List

from pgwire_calcite import normalize
from pgwire_calcite.compiler.sql_gen import (
    ColumnMeta,
    CompilationContext,
    JoinMeta,
    TableMeta,
)

log = logging.getLogger(__name__)

# Schemas Calcite/JDBC exposes that are not user data — skipped in discovery.
_SYSTEM_SCHEMAS = {"metadata", "information_schema", "pg_catalog", ""}


class SharedContexts:
    """A ``state.contexts`` that returns the same catalog model for every role.

    Phase 2 has no per-role authorization (that is Phase 5b, PGW-045); every
    authenticated role sees the same catalog. ``.get(role_id)`` therefore returns
    the one shared CompilationContext regardless of role.
    """

    def __init__(self, ctx: CompilationContext) -> None:
        self._ctx = ctx

    def get(self, role_id, default=None):
        return self._ctx


def _rows(rs):
    """Yield JDBC ResultSet rows as dicts keyed by uppercased column label."""
    md = rs.getMetaData()
    n = int(md.getColumnCount())
    labels = [str(md.getColumnLabel(i)).upper() for i in range(1, n + 1)]
    while bool(rs.next()):
        yield {labels[i - 1]: rs.getString(i) for i in range(1, n + 1)}
    rs.close()


def build_context(conn) -> tuple:
    """Introspect the Calcite JDBC connection. Returns (ctx, column_types dict)."""
    md = conn.getMetaData()
    ctx = CompilationContext()
    column_types: Dict[int, List[ColumnMeta]] = {}
    # (schema, table) -> TableMeta, so FK targets resolve.
    by_qualified: Dict[tuple, TableMeta] = {}
    next_id = 1

    # Tables + views.
    JClass_types = None
    rs = md.getTables(None, None, "%", None)
    table_keys: List[tuple] = []
    for r in _rows(rs):
        schema = r.get("TABLE_SCHEM") or ""
        name = r.get("TABLE_NAME") or ""
        ttype = (r.get("TABLE_TYPE") or "").upper()
        if schema.lower() in _SYSTEM_SCHEMAS:
            continue
        if ttype not in ("TABLE", "VIEW", "BASE TABLE", ""):
            continue
        tm = TableMeta(
            table_id=next_id,
            catalog_name="calcite",
            schema_name=schema,
            table_name=name,
            type_name=f"{schema}.{name}",
        )
        ctx.tables[tm.type_name] = tm
        by_qualified[(schema, name)] = tm
        column_types[next_id] = []
        table_keys.append((schema, name, tm))
        next_id += 1

    # Columns per table.
    for schema, name, tm in table_keys:
        crs = md.getColumns(None, schema, name, "%")
        cols: List[ColumnMeta] = []
        for r in _rows(crs):
            type_name = r.get("TYPE_NAME") or "VARCHAR"
            nullable = str(r.get("NULLABLE") or "1") != "0"
            cols.append(
                ColumnMeta(
                    column_name=r.get("COLUMN_NAME") or "",
                    data_type=normalize.duckdb_label(type_name),
                    is_nullable=nullable,
                )
            )
        column_types[tm.table_id] = cols

    # Primary keys (empty for constraint-less adapters — reported as such).
    for schema, name, tm in table_keys:
        try:
            prs = md.getPrimaryKeys(None, schema, name)
        except Exception:  # some adapters do not implement getPrimaryKeys
            continue
        pk = [r.get("COLUMN_NAME") for r in _rows(prs) if r.get("COLUMN_NAME")]
        if pk:
            ctx.pk_columns[tm.table_id] = pk

    # Foreign keys -> joins (many-to-one), for pg_constraint / ER diagrams.
    for schema, name, tm in table_keys:
        try:
            frs = md.getImportedKeys(None, schema, name)
        except Exception:
            continue
        for r in _rows(frs):
            tgt = by_qualified.get((r.get("PKTABLE_SCHEM") or "", r.get("PKTABLE_NAME") or ""))
            if tgt is None:
                continue
            fk_col = r.get("FKCOLUMN_NAME") or ""
            ctx.joins[(tm.type_name, fk_col)] = JoinMeta(
                source_column=fk_col,
                target_column=r.get("PKCOLUMN_NAME") or "",
                target=tgt,
                cardinality="many-to-one",
            )

    log.info(
        "[CATALOG] populated %d tables, %d with PKs, %d FKs from Calcite metadata",
        len(ctx.tables),
        len(ctx.pk_columns),
        len(ctx.joins),
    )
    return ctx, column_types


def populate_state(conn, state, role_id: str = "") -> None:
    """Introspect Calcite and install the catalog model onto ``state`` (PGW-012).

    Enables the intercept (``state.catalog_enabled = True``) once populated.
    """
    ctx, column_types = build_context(conn)
    grants = getattr(state, "authz_grants", None)
    if grants is not None:
        # Per-role filtered discovery (PGW-045): each role sees only granted objects.
        from pgwire_calcite.authz import AuthzContexts

        state.contexts = AuthzContexts(ctx, grants)
    else:
        state.contexts = SharedContexts(ctx)
    state.schema_build_cache = {"column_types": column_types, "tables": [], "domains": []}
    # Leave state.roles empty: catalog._populate_pg_roles_and_database adds the
    # connected role plus the standard PG system roles. Per-role role modelling
    # arrives with authz (Phase 5b, PGW-045).
    state.catalog_enabled = True
