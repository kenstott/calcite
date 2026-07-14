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

from pgwire_calcite import catalog, normalize
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
            catalog_name=catalog._DATABASE_NAME,
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

    # PGW-012 canonical source: read keys/referential constraints from Calcite's
    # Statistic API (adapter-agnostic — file TableConstraints, govdata YAML, etc.),
    # in addition to JDBC metadata above. Defensive: any adapter that exposes no
    # Statistic keys (e.g. the CSV file adapter) simply contributes nothing.
    _enrich_keys_from_statistic(conn, ctx, by_qualified)

    log.info(
        "[CATALOG] populated %d tables, %d with PKs, %d FKs from Calcite metadata",
        len(ctx.tables),
        len(ctx.pk_columns),
        len(ctx.joins),
    )
    return ctx, column_types


def _enrich_keys_from_statistic(conn, ctx: CompilationContext, by_qualified) -> None:
    """Add PKs/FKs from Calcite ``Statistic.getKeys()``/``getReferentialConstraints()``
    (PGW-012). Fully guarded — unusable/absent statistics contribute nothing."""
    import jpype

    try:
        cc = conn.unwrap(jpype.JClass("org.apache.calcite.jdbc.CalciteConnection"))
        tf = cc.getTypeFactory()
        root = cc.getRootSchema()
    except Exception:
        return

    def _fields(table):
        return [str(f) for f in table.getRowType(tf).getFieldNames()]

    for schema_name in list(root.getSubSchemaNames()):
        if str(schema_name).lower() in _SYSTEM_SCHEMAS:
            continue
        sub = root.getSubSchema(schema_name)
        if sub is None:
            continue
        for table_name in list(sub.getTableNames()):
            tm = by_qualified.get((str(schema_name), str(table_name)))
            if tm is None:
                continue
            table = sub.getTable(table_name)
            try:
                stat = table.getStatistic()
                fields = _fields(table)
            except Exception:
                continue
            # Primary key: first unique key from getKeys().
            try:
                keys = stat.getKeys()
            except Exception:
                keys = None
            if keys is not None and len(keys) > 0 and tm.table_id not in ctx.pk_columns:
                try:
                    cols = [fields[int(i)] for i in keys[0].toList()]
                    if cols:
                        ctx.pk_columns[tm.table_id] = cols
                except Exception:
                    pass
            # Referential (foreign) constraints.
            try:
                refs = stat.getReferentialConstraints()
            except Exception:
                refs = None
            for rc in list(refs) if refs else []:
                try:
                    tq = [str(x) for x in rc.getTargetQualifiedName()]
                    tgt = by_qualified.get((tq[-2], tq[-1])) if len(tq) >= 2 else None
                    if tgt is None:
                        continue
                    tfields = _fields(root.getSubSchema(tq[-2]).getTable(tq[-1]))
                    for pair in list(rc.getColumnPairs()):
                        sc = fields[int(pair.source)]
                        tc = tfields[int(pair.target)]
                        ctx.joins[(tm.type_name, sc)] = JoinMeta(
                            source_column=sc, target_column=tc, target=tgt,
                            cardinality="many-to-one",
                        )
                except Exception:
                    continue


def install_catalog(state, ctx: CompilationContext, column_types) -> None:
    """Install a catalog model (from JDBC or the bridge) onto ``state`` (PGW-012).

    Enables the intercept once populated. Leaves ``state.roles`` empty: the catalog
    adds the connected role + standard PG roles; per-role modelling is authz (5b).
    """
    grants = getattr(state, "authz_grants", None)
    if grants is not None:
        # Per-role filtered discovery (PGW-045): each role sees only granted objects.
        from pgwire_calcite.authz import AuthzContexts

        state.contexts = AuthzContexts(ctx, grants)
    else:
        state.contexts = SharedContexts(ctx)
    state.schema_build_cache = {"column_types": column_types, "tables": [], "domains": []}
    state.catalog_enabled = True


def populate_state(conn, state, role_id: str = "") -> None:
    """Introspect Calcite via JDBC and install the catalog onto ``state`` (PGW-012)."""
    ctx, column_types = build_context(conn)
    install_catalog(state, ctx, column_types)


# --- Bridge serialization (Phase 5 topology: catalog over the socket) ---------


def serialize_catalog(ctx: CompilationContext, column_types) -> dict:
    """Serialize the catalog model to JSON so the Calcite child can ship it to the
    pgwire process over the bridge (the child owns the JDBC connection)."""

    def tm(t):
        return {
            "table_id": t.table_id,
            "catalog_name": t.catalog_name,
            "schema_name": t.schema_name,
            "table_name": t.table_name,
            "type_name": t.type_name,
            "domain_id": t.domain_id,
        }

    def cm(c):
        return {"column_name": c.column_name, "data_type": c.data_type, "is_nullable": c.is_nullable}

    return {
        "tables": {tn: tm(t) for tn, t in ctx.tables.items()},
        "pk_columns": {str(tid): cols for tid, cols in ctx.pk_columns.items()},
        "joins": [
            {
                "src_type": k[0],
                "field": k[1],
                "source_column": j.source_column,
                "target_column": j.target_column,
                "target_type": j.target.type_name,
                "cardinality": j.cardinality,
            }
            for k, j in ctx.joins.items()
        ],
        "column_types": {str(tid): [cm(c) for c in cols] for tid, cols in column_types.items()},
    }


def deserialize_catalog(d: dict):
    """Rebuild (CompilationContext, column_types) from serialize_catalog()."""
    tables = {tn: TableMeta(**t) for tn, t in d["tables"].items()}
    pk = {int(tid): cols for tid, cols in d["pk_columns"].items()}
    joins = {}
    for j in d["joins"]:
        target = tables.get(j["target_type"])
        if target is None:
            continue
        joins[(j["src_type"], j["field"])] = JoinMeta(
            source_column=j["source_column"],
            target_column=j["target_column"],
            target=target,
            cardinality=j["cardinality"],
        )
    ctx = CompilationContext(tables=tables, pk_columns=pk, joins=joins)
    column_types = {
        int(tid): [ColumnMeta(**c) for c in cols] for tid, cols in d["column_types"].items()
    }
    return ctx, column_types
