# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Compilation-model types the copied catalog consumes (provisa.compiler.sql_gen shim).

Only the fields the catalog actually reads are modeled (verified against
catalog.py's ``tm.*`` / ``ctx.*`` / ``col.*`` / ``jm.*`` accesses). Populated from
Calcite JDBC metadata by catalog_populate.py.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


@dataclass
class ColumnMeta:
    """One column, as the catalog's column_types cache expects."""

    column_name: str
    data_type: str  # duckdb-style label (see normalize.duckdb_label)
    is_nullable: bool = True
    comment: Optional[str] = None  # column REMARKS -> pg_description / col_description


@dataclass
class TableMeta:
    """One table/view. ``type_name`` is a stable per-table key used by join rules."""

    table_id: int
    catalog_name: str
    schema_name: str
    table_name: str
    type_name: str
    domain_id: Optional[str] = None
    comment: Optional[str] = None  # table REMARKS -> pg_description / obj_description


@dataclass
class JoinMeta:
    """A referential (FK) relationship, in the shape catalog._build_fk_constraint_rows reads."""

    source_column: str
    target_column: str
    target: TableMeta
    cardinality: str = "many-to-one"
    source_constant: Optional[str] = None
    source_expr: Optional[str] = None


@dataclass
class CompilationContext:
    """The per-role catalog model. Everything the catalog reads off ``ctx``."""

    tables: Dict[str, TableMeta] = field(default_factory=dict)
    pk_columns: Dict[int, List[str]] = field(default_factory=dict)
    #: table_id -> list of non-PK UNIQUE key column-lists (surfaced as pg_constraint
    #: contype='u' so non-PK unique columns are valid FK targets, e.g. state_ref.state_fips)
    unique_columns: Dict[int, List[List[str]]] = field(default_factory=dict)
    #: keyed by (source TableMeta.type_name, join_field)
    joins: Dict[Tuple[str, str], JoinMeta] = field(default_factory=dict)
    #: Grouped FKs preserving multi-column FK identity, so a composite FK renders as ONE
    #: pg_constraint edge (not one per column). Each: (source type_name, target TableMeta,
    #: [(source_column, target_column), ...]). ``joins`` above is kept per-column for the
    #: query/join model; this is the constraint model.
    foreign_keys: List[Tuple[str, "TableMeta", List[Tuple[str, str]]]] = field(default_factory=list)
    physical_to_sql: Dict[Tuple[int, str], str] = field(default_factory=dict)
    virtual_columns: Dict[int, dict] = field(default_factory=dict)


def semantic_table_name(tm: TableMeta) -> str:
    """The table's exposed name. For Calcite metadata this is just table_name."""
    return tm.table_name
