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


@dataclass
class TableMeta:
    """One table/view. ``type_name`` is a stable per-table key used by join rules."""

    table_id: int
    catalog_name: str
    schema_name: str
    table_name: str
    type_name: str
    domain_id: Optional[str] = None


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
    #: keyed by (source TableMeta.type_name, join_field)
    joins: Dict[Tuple[str, str], JoinMeta] = field(default_factory=dict)
    physical_to_sql: Dict[Tuple[int, str], str] = field(default_factory=dict)
    virtual_columns: Dict[int, dict] = field(default_factory=dict)


def semantic_table_name(tm: TableMeta) -> str:
    """The table's exposed name. For Calcite metadata this is just table_name."""
    return tm.table_name
