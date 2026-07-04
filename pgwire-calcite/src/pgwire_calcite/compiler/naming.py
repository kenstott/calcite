# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Naming helpers the copied catalog expects (provisa.compiler.naming shim).

In provisa these mapped internal domain/physical names to SQL-exposed names. In
pgwire-calcite the names already come straight from Calcite's JDBC metadata (the
form Calcite itself resolves), so the SQL-exposed name IS that name — these are
identity. Keeping discovery names identical to what Calcite stores is what makes
discover-then-query consistent by construction (PGW-016); the connection runs
with caseSensitive=false so a client that quotes the discovered name still
resolves it.
"""

from __future__ import annotations


def domain_to_sql_name(raw: str) -> str:
    """Schema/domain name as exposed in the catalog. Identity for Calcite names."""
    return raw


def apply_sql_name(physical: str) -> str:
    """Column name as exposed in the catalog. Identity for Calcite names."""
    return physical
