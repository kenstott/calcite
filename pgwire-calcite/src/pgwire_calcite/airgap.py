# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Airgap / fail-loud configuration (PGW-029).

All autoload/autofetch MUST be disabled and MUST fail loud — never silently
reach the network. This module centralizes the DuckDB knobs (the catalog
intercept uses an in-memory DuckDB; the file adapter's engine may use another)
and a verification that autofetch is actually off, so an offline install can be
asserted in CI including the rarely-used paths.
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)

#: DuckDB PRAGMAs that must be off so a missing extension fails loudly instead of
#: silently installing from the network.
_DUCKDB_NO_AUTOFETCH = (
    "SET autoinstall_known_extensions=false",
    "SET autoload_known_extensions=false",
)


def configure_duckdb(con) -> None:
    """Disable DuckDB extension autoinstall/autoload on a connection (fail loud)."""
    for pragma in _DUCKDB_NO_AUTOFETCH:
        con.execute(pragma)


def duckdb_autofetch_disabled(con) -> bool:
    """True iff both autoinstall and autoload of extensions are off."""
    ai = con.execute(
        "SELECT current_setting('autoinstall_known_extensions')"
    ).fetchone()[0]
    al = con.execute("SELECT current_setting('autoload_known_extensions')").fetchone()[0]
    return str(ai).lower() in ("false", "0") and str(al).lower() in ("false", "0")


class AirgapViolation(RuntimeError):
    """A network fetch was attempted in airgapped mode. Fails loud, never silent."""


def assert_offline_ready(con) -> None:
    """Raise if DuckDB would autofetch — used by the CI offline-install check."""
    if not duckdb_autofetch_disabled(con):
        raise AirgapViolation(
            "DuckDB extension autoinstall/autoload is enabled; airgap requires them off "
            "with extensions bundled (PGW-029)."
        )
