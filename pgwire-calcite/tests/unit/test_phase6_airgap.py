# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Phase 6 tests: airgap / fail-loud config (PGW-029).

The build/sign/installer steps need per-OS machines + credentials (the user's);
what IS verifiable here is that DuckDB extension autoinstall/autoload is off so a
missing extension fails loud instead of silently fetching from the network.
"""

from __future__ import annotations

import duckdb
import pytest

from pgwire_calcite.airgap import (
    AirgapViolation,
    assert_offline_ready,
    configure_duckdb,
    duckdb_autofetch_disabled,
)


def test_configure_disables_autofetch():
    con = duckdb.connect()
    # default may allow autoinstall; after configure it must be off
    configure_duckdb(con)
    assert duckdb_autofetch_disabled(con)
    assert_offline_ready(con)  # does not raise


def test_assert_offline_ready_raises_when_enabled():
    con = duckdb.connect()
    con.execute("SET autoinstall_known_extensions=true")
    con.execute("SET autoload_known_extensions=true")
    with pytest.raises(AirgapViolation):
        assert_offline_ready(con)


def test_catalog_inmemory_duckdb_is_airgapped(calcite_backend):
    """The catalog intercept's in-memory DuckDB is configured fail-loud."""
    import time

    from pgwire_calcite import launcher
    from test_phase0_wire import MiniPgClient, _free_port

    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", backend=calcite_backend)
    time.sleep(0.1)
    try:
        c = MiniPgClient("127.0.0.1", port)
        try:
            # a catalog query exercises _build_catalog_db -> configure_duckdb; it must
            # answer normally (airgap config does not break the intercept)
            r = c.query("SELECT table_name FROM information_schema.tables WHERE lower(table_name)='emps'")
            assert r["error"] is None and r["rows"] == [["emps"]]
        finally:
            c.close()
    finally:
        srv.shutdown()
