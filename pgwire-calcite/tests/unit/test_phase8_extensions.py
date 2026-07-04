# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Phase 8 tests (optional): extension surfaces (PGW-046/049).

The pluggable mechanism + the JSON surface: ``->``/``->>`` become Calcite
JSON_QUERY/JSON_VALUE when the surface is enabled, and are rejected loudly when
it is not (convert-or-reject, PGW-018). postgis/vector are advertised follow-ons.
"""

from __future__ import annotations

import time

import pytest

from pgwire_calcite import extensions, launcher
from pgwire_calcite.dialect import UnsupportedConstruct, transpile_pg_to_calcite

from test_phase0_wire import MiniPgClient, _free_port


def test_registry_resolve_and_advertise():
    assert extensions.resolve(["json"]) == {"json"}
    assert extensions.resolve([]) == set()
    with pytest.raises(ValueError):
        extensions.resolve(["not-an-extension"])
    rows = extensions.pg_extension_rows({"json", "vector"})
    names = {r[1] for r in rows}
    assert names == {"json", "vector"}


def test_json_op_rejected_when_surface_disabled():
    with pytest.raises(UnsupportedConstruct):
        transpile_pg_to_calcite("SELECT data->>'a' FROM t")


def test_json_op_converts_when_enabled():
    scalar = transpile_pg_to_calcite("SELECT data->>'a' AS v FROM t", json_enabled=True)
    assert "JSON_VALUE" in scalar.upper()
    subtree = transpile_pg_to_calcite("SELECT data->'a' AS v FROM t", json_enabled=True)
    assert "JSON_QUERY" in subtree.upper()


def test_json_surface_executes_on_calcite(calcite_backend):
    calcite_sql = transpile_pg_to_calcite("SELECT '{\"a\":42}'->>'a' AS v", json_enabled=True)
    assert "JSON_VALUE" in calcite_sql.upper()
    r = calcite_backend.execute_sql(calcite_sql, "u")
    assert r.rows == [("42",)]


def test_json_surface_over_the_wire(calcite_backend):
    calcite_backend._extensions.add("json")
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", backend=calcite_backend)
    time.sleep(0.1)
    try:
        c = MiniPgClient("127.0.0.1", port)
        try:
            r = c.query("SELECT '{\"a\":42}'->>'a' AS v")
            assert r["error"] is None, r["error"]
            assert r["rows"] == [["42"]]
        finally:
            c.close()
    finally:
        srv.shutdown()
        calcite_backend._extensions.discard("json")
