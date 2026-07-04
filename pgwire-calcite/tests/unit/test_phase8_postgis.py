# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Phase 8: PostGIS-subset surface (PGW-048).

With the ``postgis`` surface enabled, ST_* functions resolve via Calcite's
spatial library (the surface adds ``spatial`` to the connection ``fun``; the
function names already match PostGIS, so they pass through transpile). Verified
against real Calcite and over the wire.
"""

from __future__ import annotations

import pathlib
import time

import pytest

from pgwire_calcite import extensions, launcher
from pgwire_calcite.classpath import ClasspathError, resolve_classpath

from test_phase0_wire import MiniPgClient, _free_port

MODEL = str(pathlib.Path(__file__).resolve().parents[1] / "fixtures" / "file-model.json")


@pytest.fixture(scope="module")
def postgis_backend():
    try:
        resolve_classpath()
    except ClasspathError as exc:
        pytest.skip(f"Calcite classpath unavailable: {exc}")
    from pgwire_calcite.calcite_backend import CalciteBackend

    backend = CalciteBackend(model_path=MODEL, extensions={"postgis"}, jvm_args=["-Xmx1g"])
    yield backend
    backend.close()


def test_postgis_registered_as_implemented():
    assert extensions.REGISTRY["postgis"].implemented is True


def test_st_distance(postgis_backend):
    r = postgis_backend.execute_sql("SELECT ST_Distance(ST_Point(0,0), ST_Point(3,4)) AS d", "u")
    assert r.rows == [(5.0,)]


def test_st_contains(postgis_backend):
    r = postgis_backend.execute_sql(
        "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0,0 4,4 4,4 0,0 0))'), ST_Point(2,2)) AS c",
        "u",
    )
    assert r.rows == [(True,)]


def test_postgis_over_the_wire(postgis_backend):
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", backend=postgis_backend)
    time.sleep(0.1)
    try:
        c = MiniPgClient("127.0.0.1", port)
        try:
            r = c.query("SELECT ST_Distance(ST_Point(0,0), ST_Point(3,4)) AS d")
            assert r["error"] is None, r["error"]
            assert r["rows"] and r["rows"][0][0].startswith("5")
        finally:
            c.close()
    finally:
        srv.shutdown()
