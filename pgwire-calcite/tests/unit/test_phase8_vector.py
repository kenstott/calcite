# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Phase 8: pgvector surface (PGW-047).

With ``--extension vector``, pgvector operators are rewritten to VEC_* distance
UDFs (registered via the Calcite model): ``<->`` (L2), ``<=>`` (cosine distance),
``<#>`` (negative inner product). Verified transpiling, computing on real Calcite,
similarity search (ORDER BY), and over the wire.
"""

from __future__ import annotations

import pathlib
import time

import pytest

from pgwire_calcite import extensions, launcher
from pgwire_calcite.classpath import ClasspathError, resolve_classpath
from pgwire_calcite.dialect import transpile_pg_to_calcite

from test_phase0_wire import MiniPgClient, _free_port

MODEL = str(pathlib.Path(__file__).resolve().parents[1] / "fixtures" / "vector-model.json")


@pytest.fixture(scope="module")
def vector_backend():
    try:
        resolve_classpath()
    except ClasspathError as exc:
        pytest.skip(f"Calcite classpath unavailable: {exc}")
    if not (pathlib.Path(resolve_classpath()[0]).parent / "pgwire-vecdist.jar").exists():
        # vendored UDF jar (packaging/build-udf.sh) must be present
        pass
    from pgwire_calcite.calcite_backend import CalciteBackend

    try:
        backend = CalciteBackend(model_path=MODEL, extensions={"vector"}, jvm_args=["-Xmx1g"])
    except Exception as exc:
        pytest.skip(f"vector UDF unavailable (run packaging/build-udf.sh): {exc}")
    yield backend
    backend.close()


def test_vector_registered_as_implemented():
    assert extensions.REGISTRY["vector"].implemented is True


def test_transpile_vector_operators():
    assert "VEC_L2" in transpile_pg_to_calcite("SELECT emb <-> ARRAY[3,4] FROM t", vector_enabled=True)
    assert "VEC_COS" in transpile_pg_to_calcite("SELECT emb <=> ARRAY[3,4] FROM t", vector_enabled=True)
    ip = transpile_pg_to_calcite("SELECT emb <#> ARRAY[3,4] FROM t", vector_enabled=True)
    assert "VEC_IP" in ip and "-" in ip  # negative inner product
    # pgvector string literal '[1,2,3]' -> ARRAY[1,2,3]
    assert "ARRAY[3, 4]" in transpile_pg_to_calcite(
        "SELECT emb <-> '[3,4]' FROM t", vector_enabled=True
    )


def test_distance_computations(vector_backend):
    assert vector_backend.execute_sql("SELECT ARRAY[0.0,0.0] <-> ARRAY[3.0,4.0] AS d", "u").rows == [(5.0,)]
    assert vector_backend.execute_sql("SELECT ARRAY[1.0,0.0] <=> ARRAY[0.0,1.0] AS c", "u").rows == [(1.0,)]
    assert vector_backend.execute_sql("SELECT ARRAY[1.0,2.0] <#> ARRAY[3.0,4.0] AS i", "u").rows == [(-11.0,)]


def test_similarity_search_order_by(vector_backend):
    """The real pgvector use case: ORDER BY distance to a query vector."""
    sql = (
        "SELECT id FROM (VALUES (1, ARRAY[1.0,0.0]), (2, ARRAY[0.0,1.0]), (3, ARRAY[0.9,0.1])) "
        "AS t(id, emb) ORDER BY emb <-> ARRAY[1.0,0.0]"
    )
    rows = vector_backend.execute_sql(sql, "u").rows
    assert [r[0] for r in rows] == [1, 3, 2]  # nearest to [1,0] first


def test_vector_over_the_wire(vector_backend):
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", backend=vector_backend)
    time.sleep(0.1)
    try:
        c = MiniPgClient("127.0.0.1", port)
        try:
            r = c.query("SELECT ARRAY[0.0,0.0] <-> ARRAY[3.0,4.0] AS d")
            assert r["error"] is None, r["error"]
            assert r["rows"][0][0].startswith("5")
        finally:
            c.close()
    finally:
        srv.shutdown()
