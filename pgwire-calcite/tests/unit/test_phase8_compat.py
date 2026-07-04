# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Phase 8: compat-function surface — pg_trgm + pgcrypto (PGW-050).

similarity()/word_similarity() (trigram) and digest() (hash) as Calcite UDFs.
They pass through transpile as normal function calls (no operator rewrite; pg_trgm
`%` is intentionally NOT rewritten — it is ambiguous with numeric modulo).
"""

from __future__ import annotations

import hashlib
import pathlib
import time

import pytest

from pgwire_calcite import extensions, launcher
from pgwire_calcite.classpath import ClasspathError, resolve_classpath
from pgwire_calcite.dialect import transpile_pg_to_calcite

from test_phase0_wire import MiniPgClient, _free_port

MODEL = str(pathlib.Path(__file__).resolve().parents[1] / "fixtures" / "vector-model.json")


@pytest.fixture(scope="module")
def compat_backend():
    try:
        resolve_classpath()
    except ClasspathError as exc:
        pytest.skip(f"Calcite classpath unavailable: {exc}")
    from pgwire_calcite.calcite_backend import CalciteBackend

    try:
        backend = CalciteBackend(model_path=MODEL, jvm_args=["-Xmx1g"])
    except Exception as exc:
        pytest.skip(f"compat UDFs unavailable (run packaging/build-udf.sh): {exc}")
    yield backend
    backend.close()


def test_pg_trgm_registered_as_implemented():
    assert extensions.REGISTRY["pg_trgm"].implemented is True


def test_compat_functions_pass_through_transpile():
    out = transpile_pg_to_calcite("SELECT similarity(a, b), digest(x, y) FROM t")
    assert "SIMILARITY" in out and "DIGEST" in out


def test_similarity(compat_backend):
    identical = compat_backend.execute_sql("SELECT similarity('abc', 'abc') AS s", "u").rows[0][0]
    assert identical == 1.0
    similar = compat_backend.execute_sql("SELECT similarity('foobar', 'foobaz') AS s", "u").rows[0][0]
    assert 0.0 < similar < 1.0
    unrelated = compat_backend.execute_sql("SELECT similarity('abc', 'xyz') AS s", "u").rows[0][0]
    assert unrelated < similar


def test_word_similarity(compat_backend):
    r = compat_backend.execute_sql("SELECT word_similarity('cat', 'the cat sat') AS w", "u")
    assert r.rows[0][0] == 1.0  # 'cat' matches the word 'cat' exactly


def test_digest_matches_hashlib(compat_backend):
    for algo in ("sha256", "sha1", "md5", "sha512"):
        got = compat_backend.execute_sql(f"SELECT digest('hello', '{algo}') AS d", "u").rows[0][0]
        expected = hashlib.new(algo, b"hello").hexdigest()
        assert got == expected, algo


def test_compat_over_the_wire(compat_backend):
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", backend=compat_backend)
    time.sleep(0.1)
    try:
        c = MiniPgClient("127.0.0.1", port)
        try:
            r = c.query("SELECT digest('abc', 'sha256') AS d")
            assert r["error"] is None, r["error"]
            assert r["rows"][0][0] == hashlib.sha256(b"abc").hexdigest()
        finally:
            c.close()
    finally:
        srv.shutdown()
