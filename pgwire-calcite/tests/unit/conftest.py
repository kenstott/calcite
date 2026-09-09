# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Shared pytest fixtures.

JPype starts at most one JVM per process, so the embedded Calcite backend is a
single session-scoped fixture shared by every JVM-backed test (Phases 1, 2, …).
Skips with a clear reason only if the Calcite classpath cannot be resolved.
"""

from __future__ import annotations

import pathlib

import pytest

from pgwire_calcite.classpath import ClasspathError, resolve_classpath

FIXTURES = pathlib.Path(__file__).resolve().parents[1] / "fixtures"
MODEL = str(FIXTURES / "file-model.json")
WIDE_MODEL = str(FIXTURES / "wide-model.json")


@pytest.fixture(scope="session")
def calcite_backend():
    try:
        resolve_classpath()
    except ClasspathError as exc:
        pytest.skip(f"Calcite classpath unavailable: {exc}")
    from pgwire_calcite.calcite_backend import CalciteBackend

    backend = CalciteBackend(model_path=MODEL, jvm_args=["-Xmx1g"])
    yield backend
    backend.close()


@pytest.fixture(scope="session")
def widetypes_backend():
    """A second Calcite connection over the wide-typed fixture (Phase 4/9).

    Kept in its own model/directory so the type-coverage table does not widen the
    table list every other suite asserts on. The JVM is shared -- JPype starts at
    most one -- only the JDBC connection is separate.
    """
    try:
        resolve_classpath()
    except ClasspathError as exc:
        pytest.skip(f"Calcite classpath unavailable: {exc}")
    from pgwire_calcite.calcite_backend import CalciteBackend

    backend = CalciteBackend(model_path=WIDE_MODEL, jvm_args=["-Xmx1g"])
    yield backend
    backend.close()
