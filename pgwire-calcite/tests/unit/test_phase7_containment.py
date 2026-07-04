# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Phase 7 tests: containment (PGW-040) + corpus-upgrade gate (PGW-041).

Containment is asserted as an explicit property: a rejected/erroring query does
not break the session (the dialect firewall + Arrow bridge bound the blast
radius), and a Calcite execution error surfaces as a clean wire error with the
connection still usable. The corpus gate runs the client corpus and fails loud on
any regression.
"""

from __future__ import annotations

import pathlib
import time

import pytest

from pgwire_calcite import corpus_gate, launcher

from test_phase0_wire import MiniPgClient, _free_port

CORPUS_ROOT = pathlib.Path(__file__).resolve().parents[1] / "corpus"


@pytest.fixture()
def server(calcite_backend):
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", backend=calcite_backend)
    time.sleep(0.1)
    yield "127.0.0.1", port
    srv.shutdown()


# --- Containment (PGW-040) ----------------------------------------------------

def test_dialect_firewall_reject_does_not_break_session(server):
    """A PG-only construct is rejected loudly; the session keeps working."""
    host, port = server
    c = MiniPgClient(host, port)
    try:
        bad = c.query("SELECT DISTINCT ON (DEPTNO) DEPTNO FROM EMPS")
        assert bad["error"] is not None  # firewalled
        # same connection still serves the next query
        good = c.query("SELECT count(*) AS n FROM EMPS")
        assert good["error"] is None and good["rows"] == [["10"]]
    finally:
        c.close()


def test_calcite_error_surfaces_and_session_survives(server):
    host, port = server
    c = MiniPgClient(host, port)
    try:
        err = c.query("SELECT * FROM no_such_table_zzz")
        assert err["error"] is not None  # Calcite error contained, surfaced
        ok = c.query("SELECT 1")
        assert ok["error"] is None and ok["rows"] == [["1"]]
    finally:
        c.close()


# --- Corpus-upgrade gate (PGW-041) --------------------------------------------

def test_corpus_gate_passes(server):
    host, port = server
    c = MiniPgClient(host, port)
    try:
        result = corpus_gate.gate(c.query, phase=2, root=CORPUS_ROOT)
        assert result.passed, result.report()
        assert result.ran >= 3
    finally:
        c.close()


def test_corpus_gate_detects_regression(server):
    """The gate must FAIL loud when a probe regresses (proves it actually gates)."""
    host, port = server
    c = MiniPgClient(host, port)

    def broken_query(sql):
        # simulate a Calcite upgrade that breaks catalog introspection
        if "information_schema" in sql or "pg_catalog" in sql:
            return {"columns": [], "rows": [], "error": "relation does not exist"}
        return c.query(sql)

    try:
        result = corpus_gate.gate(broken_query, phase=2, root=CORPUS_ROOT)
        assert not result.passed
        assert result.failures
    finally:
        c.close()
