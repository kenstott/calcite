# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admission control through the real wire server and the embedded Calcite engine."""

from __future__ import annotations

import time

from pgwire_calcite import launcher
from pgwire_calcite.admission import AdmissionPolicy, TableCoverage
from pgwire_calcite.calcite_backend import CalciteBackend, CancelScope

from test_phase0_wire import MiniPgClient, _free_port


def test_unfiltered_scan_rejected_and_filtered_scan_served_over_the_wire(
    calcite_backend, monkeypatch
):
    policy = AdmissionPolicy(5, {"sales.emps": TableCoverage(1000, ("deptno",))})
    monkeypatch.setattr(CalciteBackend, "admission", policy)
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", backend=calcite_backend)
    time.sleep(0.1)
    try:
        c = MiniPgClient("127.0.0.1", port)
        try:
            rejected = c.query("SELECT ename FROM sales.emps")
            assert rejected["error"] is not None
            assert "sales.emps" in str(rejected["error"]) and "deptno" in str(rejected["error"])

            filtered = c.query("SELECT count(*) AS n FROM sales.emps WHERE deptno = 10")
            assert filtered["error"] is None, filtered["error"]

            limited = c.query("SELECT ename FROM sales.emps LIMIT 2")
            assert limited["error"] is None, limited["error"]
            assert len(limited["rows"]) == 2
        finally:
            c.close()
    finally:
        srv.shutdown()


def test_statement_queued_behind_a_held_connection_fails_fast_as_server_busy(
    calcite_backend, monkeypatch
):
    monkeypatch.setattr(CancelScope, "max_queue_wait_ms", 600)
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", backend=calcite_backend)
    time.sleep(0.1)
    _, lock = calcite_backend.lane("user")
    try:
        c = MiniPgClient("127.0.0.1", port)
        try:
            with lock:
                started = time.monotonic()
                busy = c.query("SELECT count(*) AS n FROM sales.emps")
                waited = time.monotonic() - started
            assert busy["error"] is not None and "server is busy" in busy["error"]
            assert "C57014" in busy["error"]
            assert 0.5 <= waited < 5

            after = c.query("SELECT count(*) AS n FROM sales.emps")
            assert after["error"] is None, after["error"]
        finally:
            c.close()
    finally:
        srv.shutdown()
