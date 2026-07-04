# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Phase 0 corpus harness smoke test.

Runs every corpus entry gated at phase <= 0 against the running stub server and
asserts the wire stays healthy (a result or a clean ErrorResponse — never a
dropped connection). Higher-phase entries are loaded but skipped until their
phase lands. This proves the harness records and replays; correctness of the
catalog probes is asserted in Phase 2.
"""

from __future__ import annotations

import importlib.util
import pathlib
import sys
import time

import pytest

from pgwire_calcite import launcher

from test_phase0_wire import MiniPgClient, _free_port

# Load the corpus harness module by path (tests/corpus is not an importable package).
_HARNESS = pathlib.Path(__file__).resolve().parents[1] / "corpus" / "harness.py"
_spec = importlib.util.spec_from_file_location("corpus_harness", _HARNESS)
harness = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = harness  # dataclass needs the module registered
_spec.loader.exec_module(harness)


def test_corpus_loads_and_summarizes():
    summary = harness.summarize()
    assert summary["total"] >= 1
    assert "psql" in summary["by_client"]


@pytest.fixture()
def server():
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none")
    time.sleep(0.1)
    yield "127.0.0.1", port
    srv.shutdown()


def test_phase0_corpus_replays_without_dropping_connection(server):
    host, port = server
    phase0 = harness.entries_for_phase(0)
    assert phase0, "expected at least one phase-0 corpus entry"
    c = MiniPgClient(host, port)
    try:
        for entry in phase0:
            result = c.query(entry.sql)
            # Healthy wire: either an answer or a clean error, connection intact.
            assert result is not None
            if entry.expect and "rows" in entry.expect:
                assert result["error"] is None, (entry.path.name, result["error"])
                assert result["rows"] == entry.expect["rows"], entry.path.name
            if entry.expect and "columns" in entry.expect:
                assert result["columns"] == entry.expect["columns"], entry.path.name
    finally:
        c.close()
