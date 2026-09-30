# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""corr()/regr_*() reach the file adapter's agg_* aliases via the pre-parse rewrite (no JVM)."""

from __future__ import annotations

import threading

from pgwire_calcite.calcite_backend import CalciteBackend


class _Rewriter:
    def rewrite(self, sql):
        return sql.replace("CORR(", "agg_corr(")


class _Stmt:
    def __init__(self, sink):
        self.sink = sink

    def execute(self, sql):
        self.sink.append(sql)
        return False

    def close(self):
        pass


class _Conn:
    def __init__(self, sink):
        self.sink = sink

    def createStatement(self):
        return _Stmt(self.sink)


def _backend(rewriter, sink):
    b = object.__new__(CalciteBackend)
    b._extensions = set()
    b.admission = None
    b._stats_rewriter = rewriter
    b.lane = lambda lane: (_Conn(sink), threading.RLock())
    return b


def test_execute_sql_applies_stats_rewrite():
    sink = []
    _backend(_Rewriter(), sink).execute_sql("select corr(a - b, c) from t", "r")
    assert sink == ["SELECT agg_corr(a - b, c) FROM t"]


def test_execute_sql_without_rewriter_passes_sql_unchanged():
    sink = []
    _backend(None, sink).execute_sql("select corr(a, c) from t", "r")
    assert sink == ["SELECT CORR(a, c) FROM t"]
