# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Phase 5 tests: supervisor auto-restart, backoff, crash-loop breaker, RSS
recycle, liveness restart, readiness gate, graceful shutdown (PGW-033..038).

Control logic is exercised deterministically with an injected clock and a fake
spawn; one real-subprocess test covers RSS reading + proactive recycle.
"""

from __future__ import annotations

import sys
import time

import pytest

from pgwire_calcite.supervisor import ChildSpec, Supervisor


class FakeProc:
    _ctr = 5000

    def __init__(self, argv, env=None):
        FakeProc._ctr += 1
        self.pid = FakeProc._ctr
        self.argv = argv
        self._rc = None
        self.terminated = False
        self.killed = False

    def poll(self):
        return self._rc

    def wait(self, timeout=None):
        if self._rc is None:
            self._rc = 0
        return self._rc

    def terminate(self):
        self.terminated = True
        self._rc = 0

    def kill(self):
        self.killed = True
        self._rc = -9

    def die(self, rc=1):
        self._rc = rc


class Spawner:
    def __init__(self):
        self.created = []

    def __call__(self, argv, env=None):
        p = FakeProc(argv, env)
        self.created.append(p)
        return p


class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


def _sup(spec, spawn, clock, **kw):
    return Supervisor([spec], clock=clock, spawn=spawn, **kw)


def test_restart_on_crash_after_backoff():
    sp, clk = Spawner(), Clock()
    sup = _sup(ChildSpec("x", ["cmd"]), sp, clk)
    sup.start_all()
    assert len(sp.created) == 1
    sp.created[0].die()
    sup.tick()  # observe death -> schedule restart at t+0.5
    assert len(sp.created) == 1
    clk.advance(0.6)
    sup.tick()  # backoff elapsed -> restart
    assert len(sp.created) == 2


def test_backoff_grows_between_restarts():
    sp, clk = Spawner(), Clock()
    sup = _sup(ChildSpec("x", ["cmd"]), sp, clk)
    sup.start_all()
    delays = []
    for _ in range(3):
        sp.created[-1].die()
        sup.tick()  # schedules; next_restart_at - now == delay
        delays.append(round(sup.state("x").next_restart_at - clk(), 3))
        clk.advance(sup.state("x").next_restart_at - clk() + 0.001)
        sup.tick()  # restart
    assert delays == [0.5, 1.0, 2.0]


def test_crash_loop_breaker_trips_and_fails_loud():
    sp, clk = Spawner(), Clock()
    tripped = []
    # never becomes ready -> failures never cleared by a successful run
    spec = ChildSpec("x", ["cmd"], readiness=lambda: False)
    sup = _sup(spec, sp, clk, on_crash_loop=tripped.append)
    sup.start_all()
    for _ in range(5):
        sp.created[-1].die()
        sup.tick()  # record failure (+ maybe trip)
        if sup.state("x").breaker.tripped:
            break
        clk.advance(sup.state("x").next_restart_at - clk() + 0.001)
        sup.tick()  # restart
    assert tripped == ["x"]
    assert sup.is_up("x") is False


def test_liveness_failure_restarts():
    sp, clk = Spawner(), Clock()
    healthy = {"v": True}
    spec = ChildSpec("x", ["cmd"], liveness=lambda: healthy["v"])
    sup = _sup(spec, sp, clk)
    sup.start_all()
    sup.tick()  # readiness gate passes (readiness None) -> ready
    assert sup.is_up("x")
    healthy["v"] = False
    sup.tick()  # liveness fails -> abrupt restart
    assert len(sp.created) == 2
    assert sp.created[0].killed  # restarted abruptly


def test_readiness_gate():
    sp, clk = Spawner(), Clock()
    ready = {"v": False}
    spec = ChildSpec("x", ["cmd"], readiness=lambda: ready["v"])
    sup = _sup(spec, sp, clk)
    sup.start_all()
    sup.tick()
    assert sup.is_up("x") is False  # not ready yet
    ready["v"] = True
    sup.tick()
    assert sup.is_up("x") is True


def test_rss_recycle_is_graceful_and_not_a_crash():
    sp, clk = Spawner(), Clock()
    spec = ChildSpec("x", ["cmd"], rss_limit_mb=100)
    sup = _sup(spec, sp, clk)
    sup.start_all()
    sup.tick()  # becomes ready
    sup.state("x").child.rss_mb = lambda: 999.0  # over the limit
    sup.tick()  # -> graceful recycle
    assert len(sp.created) == 2
    assert sp.created[0].terminated and not sp.created[0].killed  # graceful drain
    assert sup.state("x").breaker._failures == []  # recycle is not a crash


def test_graceful_shutdown_stops_children():
    sp, clk = Spawner(), Clock()
    sup = _sup(ChildSpec("x", ["cmd"]), sp, clk)
    sup.start_all()
    sup.shutdown()
    assert sp.created[0].terminated


def test_real_subprocess_rss_recycle():
    """Real OS RSS read + proactive recycle of a memory-hogging child.

    Runs on every platform: _read_rss_mb uses /proc where it exists and ``ps``
    where it does not, so there is no host on which this silently does nothing.
    """
    # The child must keep *touching* its pages, not merely allocate them: macOS's
    # memory compressor evicts an idle 120 MiB buffer down to ~15 MiB within a
    # second, so an allocate-then-sleep child races the compressor and reports an
    # RSS under the limit. Sweeping one byte per 4 KiB page keeps it resident on
    # every platform.
    _HOG = (
        "import os, sys, time\n"
        "n = 120 * 1024 * 1024\n"
        "mv = memoryview(bytearray(os.urandom(n)))\n"
        "sys.stderr.write('allocated\\n'); sys.stderr.flush()\n"
        "deadline = time.monotonic() + 30\n"
        "while time.monotonic() < deadline:\n"
        "    for off in range(0, n, 4096):\n"
        "        mv[off] = (mv[off] + 1) & 0xFF\n"
    )
    hog = [sys.executable, "-c", _HOG]
    spec = ChildSpec("hog", hog, rss_limit_mb=40, graceful_stop_timeout=3)
    sup = Supervisor([spec], clock=time.monotonic)
    sup.start_all()
    try:
        time.sleep(2.0)  # let it allocate and go resident
        first_pid = sup.state("hog").child.pid
        sup.tick()  # ready
        sup.tick()  # RSS over 40MiB -> recycle
        assert sup.state("hog").child.pid != first_pid, "expected a recycled process"
    finally:
        sup.shutdown()
