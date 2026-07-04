# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Thin supervisor for recyclable child processes (PGW-032..038).

A minimal, failure-averse supervisor that keeps child processes up:
- auto-restart on exit/crash, on liveness-probe failure, and on RSS threshold
  (proactive memory recycle) — PGW-034;
- exponential backoff with a crash-loop circuit breaker that FAILS LOUD after N
  rapid failures rather than spinning silently — PGW-035;
- graceful drain on intentional recycle (SIGTERM, wait, then SIGKILL) vs. abrupt
  restart on crash — PGW-036;
- readiness gating: a freshly (re)started child is not "up" until its readiness
  probe passes — PGW-037;
- children hold nothing un-rebuildable; the supervisor re-provides their argv/env
  on every restart — PGW-038.

The supervisor itself carries only this monitoring logic (PGW-033): the Calcite
JVM and the pgwire server run as children, never in the supervisor's process.

Design is ``tick()``-driven with an injectable clock so the control logic is
deterministically testable without real sleeps; ``run()`` just calls ``tick()``
on an interval.
"""

from __future__ import annotations

import logging
import os
import signal
import subprocess
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

log = logging.getLogger(__name__)

Clock = Callable[[], float]


class CrashLoopError(RuntimeError):
    """A child crash-looped past the breaker threshold. Surfaced, never swallowed."""


@dataclass
class ChildSpec:
    name: str
    argv: List[str]
    env: Optional[dict] = None
    readiness: Optional[Callable[[], bool]] = None  # True when the child can serve
    liveness: Optional[Callable[[], bool]] = None  # active health probe over the bridge
    rss_limit_mb: Optional[float] = None
    graceful_stop_timeout: float = 10.0


class Backoff:
    """Exponential backoff with a cap. reset() after a clean run."""

    def __init__(self, base: float = 0.5, factor: float = 2.0, cap: float = 30.0) -> None:
        self._base = base
        self._factor = factor
        self._cap = cap
        self._n = 0

    def next(self) -> float:
        delay = min(self._cap, self._base * (self._factor ** self._n))
        self._n += 1
        return delay

    def reset(self) -> None:
        self._n = 0


class CrashLoopBreaker:
    """Trips when >= max_failures crashes occur within `window` seconds."""

    def __init__(self, max_failures: int = 5, window: float = 60.0) -> None:
        self._max = max_failures
        self._window = window
        self._failures: List[float] = []
        self.tripped = False

    def record_failure(self, now: float) -> bool:
        self._failures.append(now)
        self._failures = [t for t in self._failures if now - t <= self._window]
        if len(self._failures) >= self._max:
            self.tripped = True
        return self.tripped

    def record_success(self, now: float) -> None:
        self._failures.clear()


def _read_rss_mb(pid: int) -> Optional[float]:
    """Resident set size in MiB from /proc (Linux). None if unavailable."""
    try:
        with open(f"/proc/{pid}/status", "r", encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    kb = float(line.split()[1])
                    return kb / 1024.0
    except (OSError, ValueError, IndexError):
        return None
    return None


class SupervisedChild:
    """Wraps one OS process: spawn, poll, RSS, graceful/abrupt stop."""

    def __init__(self, spec: ChildSpec, spawn: Callable = subprocess.Popen) -> None:
        self.spec = spec
        self._spawn = spawn
        self.process = None

    def start(self) -> None:
        env = dict(os.environ)
        if self.spec.env:
            env.update(self.spec.env)
        self.process = self._spawn(self.spec.argv, env=env)
        log.info("[SUPERVISOR] started %s pid=%s", self.spec.name, self.pid)

    @property
    def pid(self) -> Optional[int]:
        return self.process.pid if self.process is not None else None

    def poll(self) -> Optional[int]:
        return self.process.poll() if self.process is not None else None

    def is_alive(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def rss_mb(self) -> Optional[float]:
        return _read_rss_mb(self.pid) if self.pid is not None else None

    def is_ready(self) -> bool:
        if self.spec.readiness is None:
            return True
        try:
            return bool(self.spec.readiness())
        except Exception:
            return False

    def is_live(self) -> bool:
        if self.spec.liveness is None:
            return True
        try:
            return bool(self.spec.liveness())
        except Exception:
            return False

    def stop(self, graceful: bool = True) -> None:
        if self.process is None or self.process.poll() is not None:
            return
        if graceful:
            # Drain: ask nicely, wait, then force.
            self.process.terminate()
            try:
                self.process.wait(timeout=self.spec.graceful_stop_timeout)
                return
            except subprocess.TimeoutExpired:
                log.warning("[SUPERVISOR] %s did not drain in time; killing", self.spec.name)
        self.process.kill()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass


@dataclass
class _ChildState:
    child: SupervisedChild
    backoff: Backoff = field(default_factory=Backoff)
    breaker: CrashLoopBreaker = field(default_factory=CrashLoopBreaker)
    next_restart_at: float = 0.0
    ready: bool = False


class Supervisor:
    """Monitors a set of children, restarting/recycling them per policy."""

    def __init__(
        self,
        specs: List[ChildSpec],
        clock: Clock = time.monotonic,
        on_crash_loop: Optional[Callable[[str], None]] = None,
        spawn: Callable = subprocess.Popen,
    ) -> None:
        self._clock = clock
        self._on_crash_loop = on_crash_loop
        self._states: Dict[str, _ChildState] = {
            s.name: _ChildState(SupervisedChild(s, spawn=spawn)) for s in specs
        }

    def start_all(self) -> None:
        for st in self._states.values():
            st.child.start()

    def _restart(self, st: _ChildState, graceful: bool) -> None:
        st.child.stop(graceful=graceful)
        st.child.start()
        st.ready = False

    def tick(self) -> None:
        """One monitoring pass. Call on an interval (or directly in tests)."""
        now = self._clock()
        for name, st in self._states.items():
            if st.breaker.tripped:
                continue  # already surfaced; stay down until operator intervenes

            if not st.child.is_alive():
                if st.next_restart_at == 0.0:
                    # First observation of this death: count one crash failure and
                    # schedule a backed-off restart (or trip the breaker, loud).
                    if st.breaker.record_failure(now):
                        log.error(
                            "[SUPERVISOR] %s crash-looped (>=%d failures in %.0fs); FAILING LOUD",
                            name,
                            st.breaker._max,
                            st.breaker._window,
                        )
                        if self._on_crash_loop is not None:
                            self._on_crash_loop(name)
                        continue
                    delay = st.backoff.next()
                    st.next_restart_at = now + delay
                    log.warning("[SUPERVISOR] %s exited; restart in %.1fs", name, delay)
                elif now >= st.next_restart_at:
                    # Backoff window elapsed -> actually restart (abruptly).
                    self._restart(st, graceful=False)
                    st.next_restart_at = 0.0
                continue

            # Readiness gate: not "up" until the probe passes.
            if not st.ready:
                st.ready = st.child.is_ready()
                if st.ready:
                    st.backoff.reset()
                    st.breaker.record_success(now)
                continue

            # Proactive RSS recycle — an intentional, graceful recycle, NOT a crash.
            if st.child.spec.rss_limit_mb is not None:
                rss = st.child.rss_mb()
                if rss is not None and rss > st.child.spec.rss_limit_mb:
                    log.info(
                        "[SUPERVISOR] recycling %s (RSS %.0fMiB > %.0fMiB)",
                        name,
                        rss,
                        st.child.spec.rss_limit_mb,
                    )
                    self._restart(st, graceful=True)
                    continue

            # Liveness failure -> restart (abruptly; it is wedged, not draining).
            if not st.child.is_live():
                log.warning("[SUPERVISOR] %s failed liveness; restarting", name)
                self._restart(st, graceful=False)

    def due_restart(self, name: str) -> None:
        """Force a scheduled restart now (used after a backoff window elapses)."""
        st = self._states[name]
        if not st.child.is_alive() and not st.breaker.tripped:
            self._restart(st, graceful=False)
            st.next_restart_at = 0.0

    def recycle(self, name: str, graceful: bool = True) -> None:
        """Operator/scheduled recycle of a child (not counted as a crash)."""
        st = self._states[name]
        self._restart(st, graceful=graceful)

    def is_up(self, name: str) -> bool:
        st = self._states[name]
        return st.child.is_alive() and st.ready and not st.breaker.tripped

    def state(self, name: str) -> _ChildState:
        return self._states[name]

    def run(self, interval: float = 1.0, stop: Optional[Callable[[], bool]] = None) -> None:
        while stop is None or not stop():
            self.tick()
            time.sleep(interval)

    def shutdown(self) -> None:
        for st in self._states.values():
            st.child.stop(graceful=True)
