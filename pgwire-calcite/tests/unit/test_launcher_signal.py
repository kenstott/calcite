# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SIGTERM/SIGINT must request a clean shutdown, not hang or get swallowed.

The Java launcher's shutdown hook sends SIGTERM to this process and waits
(bounded) for it to exit before the JVM halts (packaging/launcher/.../Launcher.java).
If nothing re-installs Python's own handler for that signal, or something else
installs a handler afterward (as the embedded Calcite JVM does via JPype unless
started with -Xrs), SIGTERM can be swallowed and the listening port never gets
released. These tests exercise ``install_shutdown_handler`` directly and prove
it wins even when installed after another handler, without needing a real JVM.
"""

from __future__ import annotations

import os
import signal
import threading
import time

from pgwire_calcite.launcher import install_shutdown_handler, watch_owner


def test_sigterm_sets_stop_event():
    stop = threading.Event()
    install_shutdown_handler(stop)
    try:
        assert not stop.is_set()
        os.kill(os.getpid(), signal.SIGTERM)
        assert stop.is_set()
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        signal.signal(signal.SIGINT, signal.SIG_DFL)


def test_sigint_sets_stop_event():
    stop = threading.Event()
    install_shutdown_handler(stop)
    try:
        assert not stop.is_set()
        os.kill(os.getpid(), signal.SIGINT)
        assert stop.is_set()
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        signal.signal(signal.SIGINT, signal.SIG_DFL)


def test_installed_after_another_handler_wins():
    """Mirrors the JPype/embedded-JVM race: whichever installs SIGTERM last wins.

    A prior handler (standing in for the JVM's own SIGTERM handler installed
    during jpype.startJVM()) must be overridden by install_shutdown_handler
    when it is called afterward, exactly as launcher.main() now does (backend
    construction, which starts the JVM, happens before this call).
    """
    earlier_handler_ran = threading.Event()

    def _earlier(signum, frame):  # noqa: ANN001 - signal handler signature
        del signum, frame
        earlier_handler_ran.set()

    signal.signal(signal.SIGTERM, _earlier)
    stop = threading.Event()
    install_shutdown_handler(stop)
    try:
        os.kill(os.getpid(), signal.SIGTERM)
        assert stop.is_set()
        assert not earlier_handler_ran.is_set()
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        signal.signal(signal.SIGINT, signal.SIG_DFL)


def test_stop_wait_unblocks_promptly_on_sigterm():
    """End-to-end within the process: a thread blocked on stop.wait() (as
    main() is) returns within a small bound of SIGTERM arriving -- the shape
    of the ~5s port-release requirement, without needing the real server."""
    stop = threading.Event()
    install_shutdown_handler(stop)
    unblocked_at = {}
    start = time.monotonic()

    def _waiter():
        stop.wait()
        unblocked_at["t"] = time.monotonic()

    t = threading.Thread(target=_waiter)
    t.start()
    try:
        time.sleep(0.05)
        os.kill(os.getpid(), signal.SIGTERM)
        t.join(timeout=2.0)
        assert not t.is_alive()
        assert unblocked_at["t"] - start < 1.0
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        signal.signal(signal.SIGINT, signal.SIG_DFL)


def test_owner_watch_requests_shutdown_when_the_owner_dies():
    """--owner-pid: the server stops on its own once the host that started it is gone, with
    no signal delivered — the case a SIGKILLed host leaves behind."""
    import subprocess
    import sys

    owner = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    stop = threading.Event()
    try:
        watch_owner(owner.pid, stop)
        assert not stop.wait(1.5), "stop set while the owner was alive"
        owner.kill()
        owner.wait(timeout=5)
        assert stop.wait(5), "stop not set within 5s of the owner dying"
    finally:
        if owner.poll() is None:
            owner.kill()
