# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Regression test for the startup-handshake timeout in CalciteHandler.

Measured live (2026-09-21): a long-lived pgwire-govdata process accumulated
hundreds of stuck handler threads over a few hours and stopped answering real
queries. Root cause: handle_startup's first read is a plain blocking socket
read with no timeout, so a connection that opens the TCP socket but never
sends a complete startup packet -- a bare liveness probe, a client that
connects and vanishes -- parks that thread forever: handle() never returns,
finish() never runs, and the FD is never released. This test connects and
sends nothing, and asserts the server actually closes the connection and
recovers its connection count instead of hanging.
"""

from __future__ import annotations

import socket
import time

import pytest

from pgwire_calcite import launcher
from pgwire_calcite.server import CalciteHandler
from tests.unit.test_phase0_wire import _free_port


class _NoOpBackend:
    """A backend is never reached -- the probe never completes the handshake."""

    def ready(self) -> bool:
        return True


@pytest.fixture()
def bare_server():
    backend = _NoOpBackend()
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", backend=backend)
    time.sleep(0.1)
    yield srv, "127.0.0.1", port
    srv.shutdown()


def test_incomplete_handshake_is_dropped_not_leaked(bare_server):
    srv, host, port = bare_server

    sock = socket.create_connection((host, port), timeout=5)
    try:
        # Deliberately send nothing -- exactly a bare TCP liveness probe, the
        # shape of connection that used to park a handler thread forever.
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and srv._active_connections < 1:
            time.sleep(0.02)
        assert srv._active_connections >= 1

        # The server must close its end well inside the startup timeout bound
        # rather than hanging -- it may first send a Postgres ErrorResponse
        # ('E'), so drain to EOF rather than expecting a bare, immediate close.
        sock.settimeout(CalciteHandler._STARTUP_TIMEOUT_SECONDS + 10)
        saw_eof = False
        for _ in range(1000):
            data = sock.recv(4096)
            if data == b"":
                saw_eof = True
                break
        assert saw_eof, "server should close an incomplete handshake, not hang"
    finally:
        sock.close()

    # finish() must have run and released the connection slot.
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and srv._active_connections != 0:
        time.sleep(0.05)
    assert srv._active_connections == 0
