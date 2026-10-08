# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Start-up and shutdown: what a client sees before the server can answer, and what
happens to the port and the connected clients when the server stops or fails to start.
"""

from __future__ import annotations

import signal
import socket
import struct
import subprocess
import sys
import time

import psycopg
import pytest

from pgwire_calcite import launcher
from pgwire_calcite.backend import StubBackend

from test_phase0_wire import MiniPgClient, _free_port


def _wait_for_port(port: int, timeout_s: float = 30.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.5).close()
            return
        except OSError:
            time.sleep(0.05)
    raise AssertionError(f"nothing listening on {port}")


# --- before the server can answer ----------------------------------------------


def test_a_client_that_connects_during_startup_is_told_the_server_is_starting():
    """The port is claimed before the backend is built. A client that connects in that
    window gets PostgreSQL's own answer for it (FATAL 57P03), at once, instead of a
    connection nothing ever replies on."""
    port = _free_port()
    sock = launcher.claim_listen_socket("127.0.0.1", port)
    assert sock is not None
    responder = launcher.StartupResponder(sock).start()
    try:
        started = time.monotonic()
        with pytest.raises(psycopg.OperationalError) as caught:
            psycopg.connect(f"host=127.0.0.1 port={port} user=t dbname=postgres connect_timeout=10")
        assert time.monotonic() - started < 5
        # libpq reports a failed connection by message only; the SQLSTATE on the wire is
        # asserted in the raw-socket test below.
        assert "FATAL:  the database system is starting up" in str(caught.value)
    finally:
        # serve() stops the responder and the same socket starts answering for real.
        srv = launcher.serve(
            host="127.0.0.1", port=port, backend=StubBackend(), sock=sock,
            startup_responder=responder,
        )
    try:
        client = MiniPgClient("127.0.0.1", port)
        assert client.query("SELECT 1")["rows"] == [["1"]]
        client.close()
    finally:
        srv.shutdown()
        srv.server_close()


def test_the_startup_answer_survives_clients_that_send_nothing_or_garbage():
    port = _free_port()
    sock = launcher.claim_listen_socket("127.0.0.1", port)
    responder = launcher.StartupResponder(sock).start()
    try:
        socket.create_connection(("127.0.0.1", port)).close()  # a bare port probe
        junk = socket.create_connection(("127.0.0.1", port))
        junk.sendall(b"GET / HTTP/1.1\r\n\r\n")
        junk.settimeout(5)
        # Closed without an answer -- not a PostgreSQL client. The close arrives as EOF,
        # or as a reset when the server had not read all of what was sent.
        try:
            assert junk.recv(16) == b""
        except ConnectionResetError:
            pass
        junk.close()
        # An SSLRequest is declined and the startup packet that follows is answered.
        tls = socket.create_connection(("127.0.0.1", port))
        tls.sendall(struct.pack("!II", 8, 80877103))
        assert tls.recv(1) == b"N"
        body = struct.pack("!I", 196608) + b"user\x00t\x00\x00"
        tls.sendall(struct.pack("!I", len(body) + 4) + body)
        tls.settimeout(5)
        answer = tls.recv(4096)
        assert answer[:1] == b"E" and b"57P03" in answer
        tls.close()
    finally:
        responder.stop()
        sock.close()


def test_a_backend_that_fails_to_build_exits_the_process_instead_of_squatting_the_port(
    monkeypatch,
):
    """After the port is claimed, a start-up failure must end the process: a process the
    JVM's threads keep alive holds the port and answers nothing, and every later start
    loses the bind to it."""
    port = _free_port()
    exits: list = []

    def _boom(*args, **kwargs):
        raise RuntimeError("Environment variable 'GOVDATA_PARQUET_DIR' is not defined")

    def _record(stage, host, port_):
        exits.append((stage, host, port_))

    monkeypatch.setattr(launcher, "build_backend", _boom)
    monkeypatch.setattr(launcher, "exit_startup_failed", _record)

    with pytest.raises(RuntimeError):
        launcher.main(["--port", str(port), "--backend", "calcite", "--model", "missing.json"])

    assert exits == [("building the backend", "127.0.0.1", port)]


def test_jvm_arguments_reach_the_embedded_jvm(monkeypatch):
    """--jvm-arg: a host that needs a JVM option no longer has to smuggle it in through
    JAVA_TOOL_OPTIONS."""
    import pgwire_calcite.calcite_backend as cb

    seen: dict = {}

    class _Recorder:
        def __init__(self, **kwargs):
            seen.update(kwargs)

    monkeypatch.setattr(cb, "CalciteBackend", _Recorder)
    launcher.build_backend("calcite", "m.json", jvm_args=["-Xmx2g", "-Dduckdb.x=/tmp/y"])
    assert seen["jvm_args"] == ["-Xmx2g", "-Dduckdb.x=/tmp/y"]


def test_idle_shutdown_can_be_set_by_the_launcher(monkeypatch):
    import pgwire_calcite.server as server_mod

    seen: list = []
    monkeypatch.setattr(
        server_mod,
        "maybe_start_idle_shutdown_watcher",
        lambda server, seconds=None: seen.append(seconds),
    )
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, idle_shutdown_seconds=120.0)
    try:
        assert seen == [120.0]
    finally:
        srv.shutdown()
        srv.server_close()


# --- shutdown -------------------------------------------------------------------


def test_close_server_ends_idle_sessions_instead_of_running_out_the_budget():
    """An idle client parks its request thread in a read with no timeout. Shutdown used
    to wait the whole budget on it and then force the exit; it now ends the session."""
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, backend=StubBackend())
    client = MiniPgClient("127.0.0.1", port)
    assert client.query("SELECT 1")["rows"] == [["1"]]
    forced: list = []

    started = time.monotonic()
    launcher.close_server(srv, budget=5.0, exit_fn=forced.append)

    assert forced == []
    assert time.monotonic() - started < 3
    client.sock.settimeout(5)
    assert client.sock.recv(16) == b""  # the server closed the session
    client.sock.close()


def test_sigterm_with_a_client_connected_exits_cleanly_and_releases_the_port(tmp_path):
    """End to end, in a process of its own: SIGTERM while a client sits connected must
    exit 0 promptly, release the port and remove the pid file it wrote."""
    port = _free_port()
    pid_file = tmp_path / "pgwire.pid"
    proc = subprocess.Popen(
        [
            sys.executable, "-m", "pgwire_calcite.launcher",
            "--port", str(port), "--backend", "stub", "--pid-file", str(pid_file),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_for_port(port)
        client = None
        deadline = time.monotonic() + 30
        while client is None:
            try:
                client = MiniPgClient("127.0.0.1", port)
            except ConnectionError:  # still starting (57P03)
                assert time.monotonic() < deadline
                time.sleep(0.05)
        assert client.query("SELECT 1")["rows"] == [["1"]]
        assert int(pid_file.read_text()) == proc.pid

        started = time.monotonic()
        proc.send_signal(signal.SIGTERM)
        code = proc.wait(timeout=20)

        assert code == 0
        assert time.monotonic() - started < 5
        assert not pid_file.exists()
        # The port is free again.
        probe = socket.socket()
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind(("127.0.0.1", port))
        probe.close()
        client.sock.close()
    finally:
        if proc.poll() is None:
            proc.kill()  # the process this test started, by its own handle
            proc.wait(timeout=10)


def test_a_failed_startup_exits_with_status_1_and_frees_the_port():
    """End to end, in a process of its own: a backend that cannot be built (here, a
    classpath naming a jar that does not exist) ends the process and releases the port."""
    import os

    port = _free_port()
    env = dict(os.environ, PGWIRE_CALCITE_CLASSPATH="/nonexistent/calcite.jar")
    proc = subprocess.Popen(
        [
            sys.executable, "-m", "pgwire_calcite.launcher",
            "--port", str(port), "--backend", "calcite", "--model", "missing.json",
        ],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        _, stderr = proc.communicate(timeout=60)
    finally:
        if proc.poll() is None:
            proc.kill()  # the process this test started, by its own handle
            proc.wait(timeout=10)
    assert proc.returncode == launcher.EXIT_STARTUP_FAILED
    assert "startup failed while building the backend" in stderr
    assert "ClasspathError" in stderr  # the cause is logged, not just the exit
    probe = socket.socket()
    probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    probe.bind(("127.0.0.1", port))
    probe.close()


@pytest.mark.parametrize(
    "flags, expected",
    [
        ([], "served"),  # the default: a connection made during start-up waits and is served
        (["--reject-while-starting"], "57P03"),
    ],
)
def test_what_a_client_sees_when_it_connects_while_the_backend_is_built(
    monkeypatch, flags, expected
):
    """Through the launcher's own main(): the backend build is slow, and a client connects
    in the middle of it. By default the connection is held until the server can answer (a
    client that connects once and waits depends on that); with --reject-while-starting it
    is told at once that the server is starting."""
    import threading

    port = _free_port()
    seen: dict = {}

    def _client() -> None:
        try:
            client = MiniPgClient("127.0.0.1", port)
            seen["outcome"] = "served" if client.query("SELECT 1")["rows"] == [["1"]] else "?"
            seen["served_at"] = time.monotonic()
            client.sock.close()
        except ConnectionError as exc:
            seen["outcome"] = "57P03" if "57P03" in str(exc) else repr(exc)
            seen["refused_at"] = time.monotonic()

    built: dict = {}

    def _slow_build(*args, **kwargs):
        worker = threading.Thread(target=_client)
        worker.start()
        built["worker"] = worker
        time.sleep(0.8)  # the client is connected (or answered) well inside this window
        built["at"] = time.monotonic()
        return StubBackend()

    def _stop_once_the_client_is_done(stop) -> None:
        built["worker"].join(20)
        stop.set()

    monkeypatch.setattr(launcher, "build_backend", _slow_build)
    monkeypatch.setattr(launcher, "install_shutdown_handler", _stop_once_the_client_is_done)

    assert launcher.main(["--port", str(port), "--backend", "stub", *flags]) == 0

    assert seen["outcome"] == expected
    if expected == "served":
        assert seen["served_at"] > built["at"]  # answered only once the backend existed
    else:
        assert seen["refused_at"] < built["at"]  # answered while it was still being built
