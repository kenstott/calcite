#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE-BSL.txt file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""A data query, when the data server cannot be started, must be answered with an error that
names the cause, within seconds. Claude Desktop cancels a tool call after four minutes; a
server that waits longer than that tells the user nothing (issue 484).

    mcp_unreachable_query.py "<path to the installed launcher>"

The installed launcher is started with ASKAMERICA_PGWIRE_LAUNCHER naming a stand-in for the
pgwire-govdata server that exits at once, on a port nothing listens on. It is sent
`initialize` and then one `query` tool call. Exits 0 when that call is answered within
LIMIT_SECONDS with an error naming pgwire-govdata, 1 otherwise. Reads no data, needs no key.
"""
import json
import os
import queue
import socket
import subprocess
import sys
import tempfile
import threading
import time

LIMIT_SECONDS = 60
STARTUP_SECONDS = 600  # a first start may fetch the engine jar before it answers


def stand_in_server(root):
    """A bundle directory whose launcher exits at once, laid out as the engine expects."""
    os.makedirs(os.path.join(root, "bin"))
    if os.name == "nt":
        launcher = os.path.join(root, "bin", "pgwire-govdata.bat")
        with open(launcher, "w") as f:
            f.write("@echo stand-in pgwire-govdata: exiting at once\r\n@exit /b 1\r\n")
    else:
        launcher = os.path.join(root, "bin", "pgwire-govdata")
        with open(launcher, "w") as f:
            f.write("#!/bin/sh\necho 'stand-in pgwire-govdata: exiting at once'\nexit 1\n")
        os.chmod(launcher, 0o755)
        # The engine starts the launcher through the bundle's own Python.
        os.makedirs(os.path.join(root, "cpython", "bin"))
        os.symlink(sys.executable, os.path.join(root, "cpython", "bin", "python"))
    return launcher


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def main() -> int:
    launcher = sys.argv[1]
    with tempfile.TemporaryDirectory() as root:
        env = dict(os.environ)
        env["ASKAMERICA_PGWIRE_LAUNCHER"] = stand_in_server(root)
        env["ASKAMERICA_PGWIRE_PORT"] = str(free_port())
        proc = subprocess.Popen(
            [launcher, "--mcp"], env=env,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        lines: "queue.Queue[tuple[str, bytes]]" = queue.Queue()

        def pump(name, stream):
            for raw in iter(stream.readline, b""):
                lines.put((name, raw))

        for name, stream in (("out", proc.stdout), ("err", proc.stderr)):
            threading.Thread(target=pump, args=(name, stream), daemon=True).start()

        seen = []

        def send(message):
            proc.stdin.write((json.dumps(message) + "\n").encode("utf-8"))
            proc.stdin.flush()

        def answer(request_id, within):
            deadline = time.monotonic() + within
            while time.monotonic() < deadline:
                try:
                    name, raw = lines.get(timeout=max(0.1, deadline - time.monotonic()))
                except queue.Empty:
                    break
                text = raw.decode("utf-8", "replace").rstrip()
                seen.append(f"[{name}] {text}")
                if name != "out":
                    continue
                try:
                    message = json.loads(text)
                except ValueError:
                    continue
                if isinstance(message, dict) and message.get("id") == request_id:
                    return message
            return None

        try:
            send({"jsonrpc": "2.0", "id": 1, "method": "initialize",
                  "params": {"protocolVersion": "2024-11-05", "capabilities": {},
                             "clientInfo": {"name": "installer-test", "version": "0"}}})
            if answer(1, STARTUP_SECONDS) is None:
                print("::error::the server did not answer initialize")
                return report(seen, 1)
            send({"jsonrpc": "2.0", "method": "notifications/initialized"})
            started = time.monotonic()
            send({"jsonrpc": "2.0", "id": 2, "method": "tools/call",
                  "params": {"name": "query",
                             "arguments": {"sql": "SELECT 1 FROM sec.filing_metadata"}}})
            reply = answer(2, LIMIT_SECONDS)
            waited = time.monotonic() - started
        finally:
            proc.kill()

    if reply is None:
        print(f"::error::a query with the data server unreachable was not answered within "
              f"{LIMIT_SECONDS}s; the user would be told nothing")
        return report(seen, 1)
    text = json.dumps(reply)
    print(f"answered after {waited:.1f}s: {text[:1200]}")
    is_error = "error" in reply or reply.get("result", {}).get("isError") is True
    if not is_error:
        print("::error::the query was answered without an error although no data server runs")
        return report(seen, 1)
    if "pgwire-govdata" not in text:
        print("::error::the error does not name the data server (pgwire-govdata)")
        return report(seen, 1)
    return 0


def report(seen, code):
    print("what the server wrote:")
    for line in seen[-60:]:
        print("  " + line[:300])
    return code


if __name__ == "__main__":
    sys.exit(main())
