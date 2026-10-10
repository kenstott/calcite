#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE-BSL.txt file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""The data server the engine spawns must come up and stay up.

    mcp_server_reachable.py "<installed launcher>" "<pgwire-govdata launcher>" <instances>

Starts <instances> AskAmerica MCP processes at the same moment, each told to use the given
pgwire-govdata launcher (ASKAMERICA_PGWIRE_LAUNCHER) on one free port. Every MCP process
spawns the server at its own start; by design one server binds the port and the others'
exit. Exits 0 when the port accepts a connection within OPEN_SECONDS and still does
HOLD_SECONDS later, 1 otherwise, printing what the MCP processes said about the spawn and
the end of the server's own output.

Reads no data and needs no key: the server is given placeholders for the store, and no query
is sent. What this shows is that the server starts where it was unpacked and listens; that it
answers queries needs the store.
"""
import json
import os
import socket
import subprocess
import sys
import threading
import time

OPEN_SECONDS = 300
MIN_CLASSPATH_ENTRIES = 200
HOLD_SECONDS = 30
PLACEHOLDERS = {
    "GOVDATA_PARQUET_DIR": "s3://askamerica-installer-test/placeholder",
    "AWS_ACCESS_KEY_ID": "placeholder",
    "AWS_SECRET_ACCESS_KEY": "placeholder",
    "AWS_ENDPOINT_OVERRIDE": "https://placeholder.invalid",
}


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def port_open(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=2):
            return True
    except OSError:
        return False


def main() -> int:
    launcher, server_launcher, instances = sys.argv[1], sys.argv[2], int(sys.argv[3])
    if not os.path.isfile(server_launcher):
        print(f"::error::the pgwire-govdata launcher is not at {server_launcher}")
        return 1
    port = free_port()
    env = dict(os.environ)
    env.update(PLACEHOLDERS)
    env["ASKAMERICA_PGWIRE_LAUNCHER"] = server_launcher
    env["ASKAMERICA_PGWIRE_PORT"] = str(port)
    spawn_log = os.path.join(os.path.expanduser("~"), ".askamerica", "pgwire-govdata", "spawn.log")
    log_start = os.path.getsize(spawn_log) if os.path.exists(spawn_log) else 0

    said = []
    procs = []

    def pump(i, stream, keep):
        for raw in iter(stream.readline, b""):
            text = raw.decode("utf-8", "replace").rstrip()
            if keep and "pgwire" in text:
                said.append(f"[mcp {i}] {text}")

    initialize = (json.dumps({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {"protocolVersion": "2024-11-05", "capabilities": {},
                   "clientInfo": {"name": "installer-test", "version": "0"}}}) + "\n").encode()
    for i in range(1, instances + 1):
        p = subprocess.Popen([launcher, "--mcp"], env=env, stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        procs.append(p)
        threading.Thread(target=pump, args=(i, p.stdout, False), daemon=True).start()
        threading.Thread(target=pump, args=(i, p.stderr, True), daemon=True).start()
        p.stdin.write(initialize)
        p.stdin.flush()

    opened_after = None
    held = False
    try:
        started = time.monotonic()
        while time.monotonic() - started < OPEN_SECONDS:
            if port_open(port):
                opened_after = time.monotonic() - started
                break
            time.sleep(1)
        if opened_after is not None:
            time.sleep(HOLD_SECONDS)
            held = port_open(port)
    finally:
        for p in procs:
            p.kill()

    print(f"{instances} MCP process(es), data server on 127.0.0.1:{port}")
    print("what the MCP processes said about the server:")
    for line in said[-40:]:
        print("  " + line[:400])
    print(f"the end of the server's own output ({spawn_log}):")
    if os.path.exists(spawn_log):
        with open(spawn_log, "rb") as f:
            f.seek(log_start)
            for line in f.read().decode("utf-8", "replace").splitlines()[-60:]:
                print("  " + line[:400])
    else:
        print("  (no such file)")
    if opened_after is None:
        print(f"::error::with {instances} MCP process(es) the data server never listened on its "
              f"port within {OPEN_SECONDS}s")
        return 1
    print(f"the port opened after {opened_after:.0f}s")
    # The server says how many jars its JVM was given. The Windows launcher of releases up to
    # 0.109.2 built its classpath in one cmd.exe variable, which stops at 8,191 characters:
    # 63 of about 260 jars, and no Calcite (issue to be linked). Too few is a failed start.
    entries = None
    if os.path.exists(spawn_log):
        with open(spawn_log, "rb") as f:
            f.seek(log_start)
            for line in f.read().decode("utf-8", "replace").splitlines():
                if "JVM started with" in line and "classpath entries" in line:
                    entries = int(line.split("JVM started with")[1].split()[0])
    print(f"classpath entries the server's JVM started with: {entries}")
    if entries is not None and entries < MIN_CLASSPATH_ENTRIES:
        print(f"::error::with {instances} MCP process(es) the data server's JVM was given "
              f"{entries} jars; a whole bundle has more than {MIN_CLASSPATH_ENTRIES}")
        return 1
    if not held:
        print(f"::error::with {instances} MCP process(es) the data server opened its port and "
              f"was gone {HOLD_SECONDS}s later")
        return 1
    print(f"and was still open {HOLD_SECONDS}s later")
    return 0


if __name__ == "__main__":
    # What the server wrote is printed as it came; on Windows the console encoding cannot
    # hold every character of it, and a print must not be what fails the check.
    for stream in (sys.stdout, sys.stderr):
        stream.reconfigure(encoding="utf-8", errors="replace")
    sys.exit(main())
