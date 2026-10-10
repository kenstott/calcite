#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE-BSL.txt file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""Start an installed AskAmerica MCP launcher the way Claude Desktop does, and complete the
MCP `initialize` exchange over its standard input and output.

    mcp_handshake.py "<path to the installed launcher>"

Exits 0 when the server answers `initialize` with its serverInfo, in UTF-8, 1 otherwise
(printing what it wrote). The server is given no API key and reads no data: `initialize` needs neither.
"""
import json
import os
import queue
import subprocess
import sys
import threading

TIMEOUT_SECONDS = 600  # a first start may fetch the engine jar before it answers
# Marks a line of the server's standard output that is not UTF-8. MCP is UTF-8; a server that
# writes in the system's code page corrupts every non-ASCII character it sends (issue 486).
NOT_UTF8 = "[test] standard output is not valid UTF-8: "


def handshake(command, cwd=None, env=None, shell=False):
    """Starts `command`, sends `initialize`, and returns (answered, lines it wrote)."""
    proc = subprocess.Popen(
        command, cwd=cwd, env=env, shell=shell,
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    lines: "queue.Queue[tuple[str, bytes]]" = queue.Queue()

    def pump(name, stream):
        for raw in iter(stream.readline, b""):
            lines.put((name, raw))
        lines.put((name, b""))

    for name, stream in (("out", proc.stdout), ("err", proc.stderr)):
        threading.Thread(target=pump, args=(name, stream), daemon=True).start()

    request = {
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {"protocolVersion": "2024-11-05", "capabilities": {},
                   "clientInfo": {"name": "installer-test", "version": "0"}},
    }
    seen = []
    closed = 0
    try:
        try:
            proc.stdin.write((json.dumps(request) + "\n").encode("utf-8"))
            proc.stdin.flush()
        except OSError as e:
            seen.append(f"[test] the process closed its input before reading: {e}")
        while closed < 2:
            try:
                name, raw = lines.get(timeout=TIMEOUT_SECONDS)
            except queue.Empty:
                seen.append(f"[test] no answer to initialize within {TIMEOUT_SECONDS}s")
                break
            if raw == b"":
                closed += 1
                continue
            text = raw.decode("utf-8", "replace").rstrip()
            if name == "out":
                try:
                    raw.decode("utf-8")
                except UnicodeDecodeError as e:
                    seen.append(NOT_UTF8 + f"byte 0x{raw[e.start]:02x} at {e.start} in: "
                                + text[max(0, e.start - 40):e.start + 40])
            seen.append(f"[{name}] {text}")
            if name != "out":
                continue
            try:
                message = json.loads(text)
            except ValueError:
                continue
            if isinstance(message, dict) and message.get("id") == 1 and "result" in message:
                info = message["result"].get("serverInfo", {})
                seen.append("[test] initialize answered: " + json.dumps(message["result"])[:400])
                if info.get("name") == "AskAmerica":
                    return True, seen
                seen.append("[test] the answer names another server")
                break
    finally:
        if shell and os.name == "nt":
            # The shell's child is the server: end the whole tree.
            subprocess.run(["taskkill", "/T", "/F", "/PID", str(proc.pid)],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        proc.kill()
    return False, seen


def main() -> int:
    launcher = sys.argv[1]
    if not os.path.isfile(launcher):
        print(f"the launcher is not at {launcher}", file=sys.stderr)
        return 1
    answered, seen = handshake([launcher, "--mcp"])
    garbled = [line for line in seen if line.startswith(NOT_UTF8)]
    if answered and not garbled:
        print(seen[-1][len("[test] "):])
        return 0
    if answered:
        print("the server answered initialize, but not in UTF-8:", file=sys.stderr)
        for line in garbled:
            print("  " + line, file=sys.stderr)
        return 1
    print("the server did not complete the MCP handshake; what it wrote:", file=sys.stderr)
    for line in seen[-80:]:
        print("  " + line, file=sys.stderr)
    return 1


if __name__ == "__main__":
    # What the server wrote is printed as it came; on Windows the console encoding cannot
    # hold every character of it, and a print must not be what fails the check.
    for stream in (sys.stdout, sys.stderr):
        stream.reconfigure(encoding="utf-8", errors="replace")
    sys.exit(main())
