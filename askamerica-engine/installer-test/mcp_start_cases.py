#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE-BSL.txt file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""Start an installed AskAmerica MCP launcher the other ways a host starts it, beyond the one
plain start mcp_handshake.py makes. Claude Desktop starts the server once for its own chat and
again, from a pool, for Cowork and Code sessions.

    mcp_start_cases.py "<path to the installed launcher>"

Cases that must complete the MCP `initialize` exchange:
  - two starts at the same moment, both, with the engine jar already on the machine;
  - two starts at the same moment, both, with NO engine jar on the machine yet: each has to
    fetch it, one waits on the other's download. The jar is served to them from this machine
    (ASKAMERICA_ENGINE_URL), so this shows that both come up, not how long a real 415 MB
    download keeps the second one waiting;
  - a start from another working directory;
  - on Windows, a start through cmd.exe with the launcher's path quoted.
Cases that must complete it OR fail with a message of the launcher's own:
  - a start with a bare environment;
  - a start whose profile variables point at an empty directory.
Shown, never failing the run: on Windows, a start through cmd.exe with the path NOT quoted,
which is what a spawner that joins a command and its arguments into one shell line produces.
The launcher's path has spaces; what Windows prints for it is here to be compared with a log.

Exits 0 when every required case holds, 1 otherwise.
"""
import functools
import http.server
import os
import sys
import tempfile
import threading

from mcp_handshake import handshake

# What the launcher itself prints when it cannot start the server.
OWN_ERRORS = ("[askamerica-launcher]", "[askamerica-mcp]", "Could not obtain the AskAmerica engine")
# The engine jar this run built; kept in every environment, standing for the jar a user's
# machine already has cached.
KEPT = ("ASKAMERICA_ENGINE_JAR",)


def bare_environment():
    names = ("SystemRoot", "PATH") if os.name == "nt" else ("PATH",)
    return {n: os.environ[n] for n in names + KEPT if n in os.environ}


def empty_profile_environment(empty):
    env = dict(os.environ)
    names = (("USERPROFILE", "LOCALAPPDATA", "APPDATA", "HOME") if os.name == "nt"
             else ("HOME", "XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_CACHE_HOME"))
    for n in names:
        env[n] = empty
    if os.name == "nt":
        drive, path = os.path.splitdrive(empty)
        env["HOMEDRIVE"], env["HOMEPATH"] = drive, path
    return env


def report(title, answered, seen):
    print(f"--- {title}: {'answered initialize' if answered else 'did NOT answer initialize'}")
    for line in seen[-25:]:
        print("    " + line[:300])
    sys.stdout.flush()


def named_error(seen):
    return any(marker in line for line in seen for marker in OWN_ERRORS)


def two_at_once(command, title, failures, env=None):
    results = [None, None]

    def start(i):
        results[i] = handshake(command, env=env)

    threads = [threading.Thread(target=start, args=(i,)) for i in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    for i, (answered, seen) in enumerate(results):
        report(f"{title}, start {i + 1}", answered, seen)
        if not answered:
            failures.append(f"{title}: start {i + 1} did not answer")


def two_first_starts(command, failures):
    """Two starts at once on a machine with no engine jar: both fetch it, from this machine."""
    title = "two at once, no engine jar on the machine"
    built = os.environ.get("ASKAMERICA_ENGINE_JAR")
    cached = os.path.join(os.path.expanduser("~"), ".askamerica", "engine", "askamerica-engine.jar")
    if not built or not os.path.isfile(built):
        failures.append(f"{title}: ASKAMERICA_ENGINE_JAR does not name the jar to serve")
        return
    if os.path.exists(cached):
        failures.append(f"{title}: a jar is already cached at {cached}; the case cannot run")
        return
    handler = functools.partial(
        http.server.SimpleHTTPRequestHandler, directory=os.path.dirname(os.path.abspath(built)))
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        env = {k: v for k, v in os.environ.items() if k != "ASKAMERICA_ENGINE_JAR"}
        env["ASKAMERICA_ENGINE_URL"] = (
            f"http://127.0.0.1:{server.server_address[1]}/{os.path.basename(built)}")
        two_at_once(command, title, failures, env=env)
    finally:
        server.shutdown()


def main() -> int:
    launcher = sys.argv[1]
    command = [launcher, "--mcp"]
    failures = []

    two_at_once(command, "two at once", failures)

    with tempfile.TemporaryDirectory() as elsewhere:
        answered, seen = handshake(command, cwd=elsewhere)
        report(f"from another working directory ({elsewhere})", answered, seen)
        if not answered:
            failures.append("from another working directory: did not answer")

    answered, seen = handshake(command, env=bare_environment())
    report("bare environment (" + ", ".join(sorted(bare_environment())) + ")", answered, seen)
    if not answered and not named_error(seen):
        failures.append("bare environment: neither an answer nor an error of the launcher's own")

    with tempfile.TemporaryDirectory() as empty:
        answered, seen = handshake(command, env=empty_profile_environment(empty))
        report(f"profile variables pointing at an empty directory ({empty})", answered, seen)
        if not answered and not named_error(seen):
            failures.append(
                "empty profile: neither an answer nor an error of the launcher's own")

    if os.name == "nt":
        answered, seen = handshake(f'"{launcher}" --mcp', shell=True)
        report("through cmd.exe, path quoted", answered, seen)
        if not answered:
            failures.append("through cmd.exe with the path quoted: did not answer")
        answered, seen = handshake(f"{launcher} --mcp", shell=True)
        report("through cmd.exe, path NOT quoted (shown only)", answered, seen)

    # Last: it leaves a jar in the cache, which the cases above must not find.
    two_first_starts(command, failures)

    for failure in failures:
        print(f"::error::{failure}")
    return 1 if failures else 0


if __name__ == "__main__":
    # What the server wrote is printed as it came; on Windows the console encoding cannot
    # hold every character of it, and a print must not be what fails the check.
    for stream in (sys.stdout, sys.stderr):
        stream.reconfigure(encoding="utf-8", errors="replace")
    sys.exit(main())
