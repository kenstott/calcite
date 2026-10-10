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
  - two starts at the same moment, both;
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


def main() -> int:
    launcher = sys.argv[1]
    command = [launcher, "--mcp"]
    failures = []

    results = [None, None]

    def start(i):
        results[i] = handshake(command)

    threads = [threading.Thread(target=start, args=(i,)) for i in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    for i, (answered, seen) in enumerate(results):
        report(f"two at once, start {i + 1}", answered, seen)
        if not answered:
            failures.append(f"two at once: start {i + 1} did not answer")

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

    for failure in failures:
        print(f"::error::{failure}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
