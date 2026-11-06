#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE-BSL.txt file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""A made-up API key for the installer tests, with nothing leaving the machine.

The engine does nothing for a caller with no API key. A test that needs the engine to go
further gives it a made-up key, a key service of its own on loopback, and a placeholder set
of object-store credentials in the engine's cache, so that neither the key nor a request for
credentials is ever sent to the real service.
"""
import http.server
import json
import os
import threading

MADE_UP_KEY = "ask_installer_test_key_0123456789"
PLACEHOLDER_CREDENTIALS = {
    "accessKeyId": "placeholder",
    "secretAccessKey": "placeholder",
    "endpoint": "https://placeholder.invalid",
    "region": "auto",
}


class _KeyService(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def _send(self, status, body):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.headers.get("X-API-Key") == MADE_UP_KEY:
            self._send(200, {"remaining_bytes": 1000000000})
        else:
            self._send(401, {"error": "refused"})

    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length", "0")))
        self._send(200, {})


def credentials_cache():
    return os.path.join(os.path.expanduser("~"), ".askamerica", "credentials.json")


def with_made_up_key(env):
    """Adds the made-up key and its loopback key service to `env`; returns the service."""
    service = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _KeyService)
    threading.Thread(target=service.serve_forever, daemon=True).start()
    env["ASKAMERICA_API_KEY"] = MADE_UP_KEY
    env.pop("FREE_ASKAMERICA_KEY", None)
    env["ASKAMERICA_API_URL"] = f"http://127.0.0.1:{service.server_address[1]}"
    cache = credentials_cache()
    os.makedirs(os.path.dirname(cache), exist_ok=True)
    with open(cache, "w", encoding="utf-8") as f:
        json.dump(PLACEHOLDER_CREDENTIALS, f)
    return service


def without_any_key(env):
    """Makes `env` a new user's: no key, no object-store settings, no cached credentials."""
    for name in list(env):
        if name in ("ASKAMERICA_API_KEY", "FREE_ASKAMERICA_KEY", "GOVDATA_PARQUET_DIR") \
                or name.startswith("AWS_"):
            del env[name]
    if os.path.exists(credentials_cache()):
        os.remove(credentials_cache())
