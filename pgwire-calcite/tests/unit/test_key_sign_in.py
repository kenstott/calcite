# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE-BSL.txt file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""Key sign-in (pgwire-govdata): the password of every connection is its AskAmerica API key.

The server refuses a connection with no key, an empty one, or one the key service refuses;
refuses, with another code, when the key service cannot be asked; and checks quota and
reports usage under the key of the connection a statement ran on -- two connections with two
keys are metered separately. No key appears in an error or a log line.

The key service is a stand-in on loopback; the keys are made up.
"""

from __future__ import annotations

import http.server
import json
import logging
import threading
import time

import pytest

from pgwire_calcite import launcher, metering
from pgwire_calcite.auth import AskAmericaKeyProvider
from pgwire_calcite.throttle import reset_login_throttle

from test_phase0_wire import MiniPgClient, _free_port

GOOD_A = "ask_test_key_alpha_0123456789"
GOOD_B = "ask_test_key_bravo_9876543210"
SPENT = "ask_test_key_spent_5555555555"
REFUSED = "ask_test_key_refused_0000000000"
ALL_KEYS = (GOOD_A, GOOD_B, SPENT, REFUSED)


class _KeyService(http.server.BaseHTTPRequestHandler):
    """/v1/quota and /v1/metering/usage as the server calls them."""

    quota_calls: list = []
    usage: list = []

    def log_message(self, *args):  # quiet
        pass

    def do_GET(self):
        key = self.headers.get("X-API-Key")
        type(self).quota_calls.append(key)
        if key in (GOOD_A, GOOD_B):
            self._answer(200, {"remaining_bytes": 1000})
        elif key == SPENT:
            self._answer(200, {"remaining_bytes": 0})
        else:
            self._answer(401, {"error": "refused"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(length) or b"{}")
        type(self).usage.append((self.headers.get("X-API-Key"), body))
        self._answer(200, {})

    def _answer(self, status, body):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


@pytest.fixture()
def key_service(monkeypatch):
    _KeyService.quota_calls = []
    _KeyService.usage = []
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _KeyService)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    monkeypatch.setenv("ASKAMERICA_API_URL", f"http://127.0.0.1:{httpd.server_address[1]}")
    # The starter's key in the server's environment is never anyone's metering identity.
    monkeypatch.setenv("ASKAMERICA_API_KEY", "ask_test_key_of_the_starter")
    metering._quota_cache.clear()
    reset_login_throttle()
    yield _KeyService
    httpd.shutdown()
    metering._quota_cache.clear()
    reset_login_throttle()


@pytest.fixture()
def server(key_service):
    port = _free_port()
    srv = launcher.serve(
        host="127.0.0.1", port=port, auth="none", auth_provider=AskAmericaKeyProvider())
    time.sleep(0.1)
    yield port
    srv.shutdown()


def _refused(port, password):
    with pytest.raises(ConnectionError) as exc:
        MiniPgClient("127.0.0.1", port, user="askamerica", password=password)
    return str(exc.value)


def test_a_connection_with_no_key_or_an_empty_one_is_refused(server, key_service):
    assert "28P01" in _refused(server, None)
    assert "28P01" in _refused(server, "")
    assert key_service.quota_calls == [], "an absent key is refused without asking the service"


def test_a_key_the_service_refuses_is_refused_by_name(server):
    message = _refused(server, REFUSED)
    assert "28P01" in message
    assert REFUSED not in message


def test_an_accepted_key_signs_in_and_a_spent_key_is_admitted_but_cannot_query(server):
    c = MiniPgClient("127.0.0.1", server, user="askamerica", password=GOOD_A)
    assert c.query("SELECT 1")["rows"]
    c.close()

    spent = MiniPgClient("127.0.0.1", server, user="askamerica", password=SPENT)
    result = spent.query("SELECT 1")
    assert "53400" in json.dumps(result), result
    assert SPENT not in json.dumps(result)
    spent.close()


def test_sign_in_fails_closed_when_the_key_service_cannot_be_asked(server, monkeypatch):
    monkeypatch.setenv("ASKAMERICA_API_URL", f"http://127.0.0.1:{_free_port()}")
    message = _refused(server, GOOD_A)
    assert "08006" in message
    assert GOOD_A not in message


def test_two_connections_with_two_keys_are_metered_separately(server, key_service):
    a = MiniPgClient("127.0.0.1", server, user="askamerica", password=GOOD_A)
    b = MiniPgClient("127.0.0.1", server, user="askamerica", password=GOOD_B)
    assert a.query("SELECT 1")["rows"]
    assert b.query("SELECT 1")["rows"]
    assert b.query("SELECT 1")["rows"]
    a.close()
    b.close()
    deadline = time.time() + 5
    while time.time() < deadline and len(key_service.usage) < 3:
        time.sleep(0.05)
    by_key = [key for key, _ in key_service.usage]
    assert sorted(by_key) == sorted([GOOD_A, GOOD_B, GOOD_B]), by_key
    assert "ask_test_key_of_the_starter" not in by_key
    assert "ask_test_key_of_the_starter" not in key_service.quota_calls


def test_a_wrong_key_locks_out_only_itself(server):
    from pgwire_calcite.throttle import login_throttle

    for _ in range(login_throttle().max_attempts + 1):
        _refused(server, REFUSED)
    c = MiniPgClient("127.0.0.1", server, user="askamerica", password=GOOD_A)
    c.close()


def test_no_key_appears_in_any_log_line(server, caplog):
    caplog.set_level(logging.DEBUG)
    _refused(server, REFUSED)
    _refused(server, None)
    c = MiniPgClient("127.0.0.1", server, user="askamerica", password=GOOD_A)
    c.query("SELECT 1")
    c.close()
    spent = MiniPgClient("127.0.0.1", server, user="askamerica", password=SPENT)
    spent.query("SELECT 1")
    spent.close()
    time.sleep(0.3)
    logged = "\n".join(r.getMessage() + " " + repr(r.args) for r in caplog.records)
    for key in ALL_KEYS:
        assert key not in logged
        assert key[:16] not in logged


def test_a_connection_with_no_key_is_not_metered(key_service):
    """Other adapters, and a server started with key sign-in switched off for a test."""
    metering.enforce_quota(None)
    sampler = metering.EgressSampler(1)
    sampler.observe(["x"])
    sampler.finish(None, "SELECT 1", 1)
    time.sleep(0.2)
    assert key_service.quota_calls == []
    assert key_service.usage == []
