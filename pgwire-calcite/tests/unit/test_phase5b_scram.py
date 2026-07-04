# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Phase 5b tests: SASL SCRAM-SHA-256 wire exchange (RFC 5802 / RFC 7677, PGW-043).

The exchange logic is proven with a self-contained SCRAM client (no wire), and
the full handshake is exercised against a real SCRAM client (psql) which
negotiates SCRAM automatically when the server offers it.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import subprocess
import time

import pytest

from pgwire_calcite import launcher
from pgwire_calcite.auth import AccountStore, LocalAccountsProvider, ScramVerifier
from pgwire_calcite.scram import ScramServerExchange

from test_phase0_wire import _free_port


def _client_proof(password, salt, iterations, auth_message):
    salted = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, iterations)
    client_key = hmac.new(salted, b"Client Key", "sha256").digest()
    stored_key = hashlib.sha256(client_key).digest()
    client_sig = hmac.new(stored_key, auth_message.encode(), "sha256").digest()
    return bytes(a ^ b for a, b in zip(client_key, client_sig))


def _run_client(exchange, password, user="alice", client_nonce="clientNONCE1234"):
    client_first_bare = f"n={user},r={client_nonce}"
    server_first = exchange.server_first("n,," + client_first_bare)
    attrs = dict(a.split("=", 1) for a in server_first.split(","))
    salt = base64.b64decode(attrs["s"])
    iterations = int(attrs["i"])
    nonce = attrs["r"]
    without_proof = f"c=biws,r={nonce}"
    auth_message = f"{client_first_bare},{server_first},{without_proof}"
    proof = _client_proof(password, salt, iterations, auth_message)
    client_final = without_proof + ",p=" + base64.b64encode(proof).decode()
    return exchange.verify_final(client_final), auth_message


def test_scram_exchange_accepts_correct_password():
    v = ScramVerifier.create("hunter2")
    ex = ScramServerExchange(v, server_nonce="serverNONCE5678")
    (ok, server_final), auth_message = _run_client(ex, "hunter2")
    assert ok
    # server_final carries the ServerSignature the client can verify
    expected = hmac.new(v.server_key, auth_message.encode(), "sha256").digest()
    assert server_final == "v=" + base64.b64encode(expected).decode()


def test_scram_exchange_rejects_wrong_password():
    v = ScramVerifier.create("hunter2")
    ex = ScramServerExchange(v, server_nonce="serverNONCE5678")
    (ok, server_final), _ = _run_client(ex, "WRONG")
    assert ok is False and server_final is None


@pytest.mark.skipif(not os.path.exists("/usr/bin/psql"), reason="psql not available")
def test_scram_end_to_end_with_psql(tmp_path):
    store = AccountStore(tmp_path / "accounts.json")
    store.add("alice", "s3cret")
    provider = LocalAccountsProvider(store, scram_wire=True)
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", auth_provider=provider)
    time.sleep(0.2)
    try:
        env = dict(os.environ, PGPASSWORD="s3cret")
        out = subprocess.run(
            ["/usr/bin/psql", f"host=127.0.0.1 port={port} user=alice dbname=calcite",
             "-tAc", "SELECT 1"],
            capture_output=True, text=True, env=env, timeout=20,
        )
        assert out.returncode == 0, out.stderr
        assert out.stdout.strip() == "1", out.stdout + out.stderr
        # wrong password -> SCRAM fails
        bad = subprocess.run(
            ["/usr/bin/psql", f"host=127.0.0.1 port={port} user=alice dbname=calcite",
             "-tAc", "SELECT 1"],
            capture_output=True, text=True, env=dict(os.environ, PGPASSWORD="wrong"), timeout=20,
        )
        assert bad.returncode != 0
    finally:
        srv.shutdown()
