# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Phase 5b tests: pluggable auth providers + local accounts (SCRAM-SHA-256).

Covers PGW-042 (provider interface) and PGW-043 (local accounts, SCRAM verifiers
at rest, verified on the wire) with the StubBackend so no JVM is needed. OIDC
(PGW-044) and per-role authz (PGW-045) are the remaining Phase-5b sub-pieces.
"""

from __future__ import annotations

import time

import pytest

from pgwire_calcite import launcher
from pgwire_calcite.auth import (
    AccountStore,
    LocalAccountsProvider,
    ScramVerifier,
    TrustProvider,
    build_provider,
)

from test_phase0_wire import MiniPgClient, _free_port


def test_scram_verifier_roundtrip_and_no_plaintext():
    v = ScramVerifier.create("hunter2")
    assert v.verify("hunter2")
    assert not v.verify("wrong")
    # what's stored contains no plaintext and re-loads faithfully
    j = v.to_json()
    assert "hunter2" not in str(j)
    assert j["mechanism"] == "SCRAM-SHA-256"
    v2 = ScramVerifier.from_json(j)
    assert v2.verify("hunter2") and not v2.verify("nope")


def test_account_store_persists(tmp_path):
    path = tmp_path / "accounts.json"
    store = AccountStore(path)
    store.add("alice", "s3cret")
    store.add("bob", "pw")
    assert store.list_users() == ["alice", "bob"]
    assert store.verify("alice", "s3cret") and not store.verify("alice", "x")
    # reload from disk
    store2 = AccountStore(path)
    assert store2.verify("bob", "pw")
    assert store2.remove("bob") and not store2.remove("bob")
    assert AccountStore(path).list_users() == ["alice"]


def test_trust_provider_requires_no_password():
    p = TrustProvider()
    assert p.requires_password is False
    assert p.authenticate("anyone", "") == "anyone"


def test_build_provider_local_requires_store():
    with pytest.raises(ValueError):
        build_provider("local", None)


@pytest.fixture()
def local_auth_server(tmp_path):
    store = AccountStore(tmp_path / "accounts.json")
    store.add("alice", "s3cret")
    provider = LocalAccountsProvider(store)
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", auth_provider=provider)
    time.sleep(0.1)
    yield "127.0.0.1", port
    srv.shutdown()


def test_local_accounts_accepts_correct_password(local_auth_server):
    host, port = local_auth_server
    c = MiniPgClient(host, port, user="alice", password="s3cret")
    try:
        assert c.query("SELECT 1")["rows"] == [["1"]]
    finally:
        c.close()


def test_local_accounts_rejects_wrong_password(local_auth_server):
    host, port = local_auth_server
    with pytest.raises(ConnectionError) as exc:
        MiniPgClient(host, port, user="alice", password="WRONG")
    assert "28P01" in str(exc.value) or "authentication failed" in str(exc.value)


def test_local_accounts_rejects_unknown_user(local_auth_server):
    host, port = local_auth_server
    with pytest.raises(ConnectionError):
        MiniPgClient(host, port, user="mallory", password="whatever")
