# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Phase 5b tests: per-role authorization (PGW-045).

A restricted role sees ONLY its granted tables in discovery, and is DENIED on
direct access to a non-granted table — enforced on the same grants in both
paths. Uses the in-process Calcite backend + catalog.
"""

from __future__ import annotations

import time

import pytest

from pgwire_calcite import launcher
from pgwire_calcite.authz import RoleGrants, referenced_tables

from test_phase0_wire import MiniPgClient, _free_port


def test_grants_matching():
    g = RoleGrants.from_dict({"analyst": {"SALES.DEPTS"}, "admin": {"*"}})
    assert g.allows("admin", "SALES", "EMPS")
    assert g.allows("analyst", "SALES", "DEPTS")
    assert g.allows("analyst", "", "depts")  # unqualified, case-insensitive
    assert not g.allows("analyst", "SALES", "EMPS")
    assert not g.allows("nobody", "SALES", "DEPTS")  # unknown role -> deny


def test_referenced_tables():
    refs = referenced_tables("SELECT * FROM sales.emps e JOIN depts d ON e.deptno=d.deptno")
    names = {t for _, t in refs}
    assert "emps" in names and "depts" in names


@pytest.fixture()
def authz_server(calcite_backend):
    grants = RoleGrants.from_dict({"analyst": {"SALES.DEPTS"}, "admin": {"*"}})
    port = _free_port()
    srv = launcher.serve(
        host="127.0.0.1", port=port, auth="none", backend=calcite_backend, authz_grants=grants
    )
    time.sleep(0.1)
    yield "127.0.0.1", port
    srv.shutdown()


def test_restricted_role_discovers_only_granted_tables(authz_server):
    host, port = authz_server
    c = MiniPgClient(host, port, user="analyst")
    try:
        r = c.query(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema NOT IN ('pg_catalog','information_schema') ORDER BY table_name"
        )
        assert r["error"] is None, r["error"]
        assert [row[0] for row in r["rows"]] == ["depts"]  # emps hidden
    finally:
        c.close()


def test_admin_role_discovers_all(authz_server):
    host, port = authz_server
    c = MiniPgClient(host, port, user="admin")
    try:
        r = c.query(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema NOT IN ('pg_catalog','information_schema') ORDER BY table_name"
        )
        assert [row[0] for row in r["rows"]] == ["depts", "emps"]
    finally:
        c.close()


def test_restricted_role_denied_on_ungranted_query(authz_server):
    host, port = authz_server
    c = MiniPgClient(host, port, user="analyst")
    try:
        denied = c.query("SELECT count(*) FROM EMPS")
        assert denied["error"] is not None and "42501" in denied["error"]
        allowed = c.query("SELECT count(*) AS n FROM DEPTS")
        assert allowed["error"] is None, allowed["error"]
        assert allowed["rows"] == [["4"]]
    finally:
        c.close()
