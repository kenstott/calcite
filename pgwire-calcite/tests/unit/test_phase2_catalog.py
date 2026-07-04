# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Phase 2 exit-gate tests: catalog/discovery from Calcite metadata.

Covers PGW-010/011/012/013/016 (discovery side): the pg_catalog/information_schema
intercept is populated from Calcite JDBC metadata, answers the DBeaver/DataGrip-
style introspection joins, invents no constraints, and stays consistent with the
query path (discover-then-query resolves). The user validates real DBeaver
manually; these replay the introspection SQL the intercept was built for.
"""

from __future__ import annotations

import time

import pytest

from pgwire_calcite import launcher

from test_phase0_wire import MiniPgClient, _free_port


@pytest.fixture(scope="module")
def catalog_server(calcite_backend):
    """Serve the shared Calcite backend; serve() populates the catalog intercept."""
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", backend=calcite_backend)
    import pgwire_calcite.server as server_mod

    assert server_mod.state.catalog_enabled, "catalog must be enabled after populate"
    time.sleep(0.1)
    yield "127.0.0.1", port
    srv.shutdown()


@pytest.fixture()
def client(catalog_server):
    host, port = catalog_server
    c = MiniPgClient(host, port)
    yield c
    c.close()


def test_information_schema_tables_from_calcite(client):
    r = client.query(
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE table_schema NOT IN ('pg_catalog', 'information_schema') "
        "ORDER BY table_name"
    )
    assert r["error"] is None, r["error"]
    assert r["rows"] == [["SALES", "depts"], ["SALES", "emps"]]


def test_information_schema_columns_types(client):
    r = client.query(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE lower(table_name) = 'emps' ORDER BY ordinal_position"
    )
    assert r["error"] is None, r["error"]
    assert r["rows"] == [
        ["empno", "integer"],
        ["ename", "character varying"],
        ["deptno", "integer"],
        ["sal", "double precision"],
    ]


def test_dbeaver_style_pg_attribute_join(client):
    r = client.query(
        "SELECT c.relname, a.attname, a.attnum "
        "FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid "
        "WHERE n.nspname = 'SALES' AND a.attnum > 0 "
        "ORDER BY c.relname, a.attnum"
    )
    assert r["error"] is None, r["error"]
    assert r["rows"] == [
        ["depts", "deptno", "1"],
        ["depts", "dname", "2"],
        ["emps", "empno", "1"],
        ["emps", "ename", "2"],
        ["emps", "deptno", "3"],
        ["emps", "sal", "4"],
    ]


def test_invents_no_constraints(client):
    """PGW-013: the file adapter declares no keys -> pg_constraint reports none,
    as a well-formed empty result (not an invented overlay)."""
    r = client.query(
        "SELECT c.conname FROM pg_catalog.pg_constraint c "
        "JOIN pg_catalog.pg_class r ON r.oid = c.conrelid "
        "WHERE lower(r.relname) IN ('emps', 'depts')"
    )
    assert r["error"] is None, r["error"]
    assert r["rows"] == []


def test_oids_are_self_consistent(client):
    """PGW-011: the same relation OID joins pg_class<->pg_attribute consistently."""
    r = client.query(
        "SELECT c.oid = a.attrelid FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid "
        "WHERE lower(c.relname) = 'emps' LIMIT 1"
    )
    assert r["error"] is None, r["error"]
    assert r["rows"] and r["rows"][0][0] in ("t", "true", "1")


def test_discover_then_query_consistency(client):
    """PGW-016: a name discovered via the catalog resolves in the query path."""
    disco = client.query(
        "SELECT table_name FROM information_schema.tables WHERE lower(table_name) = 'emps'"
    )
    assert disco["rows"], "emps must be discoverable"
    name = disco["rows"][0][0]
    q = client.query(f"SELECT count(*) AS n FROM {name}")
    assert q["error"] is None, q["error"]
    assert q["rows"] == [["10"]]


def test_phase2_corpus_replays_against_catalog(client):
    """PGW-041: the DBeaver/DataGrip introspection corpus answers via the catalog."""
    import importlib.util
    import pathlib
    import sys

    harness_path = pathlib.Path(__file__).resolve().parents[1] / "corpus" / "harness.py"
    spec = importlib.util.spec_from_file_location("corpus_harness_p2", harness_path)
    harness = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = harness
    spec.loader.exec_module(harness)

    entries = [e for e in harness.entries_for_phase(2) if e.min_phase == 2]
    assert entries, "expected phase-2 introspection corpus entries"
    for entry in entries:
        r = client.query(entry.sql)
        assert r["error"] is None, (entry.path.name, r["error"])
        assert r["columns"], (entry.path.name, "introspection probe returned no columns")
