# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""INSERT / UPDATE / DELETE through the wire server.

A server routes writes only when started with ``--allow-writes``; the Calcite model then
decides per table (a table that is not modifiable is rejected by the validator). The engine
tests write to a DuckDB file through Calcite's JDBC adapter, whose tables are modifiable, and
show that the file adapter's are not.
"""

from __future__ import annotations

import json
import time

import psycopg
import pytest

from pgwire_calcite import launcher
from pgwire_calcite.classpath import ClasspathError, resolve_classpath

from test_phase0_wire import MiniPgClient, _free_port


@pytest.fixture(scope="module")
def writable_backend(tmp_path_factory, calcite_backend):
    """A Calcite connection over a DuckDB file with one table, T(ID, QTY).

    Integer columns only: DuckDB reports VARCHAR with no length, which Calcite's JDBC
    adapter reads as VARCHAR(0) and truncates every written string to.
    """
    del calcite_backend  # only for the JVM it starts
    try:
        resolve_classpath()
    except ClasspathError as exc:
        pytest.skip(f"Calcite classpath unavailable: {exc}")
    import jpype

    from pgwire_calcite.calcite_backend import CalciteBackend

    root = tmp_path_factory.mktemp("dml")
    url = f"jdbc:duckdb:{root / 'writable.db'}"
    setup = jpype.JClass("org.duckdb.DuckDBDriver")().connect(
        url, jpype.JClass("java.util.Properties")()
    )
    try:
        stmt = setup.createStatement()
        stmt.execute('CREATE TABLE "T" ("ID" INTEGER, "QTY" INTEGER)')
        stmt.close()
    finally:
        setup.close()
    model = root / "model.json"
    model.write_text(
        json.dumps(
            {
                "version": "1.0",
                "defaultSchema": "W",
                "schemas": [
                    {
                        "name": "W",
                        "type": "jdbc",
                        "jdbcDriver": "org.duckdb.DuckDBDriver",
                        "jdbcUrl": url,
                    }
                ],
            }
        )
    )
    backend = CalciteBackend(model_path=str(model), jvm_args=["-Xmx1g"])
    yield backend
    backend.close()


def _serve(backend, **kwargs):
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", backend=backend, **kwargs)
    time.sleep(0.1)
    return srv, port


def _query(port: int, sql: str) -> dict:
    client = MiniPgClient("127.0.0.1", port)
    try:
        return client.query(sql)
    finally:
        client.close()


def _rows(port: int) -> list[list[str]]:
    r = _query(port, 'SELECT "ID", "QTY" FROM W."T" ORDER BY "ID"')
    assert r["error"] is None, r["error"]
    return r["rows"]


def test_write_is_rejected_without_allow_writes(writable_backend):
    srv, port = _serve(writable_backend)
    try:
        r = _query(port, """INSERT INTO W."T" ("ID", "QTY") VALUES (1, 10)""")
        assert r["error"] is not None
        assert "read-only" in r["error"] and "--allow-writes" in r["error"]
        assert _rows(port) == []
    finally:
        srv.shutdown()


def test_insert_update_delete_report_their_row_counts(writable_backend):
    srv, port = _serve(writable_backend, allow_writes=True)
    try:
        r = _query(port, """INSERT INTO W."T" ("ID", "QTY") VALUES (1, 10), (2, 20), (3, 30)""")
        assert r["error"] is None, r["error"]
        assert r["command_tag"] == "INSERT 0 3"
        assert _rows(port) == [["1", "10"], ["2", "20"], ["3", "30"]]

        r = _query(port, """UPDATE W."T" SET "QTY" = 99 WHERE "ID" >= 2""")
        assert r["error"] is None, r["error"]
        assert r["command_tag"] == "UPDATE 2"
        assert _rows(port) == [["1", "10"], ["2", "99"], ["3", "99"]]

        r = _query(port, """DELETE FROM W."T" WHERE "QTY" = 99""")
        assert r["error"] is None, r["error"]
        assert r["command_tag"] == "DELETE 2"
        assert _rows(port) == [["1", "10"]]

        r = _query(port, 'DELETE FROM W."T"')
        assert r["command_tag"] == "DELETE 1"
    finally:
        srv.shutdown()


def test_parameterised_write_runs_exactly_once(writable_backend):
    """The extended protocol describes a statement before executing it; describing a write
    must not run it."""
    srv, port = _serve(writable_backend, allow_writes=True)
    try:
        with psycopg.connect(
            host="127.0.0.1", port=port, user="tester", dbname="postgres", autocommit=True
        ) as conn:
            cur = conn.execute('INSERT INTO W."T" ("ID", "QTY") VALUES (%s, %s)', (7, 70))
            assert cur.rowcount == 1
            assert cur.statusmessage == "INSERT 0 1"
            cur = conn.execute('UPDATE W."T" SET "QTY" = %s WHERE "ID" = %s', (71, 7))
            assert cur.rowcount == 1
        assert _rows(port) == [["7", "71"]]
        with psycopg.connect(
            host="127.0.0.1", port=port, user="tester", dbname="postgres", autocommit=True
        ) as conn:
            assert conn.execute('DELETE FROM W."T" WHERE "ID" = %s', (7,)).rowcount == 1
        assert _rows(port) == []
    finally:
        srv.shutdown()


def test_commit_after_a_write_succeeds_and_rollback_is_refused(writable_backend):
    srv, port = _serve(writable_backend, allow_writes=True)
    try:
        client = MiniPgClient("127.0.0.1", port)
        try:
            assert client.query("BEGIN")["error"] is None
            assert client.query("""INSERT INTO W."T" ("ID", "QTY") VALUES (1, 10)""")["error"] is None
            assert client.query("COMMIT")["error"] is None

            assert client.query("BEGIN")["error"] is None
            assert client.query('DELETE FROM W."T" WHERE "ID" = 1')["error"] is None
            r = client.query("ROLLBACK")
            assert r["error"] is not None and "cannot undo" in r["error"]

            # A transaction that wrote nothing rolls back as before.
            assert client.query("BEGIN")["error"] is None
            assert client.query('SELECT count(*) FROM W."T"')["error"] is None
            assert client.query("ROLLBACK")["error"] is None
        finally:
            client.close()
        # The delete was committed when it ran: the refused ROLLBACK did not bring it back.
        assert _rows(port) == []
    finally:
        srv.shutdown()


def test_returning_is_rejected_before_the_write_runs(writable_backend):
    srv, port = _serve(writable_backend, allow_writes=True)
    try:
        r = _query(port, """INSERT INTO W."T" ("ID", "QTY") VALUES (1, 10) RETURNING "ID\"""")
        assert r["error"] is not None and "RETURNING" in r["error"]
        assert _rows(port) == []
    finally:
        srv.shutdown()


def test_table_that_is_not_modifiable_rejects_the_write(calcite_backend):
    """--allow-writes opens the route; the adapter's tables still decide. The file adapter's
    are read-only."""
    srv, port = _serve(calcite_backend, allow_writes=True)
    try:
        before = _query(port, "SELECT count(*) FROM DEPTS")["rows"]
        r = _query(port, "INSERT INTO DEPTS (DEPTNO, DNAME) VALUES (99, 'X')")
        assert r["error"] is not None
        r = _query(port, "DELETE FROM DEPTS")
        assert r["error"] is not None
        assert _query(port, "SELECT count(*) FROM DEPTS")["rows"] == before
    finally:
        srv.shutdown()


def test_grants_gate_writes(writable_backend):
    from pgwire_calcite.authz import RoleGrants

    grants = RoleGrants.from_dict({"reader": ["w.other"]})
    srv, port = _serve(writable_backend, allow_writes=True, authz_grants=grants)
    try:
        client = MiniPgClient("127.0.0.1", port, user="reader")
        try:
            r = client.query("""INSERT INTO W."T" ("ID", "QTY") VALUES (1, 10)""")
            assert r["error"] is not None and "permission denied" in r["error"]
        finally:
            client.close()
    finally:
        srv.shutdown()
