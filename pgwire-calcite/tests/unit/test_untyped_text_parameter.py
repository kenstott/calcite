"""A parameter the client sends with no declared type is typed by the statement.

psycopg sends a Python ``str`` as a parameter of unknown type. The server answered Describe
with int8 for every placeholder that had no inline cast, so a text parameter compared with a
text column was planned as a number and the statement failed.
"""

from __future__ import annotations

import time

import psycopg
import pytest

from pgwire_calcite import launcher

from test_phase0_wire import _free_port


@pytest.fixture(scope="module")
def server(calcite_backend):
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", backend=calcite_backend)
    time.sleep(0.2)
    yield port
    srv.shutdown()
    srv.server_close()


def _conn(port):
    return psycopg.connect(f"host=127.0.0.1 port={port} user=tester dbname=postgres", autocommit=True)


def test_a_text_parameter_compared_with_a_text_column_selects_by_it(server):
    with _conn(server) as conn:
        rows = conn.execute('SELECT "dname" FROM "SALES"."depts" WHERE "dname" = %s', ("ACCOUNTING",)).fetchall()
    assert rows == [("ACCOUNTING",)]


def test_a_number_parameter_compared_with_a_number_column_still_selects_by_it(server):
    with _conn(server) as conn:
        rows = conn.execute('SELECT "dname" FROM "SALES"."depts" WHERE "deptno" = %s', (10,)).fetchall()
    assert rows == [("ACCOUNTING",)]


def test_a_prepared_statement_with_a_text_parameter_selects_by_it(server):
    with _conn(server) as conn:
        sql = 'SELECT "dname" FROM "SALES"."depts" WHERE "dname" = %s'
        rows = conn.execute(sql, ("ACCOUNTING",), prepare=True).fetchall()
        again = conn.execute(sql, ("RESEARCH",), prepare=True).fetchall()
    assert (rows, again) == ([("ACCOUNTING",)], [("RESEARCH",)])


def test_asyncpg_binds_a_text_parameter_the_statement_compares_with_a_text_column(server):
    """asyncpg describes a statement before it binds, and encodes each parameter as the type
    the server reports. Reported as int8, a text parameter could not be sent at all, and the
    Describe itself failed: its example statement compared the text column with a number, which
    the engine evaluates by reading every value of the column as a number."""
    asyncpg = pytest.importorskip("asyncpg")
    import asyncio

    async def run():
        conn = await asyncpg.connect(host="127.0.0.1", port=server, user="tester", database="postgres")
        try:
            by_name = await conn.fetch('SELECT "deptno" FROM "SALES"."depts" WHERE "dname" = $1', "RESEARCH")
            by_number = await conn.fetch('SELECT "dname" FROM "SALES"."depts" WHERE "deptno" = $1', 10)
            both = await conn.fetch(
                'SELECT "dname" FROM "SALES"."depts" WHERE "deptno" = $2 AND "dname" = $1', "ACCOUNTING", 10
            )
        finally:
            await conn.close()
        return [tuple(r) for r in by_name], [tuple(r) for r in by_number], [tuple(r) for r in both]

    assert asyncio.run(run()) == ([(20,)], [("ACCOUNTING",)], [("ACCOUNTING",)])


def test_a_statement_the_engine_cannot_place_a_parameter_in_keeps_the_number_default(server):
    """``SELECT $1`` gives the engine nothing to type the parameter by: it stays a number, as
    it was, and a cast still says otherwise."""
    asyncpg = pytest.importorskip("asyncpg")
    import asyncio

    async def run():
        conn = await asyncpg.connect(host="127.0.0.1", port=server, user="tester", database="postgres")
        try:
            by_number = await conn.fetch('SELECT "deptno" FROM "SALES"."depts" WHERE "deptno" = $1::int', 20)
            by_name = await conn.fetch('SELECT "dname" FROM "SALES"."depts" WHERE "dname" = $1::text', "RESEARCH")
            return [tuple(r) for r in by_number], [tuple(r) for r in by_name]
        finally:
            await conn.close()

    assert asyncio.run(run()) == ([(20,)], [("RESEARCH",)])


def test_the_example_statement_of_a_describe_carries_a_value_of_each_parameters_type():
    from pgwire_calcite.server import _example_sql, _parameter_oid

    assert _example_sql("UPDATE t SET a = $1 WHERE b = $2", [25, 20]) == "UPDATE t SET a = '' WHERE b = 42"
    assert (_parameter_oid("VARCHAR(20)"), _parameter_oid("INTEGER"), _parameter_oid("DOUBLE")) == (25, 23, 701)
    with pytest.raises(Exception, match="not supported"):
        _parameter_oid("GEOMETRY")


def test_a_statement_after_a_row_limited_one_reads_its_own_rows(server):
    """A read that stops at a row limit leaves its portal suspended on the result. The next
    statement bound to the same portal was handed that leftover result -- so every second
    single-row read (asyncpg's fetchrow/fetchval) came back empty."""
    asyncpg = pytest.importorskip("asyncpg")
    import asyncio

    async def run():
        conn = await asyncpg.connect(host="127.0.0.1", port=server, user="tester", database="postgres")
        try:
            by_name = 'SELECT "deptno" FROM "SALES"."depts" WHERE "dname" = $1'
            return [
                await conn.fetchval(by_name, "RESEARCH"),
                await conn.fetchval(by_name, "ACCOUNTING"),
                await conn.fetchval('SELECT "dname" FROM "SALES"."depts" WHERE "deptno" = $1', 20),
                tuple(await conn.fetchrow('SELECT "deptno", "dname" FROM "SALES"."depts" WHERE "deptno" = 10')),
                len(await conn.fetch('SELECT "deptno" FROM "SALES"."depts"')),
            ]
        finally:
            await conn.close()

    assert asyncio.run(run()) == [20, 10, "RESEARCH", (10, "ACCOUNTING"), 4]
