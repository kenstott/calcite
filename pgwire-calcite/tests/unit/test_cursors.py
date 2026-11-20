# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SQL cursors: DECLARE / FETCH / MOVE / CLOSE (REQ-2255, ported from Provisa's REQ-1862).

A NO SCROLL cursor reads forward and holds no more rows than its last FETCH; a SCROLL cursor
serves PRIOR, BACKWARD, ABSOLUTE and LAST; an unknown or duplicate cursor, and backward
movement on a NO SCROLL one, are errors with PostgreSQL's SQLSTATEs.
"""

# Requirements: REQ-2255

from __future__ import annotations

import time

import pytest

from pgwire_calcite import cursors, launcher

from test_phase0_wire import MiniPgClient, _free_port



@pytest.fixture()
def client(calcite_backend):
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", backend=calcite_backend)
    time.sleep(0.1)
    c = MiniPgClient("127.0.0.1", port)
    try:
        yield c
    finally:
        c.query("CLOSE ALL")
        c.sock.close()
        srv.shutdown()


def _empnos(c: MiniPgClient) -> list:
    r = c.query("SELECT empno FROM emps ORDER BY empno")
    assert r["error"] is None, r
    return [row[0] for row in r["rows"]]


def _ok(r: dict) -> dict:
    assert r["error"] is None, r
    return r


def test_a_no_scroll_cursor_reads_forward_in_fetches(client):
    every = _empnos(client)
    assert len(every) == 10
    assert _ok(client.query(
        "DECLARE c CURSOR FOR SELECT empno FROM emps ORDER BY empno"
    ))["command_tag"] == "DECLARE CURSOR"
    r = _ok(client.query("FETCH 3 FROM c"))
    assert [row[0] for row in r["rows"]] == every[:3] and r["command_tag"] == "FETCH 3"
    assert [row[0] for row in _ok(client.query("FETCH NEXT FROM c"))["rows"]] == every[3:4]
    assert _ok(client.query("MOVE 2 IN c"))["command_tag"] == "MOVE 2"
    r = _ok(client.query("FETCH ALL FROM c"))
    assert [row[0] for row in r["rows"]] == every[6:] and r["command_tag"] == "FETCH 4"
    r = _ok(client.query("FETCH 1 FROM c"))
    assert r["rows"] == [] and r["command_tag"] == "FETCH 0"
    assert _ok(client.query("CLOSE c"))["command_tag"] == "CLOSE CURSOR"
    assert "34000" in (client.query("FETCH 1 FROM c")["error"] or "")


def test_backward_movement_needs_a_scroll_cursor(client):
    _ok(client.query("DECLARE c NO SCROLL CURSOR FOR SELECT empno FROM emps ORDER BY empno"))
    _ok(client.query("FETCH 2 FROM c"))
    assert "55000" in (client.query("FETCH PRIOR FROM c")["error"] or "")


def test_a_scroll_cursor_moves_both_ways(client):
    every = _empnos(client)
    _ok(client.query("DECLARE s SCROLL CURSOR FOR SELECT empno FROM emps ORDER BY empno"))
    assert [r[0] for r in _ok(client.query("FETCH LAST FROM s"))["rows"]] == every[-1:]
    assert [r[0] for r in _ok(client.query("FETCH PRIOR FROM s"))["rows"]] == every[-2:-1]
    assert [r[0] for r in _ok(client.query("FETCH ABSOLUTE 2 FROM s"))["rows"]] == every[1:2]
    r = _ok(client.query("FETCH BACKWARD ALL FROM s"))
    assert [row[0] for row in r["rows"]] == every[:1]


def test_scroll_positions_follow_postgresql(client):
    every = _empnos(client)
    _ok(client.query("DECLARE s SCROLL CURSOR FOR SELECT empno FROM emps ORDER BY empno"))
    _ok(client.query("FETCH ALL FROM s"))  # now after the last row
    assert [r[0] for r in _ok(client.query("FETCH PRIOR FROM s"))["rows"]] == every[-1:]
    assert [r[0] for r in _ok(client.query("FETCH RELATIVE -2 FROM s"))["rows"]] == every[-3:-2]
    assert [r[0] for r in _ok(client.query("FETCH RELATIVE 0 FROM s"))["rows"]] == every[-3:-2]
    assert [r[0] for r in _ok(client.query("FETCH FIRST FROM s"))["rows"]] == every[:1]
    assert _ok(client.query("FETCH ABSOLUTE 99 FROM s"))["rows"] == []
    assert [r[0] for r in _ok(client.query("FETCH PRIOR FROM s"))["rows"]] == every[-1:]


def test_a_duplicate_cursor_is_refused(client):
    _ok(client.query("DECLARE d CURSOR FOR SELECT empno FROM emps"))
    assert "42P03" in (client.query("DECLARE d CURSOR FOR SELECT empno FROM emps")["error"] or "")


def test_other_statements_run_while_a_cursor_is_open(client):
    every = _empnos(client)
    _ok(client.query("DECLARE c CURSOR FOR SELECT empno FROM emps ORDER BY empno"))
    _ok(client.query("FETCH 2 FROM c"))
    assert _ok(client.query("SELECT count(*) AS n FROM emps"))["rows"] == [["10"]]
    assert [r[0] for r in _ok(client.query("FETCH 2 FROM c"))["rows"]] == every[2:4]


def test_psycopg_named_cursor(calcite_backend):
    psycopg = pytest.importorskip("psycopg")
    port = _free_port()
    srv = launcher.serve(host="127.0.0.1", port=port, auth="none", backend=calcite_backend)
    time.sleep(0.1)
    try:
        with psycopg.connect(f"host=127.0.0.1 port={port} user=tester dbname=postgres") as conn:
            with conn.cursor(name="named") as cur:
                cur.itersize = 3
                cur.execute("SELECT empno FROM emps ORDER BY empno")
                got = [row[0] for row in cur]
        assert len(got) == 10 and got == sorted(got)
    finally:
        srv.shutdown()


def test_a_no_scroll_cursor_holds_only_its_last_fetch():
    pulled = []

    def source():
        for i in range(10_000):
            pulled.append(i)
            yield (i,)

    class _Result:
        def close(self):
            pass

    cs = cursors.CursorState(name="c", result=_Result(), source=source())
    for start in range(0, 500, 100):
        rows = cursors.move(cs, "forward", 100)
        assert rows == [(i,) for i in range(start, start + 100)]
        assert len(cs.buffer) == 0
    assert len(pulled) == 500  # read on demand, never ahead of the fetches


@pytest.mark.parametrize(
    "stmt, expected",
    [
        ("FETCH c", ("forward", "c", 1)),
        ("FETCH FORWARD 5 FROM c", ("forward", "c", 5)),
        ("FETCH ALL IN \"MixedCase\"", ("forward", "MixedCase", None)),
        ("FETCH BACKWARD ALL FROM c", ("backward", "c", None)),
        ("FETCH RELATIVE -2 FROM c", ("relative", "c", -2)),
        ("FETCH -3 FROM C", ("backward", "c", 3)),
    ],
)
def test_fetch_directions_parse_as_postgresql_reads_them(stmt, expected):
    assert cursors.parse_navigation(stmt, "FETCH") == expected
