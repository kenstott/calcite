# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SQL cursors: DECLARE, FETCH, MOVE and CLOSE (REQ-2255, ported from Provisa's REQ-1862).

A cursor is session-scoped: it lives until CLOSE, DISCARD ALL or the end of the connection,
as if declared WITH HOLD (pgwire-calcite has no transactions to end it). It reads its query's
result forward, one engine batch resident at a time, and holds the engine while it is open,
as a suspended portal does.

A NO SCROLL cursor (PostgreSQL's default) only moves forward, so rows behind its position are
dropped as it advances and memory is bounded by the FETCH size. A SCROLL cursor keeps every row
it has read, which serves PRIOR, BACKWARD, ABSOLUTE and LAST; that memory is the cost of asking
for a scrollable cursor.
"""

# Requirements: REQ-2255

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Iterator, List, Optional, Tuple

DECLARE_RE = re.compile(
    r"^\s*DECLARE\s+(?P<name>\"[^\"]+\"|[A-Za-z_][\w$]*)\s+"
    r"(?:BINARY\s+|INSENSITIVE\s+|ASENSITIVE\s+|(?P<scroll>NO\s+SCROLL|SCROLL)\s+)*"
    r"CURSOR\s*(?:(?:WITH|WITHOUT)\s+HOLD\s*)?FOR\s+(?P<sql>.+?)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
FETCH_RE = re.compile(r"^\s*FETCH\b", re.IGNORECASE)
MOVE_RE = re.compile(r"^\s*MOVE\b", re.IGNORECASE)
CLOSE_RE = re.compile(
    r"^\s*CLOSE\s+(?P<name>ALL|\"[^\"]+\"|[A-Za-z_][\w$]*)\s*;?\s*$", re.IGNORECASE
)
_FROM_IN_RE = re.compile(
    r"(?:\bFROM\b|\bIN\b)\s+(?P<name>\"[^\"]+\"|[A-Za-z_][\w$]*)\s*;?\s*$", re.IGNORECASE
)


class CursorError(Exception):
    sqlstate = "XX000"


class UnknownCursor(CursorError):
    sqlstate = "34000"  # invalid_cursor_name

    def __init__(self, name: str):
        super().__init__(f'cursor "{name}" does not exist')


class DuplicateCursor(CursorError):
    sqlstate = "42P03"  # duplicate_cursor

    def __init__(self, name: str):
        super().__init__(f'cursor "{name}" already exists')


class CursorNotScrollable(CursorError):
    sqlstate = "55000"  # object_not_in_prerequisite_state

    def __init__(self, name: str, direction: str):
        super().__init__(
            f'cursor "{name}" can only scan forward; DECLARE it with SCROLL to fetch '
            f"{direction.upper()}"
        )


class CursorSyntaxError(CursorError):
    sqlstate = "42601"  # syntax_error


def normalize_name(raw: str) -> str:
    """Fold an unquoted cursor name as PostgreSQL does; keep a quoted one verbatim."""
    raw = raw.strip()
    if len(raw) >= 2 and raw.startswith('"') and raw.endswith('"'):
        return raw[1:-1]
    return raw.lower()


@dataclass
class CursorState:
    """A DECLAREd cursor: its query's result, read forward into ``buffer`` on demand.

    ``pos`` is PostgreSQL's cursor position: 0 before the first row, ``k`` on row ``k``
    (1-based), one past the last row once a forward move ran off the end. ``buffer`` holds rows
    ``base + 1`` onward; a NO SCROLL cursor drops the rows up to its position as it advances, so
    ``base`` counts the rows it no longer holds."""

    name: str
    result: Any  # CalciteQueryResult: columns, typmods, rows(), close()
    source: Iterator[Any]
    scrollable: bool = False
    buffer: List[Any] = field(default_factory=list)
    base: int = 0
    pos: int = 0
    exhausted: bool = False

    def close(self) -> None:
        self.result.close()

    def row(self, index: int) -> Any:
        return self.buffer[index - 1 - self.base]

    def available(self) -> int:
        """The last row index read so far (the row count, once ``exhausted``)."""
        return self.base + len(self.buffer)


def _fill(cs: CursorState, upto: float) -> None:
    while not cs.exhausted and cs.available() < upto:
        row = next(cs.source, None)
        if row is None:
            cs.exhausted = True
            break
        cs.buffer.append(row)


def _land(cs: CursorState, target: float) -> List[Any]:
    """Move to row ``target`` and return it alone (ABSOLUTE / RELATIVE / FIRST / LAST)."""
    if target < 1:
        cs.pos = 0
        return []
    _fill(cs, target)
    if target > cs.available():
        cs.pos = cs.available() + 1
        return []
    cs.pos = int(target)
    return [cs.row(cs.pos)]


def move(cs: CursorState, direction: str, count: Optional[int]) -> List[Any]:
    """Move ``cs`` as FETCH/MOVE does and return the rows the client sees: FORWARD/BACKWARD the
    rows passed over (BACKWARD most recent first), ABSOLUTE/RELATIVE/FIRST/LAST the one row
    landed on. ``count`` None is ALL."""
    if not cs.scrollable and not (
        direction == "forward" or (direction == "relative" and (count or 0) >= 0)
    ):
        raise CursorNotScrollable(cs.name, direction)
    if direction == "forward":
        target = float("inf") if count is None else cs.pos + count
        _fill(cs, target)
        last = int(min(target, cs.available()))
        out = [cs.row(i) for i in range(cs.pos + 1, last + 1)]
        cs.pos = cs.available() + 1 if target > cs.available() else last
    elif direction == "backward":
        target = 1 if count is None else cs.pos - count
        out = [cs.row(i) for i in range(min(cs.pos - 1, cs.available()), max(target, 1) - 1, -1)]
        cs.pos = 0 if count is None or target < 1 else target
    elif direction == "absolute":
        k = count or 0
        if k < 0:
            _fill(cs, float("inf"))  # counted from the end: needs the whole extent
            k = cs.available() + k + 1
            if k < 1:
                cs.pos = 0
                return []
        out = _land(cs, k)
    elif direction == "relative":
        out = _land(cs, cs.pos + (count or 0))
    elif direction == "first":
        out = _land(cs, 1)
    elif direction == "last":
        _fill(cs, float("inf"))
        out = _land(cs, cs.available())
    else:
        raise CursorSyntaxError(f"unknown cursor direction {direction!r}")
    if not cs.scrollable:
        # Forward only: a row at or behind the position is never read again.
        drop = min(cs.pos, cs.available()) - cs.base
        if drop > 0:
            del cs.buffer[:drop]
            cs.base += drop
    return out


def parse_navigation(stmt: str, keyword: str) -> Tuple[str, str, Optional[int]]:
    """A FETCH or MOVE statement as (direction, cursor name, count); count None is ALL."""
    body = re.sub(rf"^\s*{keyword}\b", "", stmt, flags=re.IGNORECASE).strip()
    if body.endswith(";"):
        body = body[:-1].strip()
    m = _FROM_IN_RE.search(body)
    if m:
        name, spec = m.group("name"), body[: m.start()].strip()
    else:
        parts = body.rsplit(None, 1)
        if len(parts) == 2:
            spec, name = parts
        elif len(parts) == 1:
            spec, name = "", parts[0]
        else:
            raise CursorSyntaxError(f"{keyword} names no cursor")
    name = normalize_name(name)
    su = " ".join(spec.upper().split())
    if su in ("", "NEXT", "FORWARD"):
        return "forward", name, 1
    if su in ("PRIOR", "BACKWARD"):
        return "backward", name, 1
    if su == "FIRST":
        return "first", name, None
    if su == "LAST":
        return "last", name, None
    if su in ("ALL", "FORWARD ALL"):
        return "forward", name, None
    if su == "BACKWARD ALL":
        return "backward", name, None
    m2 = re.match(r"^(FORWARD|BACKWARD)\s+(\d+)$", su)
    if m2:
        return ("forward" if m2.group(1) == "FORWARD" else "backward"), name, int(m2.group(2))
    m3 = re.match(r"^(ABSOLUTE|RELATIVE)\s+([-+]?\d+)$", su)
    if m3:
        return m3.group(1).lower(), name, int(m3.group(2))
    m4 = re.match(r"^([-+]?\d+)$", su)
    if m4:
        n = int(m4.group(1))
        return ("backward", name, -n) if n < 0 else ("forward", name, n)
    raise CursorSyntaxError(f"unsupported {keyword} direction {spec!r}")
