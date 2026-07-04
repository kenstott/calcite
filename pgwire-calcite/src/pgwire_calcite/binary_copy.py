# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Server-side ``COPY (query) TO STDOUT (FORMAT binary)`` (PGW-021).

This is DuckDB's default read path once it has ``ATTACH``ed: it wraps each scan as
``COPY (SELECT ...) TO STDOUT (FORMAT binary)`` and expects the PostgreSQL binary
COPY stream. We execute the inner query through the normal session seam (so it is
transpiled + streamed via Arrow, PGW-019/020) and encode rows into pg binary COPY
using buenavista's per-BVType binary encoders — whose type OIDs already agree with
the catalog's column OIDs (normalize.py), so DuckDB decodes them correctly.

Row values stream one at a time from the Arrow generator; nothing is materialized.
Only ``TO STDOUT`` (read) is supported — this is a read federation surface.
"""

from __future__ import annotations

import logging
import re
import struct
from typing import Iterator, Optional, Tuple

from buenavista.core import BVType
from buenavista.postgres import BVTYPE_TO_PGTYPE, ServerResponse

log = logging.getLogger(__name__)

# PGCOPY signature + flags(0) + header-extension-length(0).
_COPY_BINARY_HEADER = b"PGCOPY\n\xff\r\n\x00" + struct.pack("!ii", 0, 0)
_COPY_BINARY_TRAILER = struct.pack("!h", -1)

#: Coalesce binary-COPY output into CopyData messages of at least this size, so the
#: PGCOPY header never ships alone (DuckDB rejects that) while memory stays bounded.
_COPY_FLUSH_BYTES = 65536

# FORMAT value may be a bare word or quoted (DuckDB sends FORMAT "binary").
_FMT = r"(?:\(\s*FORMAT\s+[\"']?(?P<fmt>\w+)[\"']?\s*\)|WITH\s+(?P<wbin>BINARY))?"
_COPY_TO_RE = re.compile(
    r"^\s*COPY\s*\(\s*(?P<query>.+?)\s*\)\s*TO\s+STDOUT\s*" + _FMT + r"\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
# COPY table TO STDOUT ... (DuckDB uses the (query) form, but accept table form too)
_COPY_TABLE_TO_RE = re.compile(
    r"^\s*COPY\s+(?P<table>[^\s(].*?)\s+TO\s+STDOUT\s*" + _FMT + r"\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)


class BinaryCopyError(RuntimeError):
    """COPY could not be served. Never swallowed silently."""


def parse_copy_to_stdout(sql: str) -> Optional[Tuple[str, str]]:
    """Return (inner_select_sql, format) for a COPY .. TO STDOUT, else None.

    format is 'binary' or 'text'. The table form ``COPY t TO STDOUT`` becomes
    ``SELECT * FROM t``.
    """
    m = _COPY_TO_RE.match(sql)
    if m:
        fmt = "binary" if (m.group("wbin") or (m.group("fmt") or "").lower() == "binary") else "text"
        return m.group("query"), fmt
    m = _COPY_TABLE_TO_RE.match(sql)
    if m:
        fmt = "binary" if (m.group("wbin") or (m.group("fmt") or "").lower() == "binary") else "text"
        return f"SELECT * FROM {m.group('table')}", fmt
    return None


def _binary_encoder(bvtype: BVType):
    """The pg-binary encoder for a BVType (buenavista), or None if text-only."""
    spec = BVTYPE_TO_PGTYPE.get(bvtype)
    if spec and len(spec) >= 3:
        return spec[2]
    return None


def encode_binary_copy(query_result) -> Iterator[bytes]:
    """Yield pg binary-COPY byte chunks (header, one per row, trailer).

    ``query_result`` is a buenavista QueryResult (our CalciteQueryResult): columns
    carry BVTypes, rows() streams tuples.
    """
    ncols = query_result.column_count()
    encoders = []
    for i in range(ncols):
        _, bvtype = query_result.column(i)
        enc = _binary_encoder(bvtype)
        if enc is None:
            # Fall back to UTF-8 text bytes for a type with no binary codec, rather
            # than emitting a wrong binary layout. Rare for our column types.
            enc = lambda v: str(v).encode("utf-8")  # noqa: E731
        encoders.append(enc)

    yield _COPY_BINARY_HEADER
    for row in query_result.rows():
        parts = [struct.pack("!h", ncols)]
        for i, val in enumerate(row):
            if val is None:
                parts.append(struct.pack("!i", -1))
            else:
                b = encoders[i](val)
                parts.append(struct.pack("!i", len(b)))
                parts.append(b)
        yield b"".join(parts)
    yield _COPY_BINARY_TRAILER


class BinaryCopyHandler:
    """Serves COPY .. TO STDOUT (binary/text) for the wire handler."""

    def __init__(self, handler) -> None:
        self.handler = handler  # CalciteHandler (has wfile + ctx.execute_sql path)

    def handle(self, ctx, sql: str) -> int:
        parsed = parse_copy_to_stdout(sql)
        if parsed is None:
            raise BinaryCopyError(
                "Only COPY ... TO STDOUT is supported (read federation surface). "
                "Rejected: " + sql[:200]
            )
        query, fmt = parsed
        result = ctx.execute_sql(query)  # transpiled + Arrow-streamed via the session
        ncols = result.column_count()

        self._send_copy_out_response(binary=(fmt == "binary"), ncols=ncols)
        if fmt == "binary":
            # Flush the binary COPY in threshold-sized CopyData messages. DuckDB's
            # binary reader rejects the PGCOPY header when it arrives alone in its
            # own tiny CopyData message, so we coalesce the header with following
            # rows and flush at ~64 KiB — still bounded memory (PGW-020), not a
            # full-result buffer.
            buf = bytearray()
            chunks = 0
            for chunk in encode_binary_copy(result):
                buf += chunk
                chunks += 1
                if len(buf) >= _COPY_FLUSH_BYTES:
                    self._send_copy_data(bytes(buf))
                    buf.clear()
            if buf:
                self._send_copy_data(bytes(buf))
            nrows = max(0, chunks - 2)  # exclude the header and trailer chunks
        else:
            nrows = self._send_text_copy(result)
        self._send_copy_done()
        return nrows

    # --- wire messages --------------------------------------------------------

    def _send_copy_out_response(self, binary: bool, ncols: int) -> None:
        fmt_code = 1 if binary else 0
        body = struct.pack("!Bh", fmt_code, ncols) + b"".join(
            struct.pack("!h", fmt_code) for _ in range(ncols)
        )
        self.handler.wfile.write(
            struct.pack("!ci", ServerResponse.COPY_OUT_RESPONSE, len(body) + 4)
        )
        self.handler.wfile.write(body)

    def _send_copy_data(self, data: bytes) -> None:
        self.handler.wfile.write(struct.pack("!ci", ServerResponse.COPY_DATA, len(data) + 4))
        self.handler.wfile.write(data)

    def _send_copy_done(self) -> None:
        self.handler.wfile.write(struct.pack("!ci", ServerResponse.COPY_DONE, 4))
        self.handler.wfile.flush()

    def _send_text_copy(self, result) -> int:
        n = 0
        for row in result.rows():
            line = "\t".join("\\N" if v is None else str(v) for v in row) + "\n"
            self._send_copy_data(line.encode("utf-8"))
            n += 1
        return n
