# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Server-side ``COPY (query|table) TO STDOUT`` — the one COPY dispatcher (PGW-021).

This is DuckDB's default read path once it has ``ATTACH``ed: it wraps each scan as
``COPY (SELECT ...) TO STDOUT (FORMAT binary)`` and expects the PostgreSQL binary
COPY stream. psql and other clients ask for ``FORMAT text`` / ``FORMAT csv`` over
the same statement, so all three formats share one parser, one governance seam,
and one framing/flush loop here; there is no second COPY implementation.

The inner query runs through the normal session seam (``ctx.execute_sql``), which
is where per-role authorization (PGW-045) and the query log live — so an export is
governed exactly like the equivalent SELECT.

Encoding is explicit per column: each column's *advertised* PostgreSQL type OID
(the same OID RowDescription/pg_attribute report) selects a codec from
``_BINARY_CODECS``. A column whose OID has no binary codec is a loud error raised
BEFORE CopyOutResponse is sent — never a UTF-8 text fallback under a non-text OID,
which would hand the client a correctly-framed but wrongly-typed stream.

Row values stream one at a time from the Arrow generator; nothing is materialized.
Only ``TO STDOUT`` (read) is supported — this is a read federation surface.
"""

from __future__ import annotations

import csv
import datetime
import decimal
import io
import json
import logging
import re
import struct
import uuid as _uuid
from typing import Callable, Dict, Iterator, Optional, Tuple

from buenavista.core import BVType
from buenavista.postgres import (
    BVTYPE_TO_PGTYPE,
    PG_UNKNOWN,
    ServerResponse,
    _numeric_to_pg_binary,
)

log = logging.getLogger(__name__)

# PGCOPY signature + flags(0) + header-extension-length(0).
_COPY_BINARY_HEADER = b"PGCOPY\n\xff\r\n\x00" + struct.pack("!ii", 0, 0)
_COPY_BINARY_TRAILER = struct.pack("!h", -1)

#: Coalesce COPY output into CopyData messages of at least this size, so the
#: PGCOPY header never ships alone (DuckDB rejects that) while memory stays bounded.
_COPY_FLUSH_BYTES = 65536

#: The formats this server serves. Anything else is rejected loudly.
_FORMATS = ("text", "csv", "binary")

# --- statement parsing --------------------------------------------------------
#
# Accepted, case-insensitively, with or without WITH, with or without quotes
# around the format word, with or without a trailing semicolon:
#
#   COPY (SELECT ...) TO STDOUT
#   COPY (SELECT ...) TO STDOUT (FORMAT binary)
#   COPY (SELECT ...) TO STDOUT WITH (FORMAT "csv", HEADER)
#   COPY schema.tbl   TO STDOUT WITH BINARY
#   COPY schema.tbl (a, b) TO STDOUT (FORMAT text)

_OPTS = (
    r"(?:\s+(?:WITH\s*)?\(\s*(?P<opts>[^)]*)\)"  # (FORMAT x[, ...]) / WITH (FORMAT x)
    r"|\s+WITH\s+(?P<legacy>BINARY|CSV|TEXT))?"  # legacy: WITH BINARY / WITH CSV
)
_COPY_TO_RE = re.compile(
    r"^\s*COPY\s*\(\s*(?P<query>.+?)\s*\)\s*TO\s+STDOUT" + _OPTS + r"\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
# COPY table TO STDOUT ... (DuckDB uses the (query) form, but accept table form too)
_COPY_TABLE_TO_RE = re.compile(
    r"^\s*COPY\s+(?P<table>[^\s(,]+)"
    r"(?:\s*\(\s*(?P<cols>[^)]*?)\s*\))?"
    r"\s+TO\s+STDOUT" + _OPTS + r"\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_FORMAT_OPT_RE = re.compile(r"\bFORMAT\s+[\"']?(?P<fmt>\w+)[\"']?", re.IGNORECASE)


class BinaryCopyError(RuntimeError):
    """COPY could not be served. Never swallowed silently."""


def _format_from_options(opts: Optional[str], legacy: Optional[str]) -> str:
    """The requested COPY format, defaulting to PostgreSQL's own default (text)."""
    if legacy:
        return legacy.lower()
    if opts:
        m = _FORMAT_OPT_RE.search(opts)
        if m:
            fmt = m.group("fmt").lower()
            if fmt not in _FORMATS:
                raise BinaryCopyError(
                    f"unsupported COPY format {fmt!r}; expected one of {', '.join(_FORMATS)}"
                )
            return fmt
    return "text"  # PostgreSQL's documented default when no FORMAT option is given


def parse_copy_to_stdout(sql: str) -> Optional[Tuple[str, str]]:
    """Return (inner_select_sql, format) for a COPY .. TO STDOUT, else None.

    format is one of 'text', 'csv', 'binary'. The table form ``COPY t TO STDOUT``
    becomes ``SELECT * FROM t``; an explicit column list becomes its select list.
    """
    m = _COPY_TO_RE.match(sql)
    if m:
        return m.group("query"), _format_from_options(m.group("opts"), m.group("legacy"))
    m = _COPY_TABLE_TO_RE.match(sql)
    if m:
        cols = (m.group("cols") or "").strip() or "*"
        return (
            f"SELECT {cols} FROM {m.group('table')}",
            _format_from_options(m.group("opts"), m.group("legacy")),
        )
    return None


# --- binary codecs, keyed on the advertised PostgreSQL type OID ---------------
#
# Cross-checked against provisa/pgwire/copy_binary.py's tag table and the PG
# `send` functions each type OID names. Every OID this server can advertise --
# from BVTYPE_TO_PGTYPE (wire) and normalize._TYPE_TABLE (catalog) -- has an
# entry; an OID missing here raises rather than falling back to text.

_PG_EPOCH_ORDINAL = datetime.date(2000, 1, 1).toordinal()  # 730120
_PG_EPOCH_DT = datetime.datetime(2000, 1, 1)  # noqa: DTZ001 -- PG's timestamp epoch is naive


def _enc_bool(v: object) -> bytes:
    return b"\x01" if v else b"\x00"


def _enc_int2(v: object) -> bytes:
    return struct.pack("!h", int(v))  # type: ignore[arg-type]


def _enc_int4(v: object) -> bytes:
    return struct.pack("!i", int(v))  # type: ignore[arg-type]


def _enc_int8(v: object) -> bytes:
    return struct.pack("!q", int(v))  # type: ignore[arg-type]


def _enc_float4(v: object) -> bytes:
    return struct.pack("!f", float(v))  # type: ignore[arg-type]


def _enc_float8(v: object) -> bytes:
    return struct.pack("!d", float(v))  # type: ignore[arg-type]


def _enc_text(v: object) -> bytes:
    return v.encode("utf-8") if isinstance(v, str) else str(v).encode("utf-8")


def _enc_bytea(v: object) -> bytes:
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v)
    raise BinaryCopyError(f"bytea column carried a non-bytes value: {type(v).__name__}")


def _enc_uuid(v: object) -> bytes:
    return (v if isinstance(v, _uuid.UUID) else _uuid.UUID(str(v))).bytes


def _enc_json(v: object) -> bytes:
    return (v if isinstance(v, str) else json.dumps(v)).encode("utf-8")


def _enc_jsonb(v: object) -> bytes:
    return b"\x01" + _enc_json(v)  # jsonb binary wire format version byte


def _enc_numeric(v: object) -> bytes:
    return _numeric_to_pg_binary(v if isinstance(v, decimal.Decimal) else decimal.Decimal(str(v)))


def _enc_date(v: object) -> bytes:
    if isinstance(v, datetime.datetime):
        v = v.date()
    if not isinstance(v, datetime.date):
        raise BinaryCopyError(f"date column carried a {type(v).__name__}")
    # signed: dates before 2000-01-01 are negative (buenavista's unsigned
    # to_bytes raises OverflowError on them)
    return struct.pack("!i", v.toordinal() - _PG_EPOCH_ORDINAL)


def _enc_time(v: object) -> bytes:
    if not isinstance(v, datetime.time):
        raise BinaryCopyError(f"time column carried a {type(v).__name__}")
    micros = ((v.hour * 60 + v.minute) * 60 + v.second) * 1_000_000 + v.microsecond
    return struct.pack("!q", micros)


def _to_naive_utc(v: object) -> datetime.datetime:
    if isinstance(v, datetime.datetime):
        if v.tzinfo is None:
            return v
        return v.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    if isinstance(v, datetime.date):
        return datetime.datetime(v.year, v.month, v.day)  # noqa: DTZ001 -- PG epoch is naive
    raise BinaryCopyError(f"timestamp column carried a {type(v).__name__}")


def _enc_timestamp(v: object) -> bytes:
    delta = _to_naive_utc(v) - _PG_EPOCH_DT
    micros = (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds
    return struct.pack("!q", micros)  # signed: pre-2000 timestamps are negative


def _enc_interval(v: object) -> bytes:
    if not isinstance(v, datetime.timedelta):
        raise BinaryCopyError(f"interval column carried a {type(v).__name__}")
    # PG interval binary = int64 microseconds, int32 days, int32 months (16 bytes)
    return struct.pack("!qii", v.seconds * 1_000_000 + v.microseconds, v.days, 0)


def _array_encoder(elem_oid: int, elem_enc: Callable[[object], bytes]) -> Callable[[object], bytes]:
    def _enc(v: object) -> bytes:
        items = list(v)  # type: ignore[call-overload]
        head = struct.pack("!iii", 1, 1 if any(x is None for x in items) else 0, elem_oid)
        head += struct.pack("!ii", len(items), 1)  # dim length, lower bound
        parts = []
        for x in items:
            if x is None:
                parts.append(struct.pack("!i", -1))
            else:
                b = elem_enc(x)
                parts.append(struct.pack("!i", len(b)) + b)
        return head + b"".join(parts)

    return _enc


_BINARY_CODECS: Dict[int, Callable[[object], bytes]] = {
    16: _enc_bool,  # bool
    17: _enc_bytea,  # bytea
    20: _enc_int8,  # int8
    21: _enc_int2,  # int2
    23: _enc_int4,  # int4
    25: _enc_text,  # text
    114: _enc_json,  # json
    700: _enc_float4,  # float4
    701: _enc_float8,  # float8
    1007: _array_encoder(23, _enc_int4),  # _int4
    1009: _array_encoder(25, _enc_text),  # _text
    1042: _enc_text,  # bpchar
    1043: _enc_text,  # varchar
    1082: _enc_date,  # date
    1083: _enc_time,  # time
    1114: _enc_timestamp,  # timestamp
    1184: _enc_timestamp,  # timestamptz (same layout, value normalized to UTC)
    1186: _enc_interval,  # interval
    1700: _enc_numeric,  # numeric
    2950: _enc_uuid,  # uuid
    3802: _enc_jsonb,  # jsonb
}

#: OIDs the wire may advertise that have NO defined binary representation here.
#: Listed so the error names the type instead of saying "unknown OID".
_NO_BINARY_CODEC = {
    -1: "null",
    705: "unknown",
    2277: "anyarray",
}


def advertised_oid(bvtype: BVType) -> int:
    """The PostgreSQL type OID this server advertises for ``bvtype``.

    Same lookup ``send_row_description`` uses, so the COPY body's layout and the
    type the client resolved from the catalog cannot drift.
    """
    return BVTYPE_TO_PGTYPE.get(bvtype, PG_UNKNOWN)[0]


def binary_codec(bvtype: BVType) -> Callable[[object], bytes]:
    """The binary-COPY codec for ``bvtype``, keyed on its advertised OID.

    Raises BinaryCopyError if the advertised OID has no binary representation --
    encoding it as UTF-8 text under a non-text OID would produce a well-framed
    stream the client silently decodes as the wrong value.
    """
    oid = advertised_oid(bvtype)
    codec = _BINARY_CODECS.get(oid)
    if codec is None:
        name = _NO_BINARY_CODEC.get(oid, f"oid {oid}")
        raise BinaryCopyError(
            f"COPY (FORMAT binary) cannot encode column type {bvtype.name} "
            f"({name}); use FORMAT text or FORMAT csv"
        )
    return codec


def _column_codecs(query_result) -> list:
    """Codecs for every column, resolved before a single byte is written."""
    return [binary_codec(query_result.column(i)[1]) for i in range(query_result.column_count())]


# --- encoders -----------------------------------------------------------------


def encode_binary_copy(query_result) -> Iterator[bytes]:
    """Yield pg binary-COPY byte chunks (header, one per row, trailer).

    ``query_result`` is a buenavista QueryResult (our CalciteQueryResult): columns
    carry BVTypes, rows() streams tuples.
    """
    ncols = query_result.column_count()
    encoders = _column_codecs(query_result)

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


def _text_value(v: object) -> str:
    """One cell in PG COPY *text* format (tab-separated, backslash-escaped)."""
    if v is None:
        return r"\N"
    if isinstance(v, bool):
        return "t" if v else "f"
    if isinstance(v, (bytes, bytearray, memoryview)):
        return "\\\\x" + bytes(v).hex()
    if isinstance(v, datetime.datetime):
        return v.isoformat(sep=" ")
    if isinstance(v, (datetime.date, datetime.time)):
        return v.isoformat()
    s = str(v)
    return s.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")


def _csv_value(v: object) -> object:
    if isinstance(v, bool):
        return "t" if v else "f"
    if isinstance(v, (bytes, bytearray, memoryview)):
        return "\\x" + bytes(v).hex()
    if isinstance(v, datetime.datetime):
        return v.isoformat(sep=" ")
    if isinstance(v, (datetime.date, datetime.time)):
        return v.isoformat()
    return v


def encode_text_copy(query_result) -> Iterator[bytes]:
    """Yield one PG COPY text-format line per row."""
    for row in query_result.rows():
        yield ("\t".join(_text_value(v) for v in row) + "\n").encode("utf-8")


def encode_csv_copy(query_result) -> Iterator[bytes]:
    """Yield one PG COPY csv-format line per row."""
    for row in query_result.rows():
        buf = io.StringIO()
        csv.writer(buf, lineterminator="\n").writerow(
            ["" if v is None else _csv_value(v) for v in row]
        )
        yield buf.getvalue().encode("utf-8")


_ROW_ENCODERS = {"text": encode_text_copy, "csv": encode_csv_copy}


class BinaryCopyHandler:
    """Serves COPY .. TO STDOUT (binary/text/csv) for the wire handler."""

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
        # The session seam applies per-role authz (PGW-045) and the query log, so
        # this export is governed exactly like the equivalent SELECT. A denial
        # raises PermissionError here -- before CopyOutResponse -- and the wire
        # handler turns it into SQLSTATE 42501.
        result = ctx.execute_sql(query)
        ncols = result.column_count()
        binary = fmt == "binary"
        if binary:
            # Resolve every column's codec BEFORE CopyOutResponse commits us to the
            # format: an un-encodable column must fail as a query error, not
            # mid-stream after the client has already been promised binary rows.
            _column_codecs(result)
        log.info(
            "[COPY] role=%s format=%s ncols=%d sql=%r",
            getattr(ctx.session, "role_id", None),
            fmt,
            ncols,
            query[:300],
        )

        self._send_copy_out_response(binary=binary, ncols=ncols)
        if binary:
            nrows = self._stream(encode_binary_copy(result), skip=2)
        else:
            nrows = self._stream(_ROW_ENCODERS[fmt](result), skip=0)
        self._send_copy_done()
        return nrows

    # --- framing --------------------------------------------------------------

    def _stream(self, chunks: Iterator[bytes], skip: int) -> int:
        """Flush ``chunks`` in >=64 KiB CopyData messages; return the row count.

        Coalescing matters for binary: DuckDB's reader rejects the PGCOPY header
        when it arrives alone in its own tiny CopyData message. ``skip`` is the
        number of non-row chunks the generator emits (header + trailer).
        """
        buf = bytearray()
        emitted = 0
        for chunk in chunks:
            buf += chunk
            emitted += 1
            if len(buf) >= _COPY_FLUSH_BYTES:
                self._send_copy_data(bytes(buf))
                buf.clear()
        if buf:
            self._send_copy_data(bytes(buf))
        return max(0, emitted - skip)

    # --- wire messages --------------------------------------------------------

    def _send_copy_out_response(self, binary: bool, ncols: int) -> None:
        # overall format: 0 = textual (text/csv), 1 = binary; per-column codes match.
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
