# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Server-side usage metering and quota enforcement for pgwire-govdata.

Mirrors askamerica-engine's ``UsageMetering.java`` (same endpoints, same JSON
schema, same fail-open/fail-closed semantics) -- necessary because pgwire mode
moves compute server-side, and ``UsageMetering.java`` never sees these queries:
``McpServer.getSchemaConnection()`` returns ``PgwireGovDataConnector``'s shared
connection directly whenever pgwire is enabled (the default since 0.89.0),
bypassing the Java-side Connection proxy ``UsageMetering.wrap()`` relies on
entirely. Confirmed live: a real free-tier key showed ``used_bytes: 0`` after
real query traffic, because nothing was tracking it at all.

The API key is the CONNECTION's: every connection to pgwire-govdata presents its
AskAmerica API key as its PostgreSQL password (``auth.AskAmericaKeyProvider``), the server
keeps it with that connection, and quota is checked and usage reported under it -- per
connection, never from this process's environment. Two connections with two keys are metered
separately. The key in the server's environment, if any, belongs to whoever started the
server and is used only to fetch and rotate the store credentials.

A connection that carries no key (another pgwire-* adapter, or a server started with key
sign-in switched off for a test) is not metered: every function here is a no-op for it.

No function here logs, prints or raises a key or any part of one.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
import urllib.error
import urllib.request
import uuid

log = logging.getLogger("pgwire_calcite.metering")

_QUOTA_TTL_SECONDS = 60.0
_HTTP_TIMEOUT_SECONDS = 5
_SAMPLE_ROWS = 100
_QUERY_TEXT_MAX = 1024
# Required: Cloudflare's bot-fight-mode blocks urllib's default "Python-urllib/x.y"
# User-Agent outright (HTTP 403, Cloudflare error 1010) before the request ever
# reaches the API worker -- confirmed live, and without this every quota check
# would be misread as an invalid/revoked key (block_auth is what a real 401/403
# from the API itself looks like too) and block every query. Any non-default UA
# clears it; this one is just descriptive.
_USER_AGENT = "pgwire-govdata-metering/1.0"

_FROM_TABLE_RE = re.compile(r"\bFROM\s+([A-Za-z_][\w.]*)", re.IGNORECASE)

_lock = threading.Lock()
#: key -> {"state": str, "expires": float}. One entry per key seen in the last TTL.
_quota_cache: dict = {}


def _api_base() -> str:
    return os.environ.get("ASKAMERICA_API_URL", "https://api.askamerica.ai")


class QuotaExceeded(PermissionError):
    """Raised when the connection's API key is out of monthly egress quota."""

    sqlstate = "53400"


class QuotaAuthError(PermissionError):
    """Raised when the connection's API key is invalid, expired, or revoked."""

    sqlstate = "28P01"


class KeyServiceUnavailable(Exception):
    """The key could not be checked at sign-in because the key service did not answer.

    Sign-in fails closed: a server that admitted keys it could not check would admit any
    string. SQLSTATE 08006 (connection_failure) tells the client it did nothing wrong and
    may try again.
    """

    sqlstate = "08006"


def enforce_quota(key: str | None) -> None:
    """Raise if ``key``, the connection's API key, is over quota or rejected.

    A no-op for a connection with no key. Called once per query; cheap thanks to the 60s
    cache.
    """
    if not key:
        return
    state = _quota_state(key)
    if state == "block_quota":
        raise QuotaExceeded(
            "AskAmerica monthly quota exceeded. Upgrade at https://askamerica.ai/upgrade")
    if state == "block_auth":
        raise QuotaAuthError(
            "AskAmerica API key is invalid, expired, or revoked. See https://askamerica.ai")


def verify_key(key: str) -> bool:
    """Sign-in check: True when the key service accepts ``key``, False when it refuses it.

    Raises :class:`KeyServiceUnavailable` when the service cannot be asked. A key that is
    merely out of quota is accepted here: the connection is admitted and its statements are
    refused by :func:`enforce_quota`. An answer cached by a recent statement or sign-in under
    the same key is reused.
    """
    now = time.monotonic()
    with _lock:
        cached = _quota_cache.get(key)
        if cached is not None and cached["expires"] > now:
            return True
    state = _fetch_quota_state(key, fail_open=False)
    if state == "block_auth":
        return False
    _remember(key, state, now)
    return True


def _remember(key: str, state: str, now: float) -> None:
    with _lock:
        for stale in [k for k, v in _quota_cache.items() if v["expires"] <= now]:
            del _quota_cache[stale]
        _quota_cache[key] = {"state": state, "expires": now + _QUOTA_TTL_SECONDS}


def _quota_state(key: str) -> str:
    now = time.monotonic()
    with _lock:
        cached = _quota_cache.get(key)
        if cached is not None and cached["expires"] > now:
            return cached["state"]
    state = _fetch_quota_state(key)
    # Never cache an auth block -- a brief 401 from KV propagation lag (e.g. a
    # freshly minted key) self-heals on the next query rather than locking the
    # user out for the whole TTL.
    if state != "block_auth":
        _remember(key, state, now)
    return state


def _fetch_quota_state(key: str, fail_open: bool = True) -> str:
    """``allow``, ``block_quota`` or ``block_auth`` for ``key``.

    When the service cannot be asked or answers something else: ``allow`` for a statement
    (``fail_open``; a transient fault must not hard-block a signed-in reader), and
    :class:`KeyServiceUnavailable` at sign-in. The exception text never contains the key:
    the key travels in a header, and only the status or the error's class is named.
    """
    req = urllib.request.Request(
        f"{_api_base()}/v1/quota",
        headers={"X-API-Key": key, "User-Agent": _USER_AGENT},
        method="GET")
    try:
        with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT_SECONDS) as resp:
            body = resp.read()
    except urllib.error.HTTPError as e:
        if e.code in (401, 403):
            return "block_auth"  # fail closed -- an explicit rejection
        if not fail_open:
            raise KeyServiceUnavailable(
                f"the AskAmerica key service answered HTTP {e.code}; the API key could not "
                "be checked. Try again shortly.") from None
        return "allow"  # fail open -- a transient infra error must not hard-block
    except Exception as e:
        if not fail_open:
            raise KeyServiceUnavailable(
                f"the AskAmerica key service could not be reached ({type(e).__name__}); the "
                "API key could not be checked. Try again shortly.") from None
        return "allow"  # fail open on network/timeout
    try:
        remaining = json.loads(body).get("remaining_bytes")
    except Exception:
        if not fail_open:
            raise KeyServiceUnavailable(
                "the AskAmerica key service gave an answer that could not be read; the API "
                "key could not be checked. Try again shortly.") from None
        return "allow"
    return "block_quota" if (remaining is not None and remaining <= 0) else "allow"


def report_usage_async(
    key: str | None, sql: str, row_count: int, egress_bytes: int, duration_ms: int
) -> None:
    """Fire-and-forget usage report under ``key``. No-op with no key or zero rows."""
    if not key or row_count <= 0:
        return
    t = threading.Thread(
        target=_post_usage, args=(key, sql, row_count, egress_bytes, duration_ms), daemon=True)
    t.start()


def _post_usage(key: str, sql: str, row_count: int, egress_bytes: int, duration_ms: int) -> None:
    m = _FROM_TABLE_RE.search(sql)
    table = m.group(1) if m else None
    body = json.dumps({
        "query_id": str(uuid.uuid4()),
        "table": table,
        "planned_bytes": egress_bytes,
        "actual_bytes": egress_bytes,
        "row_count": row_count,
        "duration_ms": duration_ms,
        "query_text": sql[:_QUERY_TEXT_MAX],
    }).encode("utf-8")
    req = urllib.request.Request(
        f"{_api_base()}/v1/metering/usage",
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "X-API-Key": key,
            "User-Agent": _USER_AGENT,
        },
    )
    try:
        urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT_SECONDS).read()
    except Exception:
        pass  # best-effort -- metering never surfaces an error to the caller


class EgressSampler:
    """Accumulates a byte estimate as rows stream by, mirroring UsageMetering
    .java's ResultSetHandler: UTF-8 size of the first ``_SAMPLE_ROWS`` rows,
    scaled by total row count, floored at one byte per cell so a sampling
    hiccup never drops a non-empty result from metering entirely.
    """

    __slots__ = ("_col_count", "_sample_bytes", "_row_count")

    def __init__(self, col_count: int):
        self._col_count = max(col_count, 1)
        self._sample_bytes = 0
        self._row_count = 0

    def observe(self, row: list) -> None:
        self._row_count += 1
        if self._row_count <= _SAMPLE_ROWS:
            try:
                self._sample_bytes += sum(
                    len(str(v).encode("utf-8")) for v in row if v is not None)
            except Exception:
                pass  # best-effort sampling -- never let this break a real result

    def finish(self, key: str | None, sql: str, duration_ms: int) -> None:
        """Reports what was streamed under ``key``, the key of the connection it ran on."""
        if self._row_count <= 0:
            return
        if self._row_count <= _SAMPLE_ROWS:
            est = self._sample_bytes
        else:
            est = int(self._sample_bytes / _SAMPLE_ROWS * self._row_count)
        egress = max(est, self._row_count * self._col_count)
        report_usage_async(key, sql, self._row_count, egress, duration_ms)
