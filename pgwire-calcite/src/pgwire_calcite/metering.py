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

The API key is read ONCE from ``ASKAMERICA_API_KEY``, inherited from the parent
askamerica-mcp Java process's environment when it spawns this server
(``PgwireGovDataConnector.spawnIfPossible()`` uses a plain ``ProcessBuilder``,
which inherits the parent's environment by default -- no explicit forwarding
needed). This server is single-tenant per machine (one shared local instance
per install, per kenstott/calcite#364), so every connection's usage belongs to
the same key; there is no per-connection auth to thread through here.

With no API key configured (e.g. a local dev run), every function here is a
no-op -- metering never blocks or fails a query when unconfigured.
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
_quota_cache: dict | None = None  # {"key": str, "state": str, "expires": float}


def _api_base() -> str:
    return os.environ.get("ASKAMERICA_API_URL", "https://api.askamerica.ai")


def api_key() -> str | None:
    key = os.environ.get("ASKAMERICA_API_KEY")
    return key if key else None


class QuotaExceeded(PermissionError):
    """Raised when the configured API key is out of monthly egress quota."""


class QuotaAuthError(PermissionError):
    """Raised when the configured API key is invalid, expired, or revoked."""


def enforce_quota() -> None:
    """Raise if the process's API key is over quota or rejected.

    A no-op when no key is configured (matches UsageMetering.wrap()'s "no key ->
    unmetered" behavior on the embedded-mode path). Called once per query, same
    cadence as the Java side's StatementHandler -- cheap thanks to the 60s cache.
    """
    key = api_key()
    if not key:
        return
    state = _quota_state(key)
    if state == "block_quota":
        raise QuotaExceeded(
            "AskAmerica monthly quota exceeded. Upgrade at https://askamerica.ai/upgrade")
    if state == "block_auth":
        raise QuotaAuthError(
            "AskAmerica API key is invalid, expired, or revoked. See https://askamerica.ai")


def _quota_state(key: str) -> str:
    global _quota_cache
    now = time.monotonic()
    with _lock:
        cached = _quota_cache
        if cached is not None and cached["key"] == key and cached["expires"] > now:
            return cached["state"]
    state = _fetch_quota_state(key)
    # Never cache an auth block -- a brief 401 from KV propagation lag (e.g. a
    # freshly minted key) self-heals on the next query rather than locking the
    # user out for the whole TTL, matching UsageMetering.java's quotaState().
    if state != "block_auth":
        with _lock:
            _quota_cache = {"key": key, "state": state, "expires": now + _QUOTA_TTL_SECONDS}
    return state


def _fetch_quota_state(key: str) -> str:
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
        return "allow"  # fail open -- a transient infra error must not hard-block
    except Exception:
        return "allow"  # fail open on network/timeout
    try:
        remaining = json.loads(body).get("remaining_bytes")
    except Exception:
        return "allow"
    return "block_quota" if (remaining is not None and remaining <= 0) else "allow"


def report_usage_async(sql: str, row_count: int, egress_bytes: int, duration_ms: int) -> None:
    """Fire-and-forget usage report. No-op with no key configured or zero rows."""
    key = api_key()
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

    def finish(self, sql: str, duration_ms: int) -> None:
        if self._row_count <= 0:
            return
        if self._row_count <= _SAMPLE_ROWS:
            est = self._sample_bytes
        else:
            est = int(self._sample_bytes / _SAMPLE_ROWS * self._row_count)
        egress = max(est, self._row_count * self._col_count)
        report_usage_async(sql, self._row_count, egress, duration_ms)
