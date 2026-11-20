# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's advisory locks on pgwire connections (REQ-2255, ported from Provisa's
provisa/pgwire/advisory_locks.py), as src/backend/utils/adt/lockfuncs.c answers them: exclusive
or shared, held by a session or until its transaction ends, re-entrant, each key's locks
conflicting only with another session's.

Migration tools take one before they write; asyncpg's pool runs pg_advisory_unlock_all() on every
connection it releases. The table lives in this server process, as PostgreSQL's lives in one
server's shared memory. A wait ends with the lock, the session's statement_timeout (57014) or the
client going away; there is no deadlock detector, so a deadlock ends with the timeout.

A statement answered here is one advisory function's call with constant keys and nothing else
(``SELECT pg_advisory_lock(42)``); any other statement is not this module's.
"""

# Requirements: REQ-2255

from __future__ import annotations

import re
import threading
import time
from collections import Counter
from typing import Callable, Hashable, Optional

EXCLUSIVE = "ExclusiveLock"
SHARED = "ShareLock"

_SESSION = "session"
_TRANSACTION = "transaction"

#: How often a wait looks for a timeout or a gone client.
_POLL_SECONDS = 0.05

#: name -> (mode, waits, transaction level)
_LOCKS = {
    "pg_advisory_lock": (EXCLUSIVE, True, False),
    "pg_advisory_lock_shared": (SHARED, True, False),
    "pg_advisory_xact_lock": (EXCLUSIVE, True, True),
    "pg_advisory_xact_lock_shared": (SHARED, True, True),
    "pg_try_advisory_lock": (EXCLUSIVE, False, False),
    "pg_try_advisory_lock_shared": (SHARED, False, False),
    "pg_try_advisory_xact_lock": (EXCLUSIVE, False, True),
    "pg_try_advisory_xact_lock_shared": (SHARED, False, True),
}
_UNLOCKS = {"pg_advisory_unlock": EXCLUSIVE, "pg_advisory_unlock_shared": SHARED}

_CALL_RE = re.compile(
    r"^\s*SELECT\s+(?:pg_catalog\s*\.\s*)?(?P<name>pg_(?:try_)?advisory_\w+)\s*"
    r"\(\s*(?P<args>[-+]?\d+(?:\s*,\s*[-+]?\d+)?)?\s*\)"
    r"(?:\s+(?:AS\s+)?(?P<alias>\"[^\"]+\"|\w+))?\s*;?\s*$",
    re.IGNORECASE,
)


class LockTimeout(Exception):
    sqlstate = "57014"  # query_canceled, as statement_timeout ends a wait


class AdvisoryLocks:
    """The server's advisory locks: per key, per holder, how many times at each mode and level."""

    def __init__(self) -> None:
        self._changed = threading.Condition()
        self._held: dict = {}

    def _conflicts(self, key: Hashable, mode: str, holder: object) -> bool:
        for other, counts in self._held.get(key, {}).items():
            if other is holder:
                continue
            if mode == EXCLUSIVE or any(m == EXCLUSIVE for m, _ in counts):
                return True
        return False

    def _grant(self, key: Hashable, mode: str, level: str, holder: object) -> None:
        self._held.setdefault(key, {}).setdefault(holder, Counter())[(mode, level)] += 1

    def try_acquire(self, key: Hashable, mode: str, holder: object, *, transaction: bool) -> bool:
        with self._changed:
            if self._conflicts(key, mode, holder):
                return False
            self._grant(key, mode, _TRANSACTION if transaction else _SESSION, holder)
            return True

    def acquire(
        self, key: Hashable, mode: str, holder: object, *, transaction: bool,
        timeout_ms: int = 0, client_gone: Optional[Callable[[], bool]] = None,
    ) -> None:
        """Wait for the lock; ``timeout_ms`` 0 waits without end, as statement_timeout 0 does."""
        deadline = time.monotonic() + timeout_ms / 1000.0 if timeout_ms else None
        with self._changed:
            while self._conflicts(key, mode, holder):
                if deadline is not None and time.monotonic() >= deadline:
                    raise LockTimeout("canceling statement due to statement timeout")
                if client_gone is not None and client_gone():
                    raise LockTimeout("canceling statement: the client is gone")
                self._changed.wait(_POLL_SECONDS)
            self._grant(key, mode, _TRANSACTION if transaction else _SESSION, holder)

    def _drop(self, key: Hashable, holder: object, entry: tuple) -> None:
        counts = self._held[key][holder]
        counts[entry] -= 1
        if counts[entry] <= 0:
            del counts[entry]
        if not counts:
            del self._held[key][holder]
        if not self._held[key]:
            del self._held[key]

    def release(self, key: Hashable, mode: str, holder: object) -> bool:
        """Release one session-level hold; False where ``holder`` has none."""
        entry = (mode, _SESSION)
        with self._changed:
            if self._held.get(key, {}).get(holder, Counter())[entry] <= 0:
                return False
            self._drop(key, holder, entry)
            self._changed.notify_all()
            return True

    def _release_level(self, holder: object, levels: tuple) -> None:
        with self._changed:
            for key in list(self._held):
                counts = self._held[key].get(holder)
                if counts is None:
                    continue
                for entry in [e for e in counts if e[1] in levels]:
                    del counts[entry]
                if not counts:
                    del self._held[key][holder]
                if not self._held[key]:
                    del self._held[key]
            self._changed.notify_all()

    def end_transaction(self, holder: object) -> None:
        """A transaction ended: its locks go, the session's stay."""
        self._release_level(holder, (_TRANSACTION,))

    def release_all(self, holder: object) -> None:
        """pg_advisory_unlock_all: the session-level locks."""
        self._release_level(holder, (_SESSION,))

    def end_session(self, holder: object) -> None:
        self._release_level(holder, (_SESSION, _TRANSACTION))


#: The server process's locks.
LOCKS = AdvisoryLocks()


def call(sql: str) -> Optional[tuple]:
    """(function name, key, column name) when ``sql`` is one advisory function's call with
    constant keys and nothing else; else None. A key is one bigint, or a pair of ints."""
    m = _CALL_RE.match(sql)
    if m is None:
        return None
    name = m.group("name").lower()
    if name not in _LOCKS and name not in _UNLOCKS and name != "pg_advisory_unlock_all":
        return None
    raw = m.group("args")
    args = tuple(int(a) for a in raw.split(",")) if raw else ()
    if name == "pg_advisory_unlock_all":
        if args:
            return None
    elif len(args) not in (1, 2):
        return None
    alias = m.group("alias")
    column = alias.strip('"') if alias else name
    return name, (len(args),) + args, column


def answer(
    sql: str, holder: object, *, in_transaction: bool, timeout_ms: int = 0,
    client_gone: Optional[Callable[[], bool]] = None,
) -> Optional[tuple]:
    """The (column, value) one advisory function's call answers, having taken or released its
    lock; None when ``sql`` is not such a call. The lock functions return void (''); the try
    and unlock functions a boolean."""
    found = call(sql)
    if found is None:
        return None
    name, key, column = found
    if name == "pg_advisory_unlock_all":
        LOCKS.release_all(holder)
        return column, ""
    if name in _UNLOCKS:
        return column, LOCKS.release(key, _UNLOCKS[name], holder)
    mode, waits, transaction = _LOCKS[name]
    # A transaction-level lock taken outside a transaction ends with its own statement.
    keep_until_txn_end = transaction and in_transaction
    if waits:
        LOCKS.acquire(
            key, mode, holder, transaction=keep_until_txn_end or transaction,
            timeout_ms=timeout_ms, client_gone=client_gone,
        )
        if transaction and not in_transaction:
            LOCKS.end_transaction(holder)
        return column, ""
    got = LOCKS.try_acquire(key, mode, holder, transaction=transaction)
    if got and transaction and not in_transaction:
        LOCKS.end_transaction(holder)
    return column, got
