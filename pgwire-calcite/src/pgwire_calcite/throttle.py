# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Login throttling and lockout for pgwire-calcite's credential paths (Phase 3 hardening).

Ported from provisa's ``provisa.auth.throttle`` (REQ-1393) and adapted: pgwire-calcite's
``AuthProvider.authenticate`` returns ``Optional[str]`` (``None`` on a rejected credential)
rather than raising, so the wrapper here counts a ``None`` result as a failure instead of
catching an exception. It is applied uniformly to every credential-carrying path — cleartext
password, the legacy 'simple' provider, SASL SCRAM, and OIDC/bearer — since all of them present
through the same startup-packet username.

Defaults (``PGWIRE_CALCITE_LOGIN_MAX_ATTEMPTS`` / ``_WINDOW_SECONDS`` / ``_LOCKOUT_SECONDS``,
overridable by environment for operators who need a different balance): five attempts is above
any plausible typo count, and a fifteen-minute lockout costs a legitimate user one coffee break
while costing an online guesser three orders of magnitude in throughput.

The store is per-process, in memory, with no external dependency: a brake on guessing, not a
distributed quota.
"""

from __future__ import annotations

import math
import os
import threading
import time
from typing import Callable, Optional, TypeVar

T = TypeVar("T")

_DEFAULT_MAX_ATTEMPTS = 5
_DEFAULT_WINDOW_SECONDS = 300
_DEFAULT_LOCKOUT_SECONDS = 900


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw else default


class LockedOut(PermissionError):
    """Too many recent failures for this subject; the credential was not even examined.

    A distinct type rather than the generic ``None``/failure signal used for a rejected
    credential — a lockout answered the same way as a wrong password would let the client
    keep guessing right up to (and past) the boundary instead of backing off.
    """

    def __init__(self, subject: str, retry_after: int) -> None:
        self.subject = subject
        self.retry_after = retry_after
        super().__init__(f"too many failed authentication attempts; retry in {retry_after} seconds")


class LoginThrottle:
    """Failure counter with lockout, keyed by subject.

    Reached from every pgwire connection thread concurrently, so every mutation is under
    one lock.
    """

    def __init__(
        self,
        *,
        max_attempts: int,
        window_seconds: int,
        lockout_seconds: int,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.max_attempts = max_attempts
        self.window_seconds = window_seconds
        self.lockout_seconds = lockout_seconds
        self._clock = clock
        self._lock = threading.Lock()
        self._failures: dict[str, list[float]] = {}
        self._locked_until: dict[str, float] = {}

    def check(self, subject: str) -> None:
        """Raise :class:`LockedOut` if this subject is locked; return otherwise."""
        now = self._clock()
        with self._lock:
            until = self._locked_until.get(subject)
            if until is None:
                return
            if until <= now:
                del self._locked_until[subject]
                self._failures.pop(subject, None)
                return
            retry_after = max(1, math.ceil(until - now))
        raise LockedOut(subject, retry_after)

    def record_failure(self, subject: str) -> None:
        now = self._clock()
        with self._lock:
            recent = [t for t in self._failures.get(subject, []) if now - t < self.window_seconds]
            recent.append(now)
            self._failures[subject] = recent
            if len(recent) >= self.max_attempts:
                self._locked_until[subject] = now + self.lockout_seconds

    def record_success(self, subject: str) -> None:
        with self._lock:
            self._failures.pop(subject, None)
            self._locked_until.pop(subject, None)


_throttle: Optional[LoginThrottle] = None
_throttle_lock = threading.Lock()


def login_throttle() -> LoginThrottle:
    """The process-wide throttle, built from env on first use."""
    global _throttle
    with _throttle_lock:
        if _throttle is None:
            _throttle = LoginThrottle(
                max_attempts=_int_env("PGWIRE_CALCITE_LOGIN_MAX_ATTEMPTS", _DEFAULT_MAX_ATTEMPTS),
                window_seconds=_int_env(
                    "PGWIRE_CALCITE_LOGIN_WINDOW_SECONDS", _DEFAULT_WINDOW_SECONDS
                ),
                lockout_seconds=_int_env(
                    "PGWIRE_CALCITE_LOGIN_LOCKOUT_SECONDS", _DEFAULT_LOCKOUT_SECONDS
                ),
            )
        return _throttle


def reset_login_throttle() -> None:
    """Drop the installed throttle. For tests, which must not inherit another test's counts."""
    global _throttle
    with _throttle_lock:
        _throttle = None


def subject_key(principal: str) -> str:
    """The lockout key for one authentication attempt.

    pgwire always carries a principal (the startup packet's ``user``), unlike a bearer-only
    HTTP surface, so the key is always ``user:<name>`` — never a credential digest.
    """
    return "user:" + principal


def throttled_auth(
    authenticate: Callable[[], Optional[T]], *, subject: str
) -> Optional[T]:
    """Run one ``authenticate()`` call under the login throttle.

    Raises :class:`LockedOut` before ``authenticate`` runs when the subject is locked out.
    A ``None`` result (rejected credential) counts as a failure; anything else clears the
    subject's history. A :class:`ValueError` — the provider itself is misconfigured or
    unavailable — propagates uncounted: an infrastructure fault must not lock out a
    legitimate user.
    """
    throttle = login_throttle()
    throttle.check(subject)
    result = authenticate()
    if result is None:
        throttle.record_failure(subject)
    else:
        throttle.record_success(subject)
    return result
