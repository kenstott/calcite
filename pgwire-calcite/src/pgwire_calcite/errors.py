# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The text an ErrorResponse carries for an exception, its Java causes included.

A Java exception's message is often a wrapper's alone: Calcite's JDBC adapter reports
``While executing SQL [...] on JDBC sub-schema`` and leaves what the database said to the
exception's cause. ``str()`` of a JPype exception is that top message only, so a client read the
wrapper and never the reason. Each cause's ``toString()`` follows on its own line.
"""

from __future__ import annotations

from typing import Any

#: A cause chain longer than this is cut: two Java exceptions can each be the other's cause.
_MAX_CAUSES = 16


def _java_throwable(exc: BaseException) -> Any:
    """The first Java exception of ``exc`` and the exceptions it was raised from, or None."""
    current: BaseException | None = exc
    while current is not None:
        if hasattr(current, "getCause"):
            return current
        current = current.__cause__
    return None


def error_text(exc: BaseException) -> str:
    """``str(exc)``, then a ``Caused by:`` line for each Java cause whose message it does not hold."""
    text = str(exc)
    throwable = _java_throwable(exc)
    if throwable is None:
        return text
    cause = throwable.getCause()
    for _ in range(_MAX_CAUSES):
        if cause is None:
            break
        message = cause.getMessage()
        if message is None or str(message) not in text:
            text += "\nCaused by: " + str(cause.toString())
        cause = cause.getCause()
    return text
