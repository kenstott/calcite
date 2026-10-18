# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A DECIMAL column's values are written at the column's declared scale.

An adapter hands back a DECIMAL value at the scale its source wrote it: Salesforce's
``AnnualRevenue`` of a DECIMAL(19,2) column arrives as ``500000`` (scale 0) or ``1.39E9``
(scale -7). arrow-jdbc's decimal vector refuses a value whose scale is not the vector's, and
arrow-jdbc's error report for that failure itself throws on a decimal vector ("Cannot get
simple type for type DECIMAL"), so ``SELECT * FROM "crm"."Account"`` ended the connection.
Each value is rescaled exactly; one that would lose digits still fails.
"""

from __future__ import annotations

import threading
from decimal import Decimal

import pytest

from pgwire_calcite import arrow_bridge


def _java(interface, inst):
    """``inst`` as an object of the Java ``interface``, called by arrow-jdbc in the JVM and by
    this module in Python. The interface's other methods are refused when called."""
    import jpype

    def _unsupported(name):
        def method(self, *args):
            raise NotImplementedError(f"{interface}.{name}")

        return jpype.JOverride(method)

    names = {str(m.getName()) for m in jpype.JClass(interface).class_.getMethods()}
    body = {n: _unsupported(n) for n in names}
    for n in names:
        if hasattr(type(inst), n):
            body[n] = jpype.JOverride(getattr(type(inst), n))
    body["__init__"] = lambda self, d: self.__dict__.update(d)
    cls = jpype.JImplements(interface)(type(type(inst).__name__, (), body))
    return cls(inst.__dict__)


class _Metadata:
    """``ResultSetMetaData`` of one column ``amount DECIMAL(19,2)``, nullable."""

    def getColumnCount(self):
        return 1

    def getColumnLabel(self, _i):
        return "amount"

    def getColumnName(self, _i):
        return "amount"

    def getColumnType(self, _i):
        import jpype

        return jpype.JClass("java.sql.Types").DECIMAL

    def getColumnTypeName(self, _i):
        return "DECIMAL"

    def getPrecision(self, _i):
        return 19

    def getScale(self, _i):
        return 2

    def isNullable(self, _i):
        import jpype

        return jpype.JClass("java.sql.ResultSetMetaData").columnNullable

    def getColumnDisplaySize(self, _i):
        return 21


class _Rows:
    """A ``ResultSet`` over ``values`` (decimal strings, or None for a null)."""

    def __init__(self, values):
        import jpype

        self._values = values
        self._at = -1
        self._null = False
        self._big = jpype.JClass("java.math.BigDecimal")
        self._md = _java("java.sql.ResultSetMetaData", _Metadata())

    def getMetaData(self):
        return self._md

    def next(self):
        self._at += 1
        return self._at < len(self._values)

    def getBigDecimal(self, _column):
        value = self._values[self._at]
        self._null = value is None
        return None if value is None else self._big(value)

    def wasNull(self):
        return self._null

    def close(self):
        pass

    def isClosed(self):
        return False


class _Statement:
    def __init__(self, rows):
        self._rs = _java("java.sql.ResultSet", rows)

    def setFetchSize(self, _n):
        pass

    def executeQuery(self, _sql):
        return self._rs

    def cancel(self):
        pass

    def close(self):
        pass


class _Connection:
    def __init__(self, values):
        self._values = values

    def createStatement(self):
        return _Statement(_Rows(self._values))


def _read(values):
    _, labels, batches = arrow_bridge.stream_ipc_batches(
        _Connection(values), threading.RLock(), "SELECT amount FROM t"
    )
    return labels, [row[0] for row in arrow_bridge.rows_from_ipc(batches)]


def test_a_decimal_is_written_at_the_declared_scale_and_a_null_stays_null(calcite_backend):
    # The JVM is the fixture's; the rows are this test's own result set.
    labels, read = _read(["500000", "1.39E9", "1.25", None, "-0.5", "99999999999999999.99"])
    assert labels == ["DECIMAL"]
    assert read == [
        Decimal("500000.00"),
        Decimal("1390000000.00"),
        Decimal("1.25"),
        None,
        Decimal("-0.50"),
        Decimal("99999999999999999.99"),
    ]
    assert [None if v is None else v.as_tuple().exponent for v in read] == [-2, -2, -2, None, -2, -2]


def test_a_decimal_that_would_lose_digits_still_fails(calcite_backend):
    # arrow-jdbc's report of the failed rescale throws in its turn, so only its outer
    # message reaches here.
    with pytest.raises(Exception, match="next schema root"):
        _read(["1.255"])
