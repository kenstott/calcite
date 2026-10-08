"""A column the adapter declares NOT NULL is still read as nullable.

arrow-jdbc picks a column's consumer from the nullability the JDBC metadata declares, and its
consumer for a NOT NULL column does not check for null. An adapter that declares a column
NOT NULL and then returns a null in it (seen on the cloud inventory adapter: ``SELECT *`` on
any table with rows) threw ``NullPointerException: Cannot invoke String.getBytes(Charset)
because "value" is null`` and ended the connection. The declaration is the adapter's claim;
the reader never relies on it.
"""

from __future__ import annotations

from pgwire_calcite import arrow_bridge


class _Type:
    def getTypeID(self):
        return "Utf8"


class _Utils:
    def __init__(self):
        self.calls = []

    def getConsumer(self, arrow_type, column_index, nullable, vector, config):
        self.calls.append((column_index, nullable))
        return "consumer"


def test_a_column_declared_not_null_gets_the_null_checking_consumer(calcite_backend, monkeypatch):
    # The factory implements a Java interface, so it is made with the JVM up (the fixture).
    monkeypatch.setattr(arrow_bridge, "_FACTORY_CACHE", None)
    utils = _Utils()
    factory = arrow_bridge._consumer_factory({"JdbcToArrowUtils": utils})

    assert factory.apply(_Type(), 1, False, None, None) == "consumer"
    assert factory.apply(_Type(), 2, True, None, None) == "consumer"
    assert utils.calls == [(1, True), (2, True)]
