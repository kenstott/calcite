"""``fun`` tokens resolve through ``SqlLibrary.parse`` (fun names), not ``valueOf``."""

import sys
import types

import pytest

from pgwire_calcite import function_catalog


class _FakeLibrary:
    def __init__(self, name):
        self.name = name


class _FakeSqlLibrary:
    known = {"postgresql", "spatial", "mssql", "bigquery"}
    parsed = None

    @classmethod
    def parse(cls, csv):
        cls.parsed = csv
        out = []
        for token in csv.split(","):
            if token not in cls.known:
                raise ValueError("unknown library '%s'" % token)
            out.append(_FakeLibrary(token))
        return out


class _FakeEnumSet:
    @staticmethod
    def of(library):
        return library


class _FakeFactory:
    @staticmethod
    def getOperatorTable(libs):
        return libs.name


class _FakeFactoryClass:
    INSTANCE = _FakeFactory


@pytest.fixture
def fake_jpype(monkeypatch):
    classes = {
        "org.apache.calcite.sql.fun.SqlLibrary": _FakeSqlLibrary,
        "org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory": _FakeFactoryClass,
        "java.util.EnumSet": _FakeEnumSet,
    }
    module = types.ModuleType("jpype")
    module.JClass = lambda name: classes[name]
    monkeypatch.setitem(sys.modules, "jpype", module)


def test_bigquery_token_resolves_by_fun_name(fake_jpype):
    tables = function_catalog._extra_libraries("standard,postgresql,spatial,mssql,bigquery")
    assert tables == ["postgresql", "spatial", "mssql", "bigquery"]
    assert _FakeSqlLibrary.parsed == "postgresql,spatial,mssql,bigquery"


def test_mixed_case_tokens_are_lowered(fake_jpype):
    assert function_catalog._extra_libraries("Standard, BigQuery") == ["bigquery"]


def test_standard_only_has_no_extra_libraries(fake_jpype):
    assert function_catalog._extra_libraries("standard") == []


def test_unknown_token_raises(fake_jpype):
    with pytest.raises(ValueError):
        function_catalog._extra_libraries("standard,nosuchlib")
