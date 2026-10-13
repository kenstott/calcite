# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""#518: a model whose schema factory registers no information_schema (servicenow, salesforce)
still answers it: the backend adds Calcite's own when the model has none. A model whose factory
registers one keeps it (the file adapter's: test_phase2_catalog).

The model here is a plain map schema holding one table (a CSV file, through the file adapter's
table factory), so no schema factory runs at all."""

from __future__ import annotations

import json

import pytest

from pgwire_calcite.classpath import ClasspathError, resolve_classpath


@pytest.fixture(scope="module")
def bare_backend(tmp_path_factory):
    try:
        resolve_classpath()
    except ClasspathError as exc:
        pytest.skip(f"Calcite classpath unavailable: {exc}")
    from pgwire_calcite.calcite_backend import CalciteBackend

    directory = tmp_path_factory.mktemp("bare")
    (directory / "nums.csv").write_text("ID:int,NAME:string\n1,one\n2,two\n")
    model = directory / "model.json"
    model.write_text(
        json.dumps(
            {
                "version": "1.0",
                "defaultSchema": "BARE",
                "schemas": [
                    {
                        "name": "BARE",
                        "tables": [
                            {
                                "name": "nums",
                                "type": "custom",
                                "factory": "org.apache.calcite.adapter.file.format.csv."
                                "CsvTableFactory",
                                "operand": {"file": "nums.csv"},
                            }
                        ],
                    }
                ],
            }
        )
    )
    backend = CalciteBackend(model_path=str(model), jvm_args=["-Xmx1g"])
    yield backend
    backend.close()


def test_a_model_without_one_answers_information_schema_tables(bare_backend):
    r = bare_backend.execute_sql(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'BARE'", ""
    )
    assert [tuple(row) for row in r.rows] == [("nums",)]


def test_a_model_without_one_answers_information_schema_columns(bare_backend):
    r = bare_backend.execute_sql(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'BARE' AND table_name = 'nums' ORDER BY ordinal_position",
        "",
    )
    assert len(r.rows) == 2

