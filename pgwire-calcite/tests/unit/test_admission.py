# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admission control for unfiltered large-table scans."""

from __future__ import annotations

import json

import pytest

from pgwire_calcite.admission import (
    AdmissionPolicy,
    ScanTooLarge,
    TableCoverage,
    load_coverage,
)

BIG = TableCoverage(row_count=5_000_000, partition_columns=("year",))
SMALL = TableCoverage(row_count=100, partition_columns=("year",))


def _policy():
    return AdmissionPolicy(1_000_000, {"econ.big": BIG, "econ.small": SMALL})


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM econ.big WHERE year = 2024",
        "SELECT * FROM econ.big b JOIN econ.small s ON s.year = b.year",
        "SELECT * FROM econ.small",
        "SELECT * FROM econ.big LIMIT 10",
        "SELECT * FROM other.unknown",
        "SELECT 1",
        "SET statement_timeout = 5",
    ],
)
def test_admitted(sql):
    _policy().check(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM econ.big",
        "SELECT * FROM econ.big WHERE value > 3",
        "SELECT count(*) FROM econ.big LIMIT 1",
        "SELECT * FROM econ.big ORDER BY value LIMIT 10",
        "SELECT * FROM econ.small s JOIN econ.big b ON s.id = b.id",
    ],
)
def test_rejected_names_table_and_partition_columns(sql):
    with pytest.raises(ScanTooLarge) as exc:
        _policy().check(sql)
    assert exc.value.sqlstate == "54000"
    assert "econ.big" in str(exc.value) and "year" in str(exc.value)


def test_load_coverage(tmp_path):
    f = tmp_path / "c.json"
    f.write_text(json.dumps({"Econ.Big": {"row_count": 7, "partition_columns": ["Year"]}}))
    assert load_coverage(str(f)) == {"econ.big": TableCoverage(7, ("year",))}
