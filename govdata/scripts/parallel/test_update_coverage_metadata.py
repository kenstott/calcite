#!/usr/bin/env python3
"""Tests for update-coverage-metadata.py measure mode: one per time-column kind, plus the YAML edit."""
import importlib.util
import os
import shutil
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("ucm", os.path.join(HERE, "update-coverage-metadata.py"))
ucm = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ucm)

MAX_YEAR = 2036
HAVE_DUCKDB = shutil.which("duckdb") is not None


def rel(sql_type, values):
    """A self-contained one-column relation named c, so each duckdb invocation is independent."""
    rows = ", ".join("(NULL)" if v is None else f"({v})" for v in values)
    return f"(SELECT CAST(c AS {sql_type}) AS c FROM (VALUES {rows}) t(c))"


def measure(sql_type, values):
    return ucm.measure_relation("", rel(sql_type, values), "c", MAX_YEAR)


class ParseYearLabelTest(unittest.TestCase):
    def test_accepted_forms(self):
        for value, year in (("20190331", 2019), ("2019", 2019), ("2019-03-31", 2019), ("2019-03-31T10:00:00Z", 2019),
                            ("2019-03-31 10:00:00", 2019), ("2019-2020", 2020), ("2019-20", 2020),
                            ("2025-26", 2026), ("1999-00", 2000)):
            self.assertEqual(year, ucm.parse_year_label(value, ucm.FAMILY_LABEL), value)

    def test_year_month_family(self):
        self.assertEqual(2015, ucm.parse_year_label("2015-09", ucm.FAMILY_MONTH))
        self.assertEqual(2011, ucm.parse_year_label("2011-12", ucm.FAMILY_MONTH))
        self.assertEqual(ucm.FAMILY_MONTH, ucm.two_digit_family(["2015-09", "2011-12"]))
        self.assertEqual(ucm.FAMILY_LABEL, ucm.two_digit_family(["2019-20", "2011-12"]))
        self.assertIsNone(ucm.two_digit_family(["2011-12", "2019-05-01"]))

    def test_ambiguous_or_mixed_two_digit_values_fail(self):
        with self.assertRaises(ucm.MeasureError):
            ucm.two_digit_family(["2019-20", "2015-09"])
        with self.assertRaises(ucm.MeasureError):
            ucm.parse_year_label("2011-12")
        with self.assertRaises(ucm.MeasureError):
            ucm.parse_year_label("2015-09", ucm.FAMILY_LABEL)

    def test_unparseable_values_fail_loudly(self):
        for value in ("", " ", "unknown", "2019-21", "2019-2021", "2019-02-30", "20191340",
                      "03/31/2019", "19-20", "2019-20-01x", "N/A"):
            with self.assertRaises(ucm.MeasureError, msg=value):
                ucm.parse_year_label(value, ucm.FAMILY_LABEL)


@unittest.skipUnless(HAVE_DUCKDB, "duckdb CLI not installed")
class MeasureColumnKindTest(unittest.TestCase):
    def test_integer_year_column(self):
        m = measure("INTEGER", [2019, 2019, 2021, None])
        self.assertEqual((2019, 2021, 2, False, [2019, 2021], 4),
                         (m["min_year"], m["max_year"], m["distinct_years"], m["contiguous"],
                          m["years"], m["row_count"]))

    def test_integer_column_that_is_not_a_year_fails(self):
        with self.assertRaises(ucm.MeasureError):
            measure("BIGINT", [20190101])

    def test_implausible_years_fail_for_every_kind(self):
        for sql_type, values in (("DATE", ["'2020-01-01'", "'3011-01-01'"]),
                                 ("VARCHAR", ["'2020-01-01'", "'0013-03-05'"]),
                                 ("INTEGER", [2020, 999])):
            with self.assertRaises(ucm.MeasureError, msg=sql_type) as ctx:
                measure(sql_type, values)
            self.assertIn("implausible", str(ctx.exception))

    def test_a_year_within_the_allowed_future_is_measured(self):
        self.assertEqual(2029, measure("INTEGER", [2025, 2029])["max_year"])

    def test_date_column(self):
        m = measure("DATE", ["'2020-12-31'", "'2021-01-01'", "'2022-06-30'"])
        self.assertEqual((2020, 2022, 3, True), (m["min_year"], m["max_year"], m["distinct_years"], m["contiguous"]))

    def test_timestamp_column(self):
        m = measure("TIMESTAMP", ["'2018-05-01 10:00:00'", "'2018-05-02 11:00:00'", "'2019-01-01 00:00:00'"])
        self.assertEqual((2018, 2019, 2, 3), (m["min_year"], m["max_year"], m["distinct_years"], m["row_count"]))

    def test_timestamp_with_time_zone_is_read_in_utc(self):
        m = measure("TIMESTAMPTZ", ["'2020-12-31 23:30:00+00'", "'2021-01-01 00:30:00+00'"])
        self.assertEqual((2020, 2021), (m["min_year"], m["max_year"]))

    def test_school_year_label_column(self):
        m = measure("VARCHAR", ["'2019-20'", "'2020-21'", "'2021-22'", "'2021-22'"])
        self.assertEqual((2020, 2022, 3, True, 4), (m["min_year"], m["max_year"], m["distinct_years"],
                                                   m["contiguous"], m["row_count"]))

    def test_string_year_and_iso_date_columns(self):
        self.assertEqual((2004, 2006), tuple(measure("VARCHAR", ["'2004'", "'2006'"])[k] for k in ("min_year", "max_year")))
        self.assertEqual((2021, 2023), tuple(measure("VARCHAR", ["'2021-11-03'", "'2023-02-01'"])[k] for k in ("min_year", "max_year")))

    def test_year_month_and_compact_string_columns(self):
        m = measure("VARCHAR", ["'2015-09'", "'2016-11'", "'20170303'", "'2017-05-01'"])
        self.assertEqual((2015, 2017, 3), (m["min_year"], m["max_year"], m["distinct_years"]))

    def test_unparseable_label_aborts_the_table(self):
        with self.assertRaises(ucm.MeasureError) as ctx:
            measure("VARCHAR", ["'2019-20'", "'Unknown'"])
        self.assertIn("Unknown", str(ctx.exception))

    def test_unsupported_column_type_fails(self):
        with self.assertRaises(ucm.MeasureError):
            measure("DOUBLE", [2019.5])

    def test_empty_table_is_reported_as_empty(self):
        self.assertIsNone(ucm.measure_relation("", "(SELECT CAST(NULL AS INTEGER) AS c WHERE false)", "c", MAX_YEAR))

    def test_rows_with_no_time_value_fail(self):
        with self.assertRaises(ucm.MeasureError):
            measure("INTEGER", [None, None])


YAML = """partitionedTables:
  - name: has_pattern
    timeColumn: obs  # note
    pattern: "type=a/*.parquet"
    comment: x
  - name: no_pattern
    timeColumn: school_year  # note
    comment: y
"""


class EditYamlTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, "s-schema.yaml")
        with open(self.path, "w", encoding="utf-8") as f:
            f.write(YAML)

    def tearDown(self):
        shutil.rmtree(self.dir)

    def block(self, years=(2020, 2021)):
        m = ucm.summarize_years({y: 1 for y in years}, len(years), MAX_YEAR)
        return ucm.block_from_measurement(m, "2026-10-17T00:00:00Z")

    def read(self):
        with open(self.path, encoding="utf-8") as f:
            return f.read()

    def test_block_goes_after_pattern(self):
        self.assertEqual("UPDATED", ucm.edit_yaml(self.path, "has_pattern", self.block(), False))
        lines = self.read().split("\n")
        i = lines.index('    pattern: "type=a/*.parquet"')
        self.assertIn(ucm.MARKER, lines[i + 1])
        self.assertEqual("      minYear: 2020", lines[i + 2])

    def test_table_without_pattern_needs_the_time_column_anchor(self):
        with self.assertRaises(ucm.NoAnchor):
            ucm.edit_yaml(self.path, "no_pattern", self.block(), False)
        self.assertEqual("UPDATED", ucm.edit_yaml(self.path, "no_pattern", self.block(), False,
                                                  allow_time_column_anchor=True))
        lines = self.read().split("\n")
        i = lines.index("    timeColumn: school_year  # note")
        self.assertIn(ucm.MARKER, lines[i + 1])

    def test_rewrite_replaces_and_identical_rewrite_is_unchanged(self):
        ucm.edit_yaml(self.path, "has_pattern", self.block(), False)
        self.assertEqual("UNCHANGED", ucm.edit_yaml(self.path, "has_pattern", self.block(), False))
        self.assertEqual("UPDATED", ucm.edit_yaml(self.path, "has_pattern", self.block((2020, 2022)), False))
        text = self.read()
        self.assertEqual(1, text.count("observedCoverage:"))
        self.assertIn("years: [2020, 2022]", text)
        self.assertIn("    comment: x", text)

    def test_other_table_is_untouched(self):
        ucm.edit_yaml(self.path, "has_pattern", self.block(), False)
        self.assertIn("  - name: no_pattern\n    timeColumn: school_year  # note\n    comment: y\n", self.read())

    def test_unknown_table_raises(self):
        with self.assertRaises(KeyError):
            ucm.edit_yaml(self.path, "nope", self.block(), False)


if __name__ == "__main__":
    unittest.main()
