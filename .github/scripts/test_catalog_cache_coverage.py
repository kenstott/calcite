#!/usr/bin/env python3
"""Tests pgwire-calcite/scripts/check_catalog_cache_coverage.py, the govdata bundle step's check
that the shipped catalog cache holds every table a schema YAML declares (govdata-ops#940), save the
declared tables the warehouse does not hold yet, which pgwire-govdata/unmaterialized-tables.txt
names.

Covers: the shipped cache, list and YAMLs pass together; a declared table neither cached nor
listed fails; a listed table passes; a listed table the cache holds fails; a listed table no YAML
declares fails; a malformed list line fails.

Run: python3 -m pytest -q .github/scripts/test_catalog_cache_coverage.py
"""
import glob
import os
import pickle
import subprocess
import sys
import tempfile
import textwrap
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPT = os.path.join(ROOT, "pgwire-calcite", "scripts", "check_catalog_cache_coverage.py")
SRC = os.path.join(ROOT, "pgwire-calcite", "src")
GOVDATA = os.path.join(ROOT, "pgwire-govdata")

sys.path.insert(0, SRC)
from pgwire_calcite.compiler.sql_gen import CompilationContext, TableMeta  # noqa: E402

YAML = textwrap.dedent("""\
    schemaName: ${HEALTH_SCHEMA:health}
    tables:
      - name: cached_table
    partitionedTables:
      - name: unbuilt_table
    views:
      - name: cached_view
    """)


def run(cache, listing, yamls):
    env = dict(os.environ, PYTHONPATH=SRC)
    return subprocess.run([sys.executable, SCRIPT, cache, listing, *yamls],
                          capture_output=True, text=True, env=env)


class CheckCatalogCacheCoverage(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = self.tmp.name
        self.yaml = self._write("health-schema.yaml", YAML)

    def tearDown(self):
        self.tmp.cleanup()

    def _write(self, name, text):
        path = os.path.join(self.dir, name)
        with open(path, "w") as f:
            f.write(text)
        return path

    def _cache(self, *pairs):
        ctx = CompilationContext()
        for i, (schema, table) in enumerate(pairs, 1):
            tm = TableMeta(table_id=i, catalog_name="govdata", schema_name=schema,
                           table_name=table, type_name=f"{schema}.{table}")
            ctx.tables[tm.type_name] = tm
        path = os.path.join(self.dir, "catalog-cache-test.pkl")
        with open(path, "wb") as f:
            pickle.dump((1, ctx, {}), f)
        return path

    def test_the_shipped_cache_list_and_yamls_pass_together(self):
        caches = glob.glob(os.path.join(GOVDATA, "catalog-cache-*.pkl"))
        self.assertEqual(len(caches), 1, caches)
        yamls = sorted(glob.glob(os.path.join(ROOT, "govdata", "src", "main", "resources",
                                              "*", "*-schema.yaml")))
        self.assertTrue(yamls)
        r = run(caches[0], os.path.join(GOVDATA, "unmaterialized-tables.txt"), yamls)
        self.assertEqual(r.returncode, 0, r.stderr)

    def test_a_declared_table_neither_cached_nor_listed_fails(self):
        cache = self._cache(("health", "cached_table"), ("health", "cached_view"))
        listing = self._write("unmaterialized.txt", "# nothing listed\n")
        r = run(cache, listing, [self.yaml])
        self.assertEqual(r.returncode, 1)
        self.assertIn("lacks 1 declared", r.stderr)
        self.assertIn("health.unbuilt_table", r.stderr)

    def test_a_listed_unmaterialized_table_passes(self):
        cache = self._cache(("health", "cached_table"), ("health", "cached_view"))
        listing = self._write("unmaterialized.txt",
                              "# verified absent\n\nhealth.unbuilt_table  # no ETL yet\n")
        r = run(cache, listing, [self.yaml])
        self.assertEqual(r.returncode, 0, r.stderr)

    def test_a_listed_table_the_cache_holds_fails(self):
        cache = self._cache(("health", "cached_table"), ("health", "cached_view"),
                            ("health", "unbuilt_table"))
        listing = self._write("unmaterialized.txt", "health.unbuilt_table\n")
        r = run(cache, listing, [self.yaml])
        self.assertEqual(r.returncode, 1)
        self.assertIn("lists 1 table(s) the cache holds", r.stderr)
        self.assertIn("health.unbuilt_table", r.stderr)

    def test_a_listed_table_no_yaml_declares_fails(self):
        cache = self._cache(("health", "cached_table"), ("health", "cached_view"))
        listing = self._write("unmaterialized.txt",
                              "health.unbuilt_table\nhealth.dropped_table\n")
        r = run(cache, listing, [self.yaml])
        self.assertEqual(r.returncode, 1)
        self.assertIn("lists 1 table(s) no schema YAML declares", r.stderr)
        self.assertIn("health.dropped_table", r.stderr)

    def test_a_malformed_list_line_fails(self):
        cache = self._cache(("health", "cached_table"), ("health", "cached_view"))
        listing = self._write("unmaterialized.txt", "unbuilt_table\n")
        r = run(cache, listing, [self.yaml])
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("expected schema.table", r.stderr)


if __name__ == "__main__":
    unittest.main()
