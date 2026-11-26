#!/usr/bin/env python3
"""Tests pgwire-calcite/scripts/check_catalog_cache_coverage.py, the govdata bundle step's check
that the shipped catalog cache holds every table a schema YAML declares (govdata-ops#940), save the
declared tables the warehouse does not hold yet, which have a file each in
pgwire-govdata/unmaterialized/.

Covers: the shipped cache, directory and YAMLs pass together; a declared table neither cached nor
listed fails; a listed table passes; a listed table the cache holds fails; a listed table no YAML
declares fails; a misnamed entry and a missing directory fail; two branches adding different tables
touch disjoint files and merge into a passing tree.

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

    def _listing(self, *names):
        path = os.path.join(self.dir, "unmaterialized")
        os.makedirs(path)
        for name in ("README.md", *names):
            self._write(os.path.join(path, name), "")
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
        r = run(caches[0], os.path.join(GOVDATA, "unmaterialized"), yamls)
        self.assertEqual(r.returncode, 0, r.stderr)

    def test_a_declared_table_neither_cached_nor_listed_fails(self):
        cache = self._cache(("health", "cached_table"), ("health", "cached_view"))
        listing = self._listing()
        r = run(cache, listing, [self.yaml])
        self.assertEqual(r.returncode, 1)
        self.assertIn("lacks 1 declared", r.stderr)
        self.assertIn("health.unbuilt_table", r.stderr)

    def test_a_listed_unmaterialized_table_passes(self):
        cache = self._cache(("health", "cached_table"), ("health", "cached_view"))
        listing = self._listing("health.unbuilt_table")
        r = run(cache, listing, [self.yaml])
        self.assertEqual(r.returncode, 0, r.stderr)

    def test_a_listed_table_the_cache_holds_fails(self):
        cache = self._cache(("health", "cached_table"), ("health", "cached_view"),
                            ("health", "unbuilt_table"))
        listing = self._listing("health.unbuilt_table")
        r = run(cache, listing, [self.yaml])
        self.assertEqual(r.returncode, 1)
        self.assertIn("lists 1 table(s) the cache holds", r.stderr)
        self.assertIn("health.unbuilt_table", r.stderr)

    def test_a_listed_table_no_yaml_declares_fails(self):
        cache = self._cache(("health", "cached_table"), ("health", "cached_view"))
        listing = self._listing("health.unbuilt_table", "health.dropped_table")
        r = run(cache, listing, [self.yaml])
        self.assertEqual(r.returncode, 1)
        self.assertIn("lists 1 table(s) no schema YAML declares", r.stderr)
        self.assertIn("health.dropped_table", r.stderr)

    def test_a_misnamed_entry_fails(self):
        cache = self._cache(("health", "cached_table"), ("health", "cached_view"))
        listing = self._listing("unbuilt_table")
        r = run(cache, listing, [self.yaml])
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("expected a file named schema.table", r.stderr)

    def test_a_missing_directory_fails(self):
        cache = self._cache(("health", "cached_table"), ("health", "cached_view"),
                            ("health", "unbuilt_table"))
        r = run(cache, os.path.join(self.dir, "unmaterialized"), [self.yaml])
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("FileNotFoundError", r.stderr)

    def test_branches_adding_different_tables_touch_disjoint_files_and_merge(self):
        repo = os.path.join(self.dir, "repo")
        listing = os.path.join(repo, "unmaterialized")
        os.makedirs(listing)
        env = dict(os.environ, GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1",
                   GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@example.invalid",
                   GIT_COMMITTER_NAME="t", GIT_COMMITTER_EMAIL="t@example.invalid")

        def git(*args):
            return subprocess.run(["git", "-C", repo, *args], check=True, env=env,
                                  capture_output=True, text=True).stdout

        def commit(branch, schema):
            self._write(os.path.join(repo, f"{schema}-schema.yaml"),
                        f"schemaName: {schema}\ntables:\n  - name: new_table\n")
            self._write(os.path.join(listing, f"{schema}.new_table"), "")
            git("add", "-A")
            git("commit", "-q", "-m", branch)
            return set(git("diff", "--name-only", "base", branch).split())

        git("init", "-q", "-b", "base")
        self._write(os.path.join(listing, "README.md"), "entries\n")
        git("add", "-A")
        git("commit", "-q", "-m", "base")
        git("checkout", "-q", "-b", "first", "base")
        first = commit("first", "econ")
        git("checkout", "-q", "-b", "second", "base")
        second = commit("second", "health")

        self.assertTrue(first and second)
        self.assertEqual(first & second, set())
        git("merge", "-q", "--no-edit", "first")
        yamls = sorted(glob.glob(os.path.join(repo, "*-schema.yaml")))
        self.assertEqual(len(yamls), 2)
        r = run(self._cache(), listing, yamls)
        self.assertEqual(r.returncode, 0, r.stderr)


if __name__ == "__main__":
    unittest.main()
