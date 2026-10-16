#!/usr/bin/env python3
"""Tests for clean-private-jars.py: what is kept, what is deleted, and that nothing is deleted blind."""
import importlib.util
import os
import tempfile
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("clean_private_jars", os.path.join(HERE, "clean-private-jars.py"))
cpj = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cpj)

NOW = 1_800_000_000
DAY = 86400


class ClassifyTest(unittest.TestCase):
    def classify(self, files, live=(), issues=None, hours=24):
        return cpj.classify(files, NOW, hours, {n: "1" for n in live}, issues or {})

    def test_shared_jars_are_never_private(self):
        for name in ("sih-govdata.jar", "sih-govdata-1.42.0-SNAPSHOT.jar", "sih-govdata-0.112.0.jar"):
            self.assertFalse(cpj.is_private(name), name)
        for name in ("sih-govdata-904.jar", "sih-govdata-defect-933.jar", "sih-govdata-sourcing-920.jar",
                     "sih-govdata-feed.jar"):
            self.assertTrue(cpj.is_private(name), name)

    def test_an_unused_old_private_jar_is_deleted(self):
        keep, delete = self.classify({"/h/sih-govdata-defect-1.jar": NOW - 3 * DAY})
        self.assertEqual(["/h/sih-govdata-defect-1.jar"], delete)
        self.assertEqual({}, keep)

    def test_a_shared_jar_is_kept_even_when_old(self):
        keep, delete = self.classify({"/b/sih-govdata.jar": NOW - 30 * DAY})
        self.assertEqual([], delete)
        self.assertEqual("shared jar", keep["/b/sih-govdata.jar"])

    def test_a_jar_just_built_is_kept(self):
        keep, delete = self.classify({"/h/sih-govdata-new.jar": NOW - 3600})
        self.assertEqual([], delete)
        self.assertIn("younger", keep["/h/sih-govdata-new.jar"])

    def test_a_jar_a_live_process_uses_is_kept(self):
        keep, delete = self.classify({"/h/sih-govdata-job.jar": NOW - 5 * DAY}, live=["sih-govdata-job.jar"])
        self.assertEqual([], delete)
        self.assertIn("live process", keep["/h/sih-govdata-job.jar"])

    def test_a_jar_an_open_issue_names_is_kept(self):
        keep, delete = self.classify({"/h/sih-govdata-defect-933.jar": NOW - 5 * DAY},
                                     issues={"sih-govdata-defect-933.jar": 934})
        self.assertEqual([], delete)
        self.assertIn("#934", keep["/h/sih-govdata-defect-933.jar"])

    def test_the_same_name_in_two_directories_is_judged_by_name(self):
        files = {"/h/sih-govdata-x.jar": NOW - 5 * DAY, "/b/sih-govdata-x.jar": NOW - 5 * DAY}
        keep, delete = self.classify(files, issues={"sih-govdata-x.jar": 7})
        self.assertEqual([], delete)
        self.assertEqual(2, len(keep))


class NamesTest(unittest.TestCase):
    def test_names_are_found_in_paths_commands_and_prose(self):
        text = ("run GOVDATA_JAR=/home/kstott/sih-govdata-defect-933.jar force-reprocess.sh and also "
                "govdata/build/libs/sih-govdata-904.jar, then sih-govdata-feed.jar.")
        self.assertEqual({"sih-govdata-defect-933.jar", "sih-govdata-904.jar", "sih-govdata-feed.jar"},
                         cpj.jar_names_in(text))
        self.assertEqual(set(), cpj.jar_names_in(None))


class MainTest(unittest.TestCase):
    def make(self, d, name, age_days):
        path = os.path.join(d, name)
        with open(path, "wb") as f:
            f.write(b"x" * 10)
        t = NOW - age_days * DAY
        os.utime(path, (t, t))
        return path

    def run_main(self, d, args, issues=None, issues_error=None):
        patches = [
            mock.patch.object(cpj.time, "time", return_value=NOW),
            mock.patch.object(cpj, "live_jar_names", return_value={}),
        ]
        if issues_error:
            patches.append(mock.patch.object(cpj, "open_issue_jar_names", side_effect=RuntimeError(issues_error)))
        else:
            patches.append(mock.patch.object(cpj, "open_issue_jar_names", return_value=issues or {}))
        with patches[0], patches[1], patches[2]:
            return cpj.main(args + [d])

    def test_dry_run_deletes_nothing(self):
        with tempfile.TemporaryDirectory() as d:
            jar = self.make(d, "sih-govdata-old.jar", 5)
            self.assertEqual(0, self.run_main(d, []))
            self.assertTrue(os.path.exists(jar))

    def test_delete_removes_only_the_unused_private_jars(self):
        with tempfile.TemporaryDirectory() as d:
            old = self.make(d, "sih-govdata-old.jar", 5)
            named = self.make(d, "sih-govdata-named.jar", 5)
            shared = self.make(d, "sih-govdata.jar", 5)
            fresh = self.make(d, "sih-govdata-fresh.jar", 0)
            self.assertEqual(0, self.run_main(d, ["--delete"], issues={"sih-govdata-named.jar": 12}))
            self.assertFalse(os.path.exists(old))
            for kept in (named, shared, fresh):
                self.assertTrue(os.path.exists(kept), kept)

    def test_when_the_issue_lookup_fails_nothing_is_deleted(self):
        with tempfile.TemporaryDirectory() as d:
            jar = self.make(d, "sih-govdata-old.jar", 5)
            self.assertEqual(2, self.run_main(d, ["--delete"], issues_error="no network"))
            self.assertTrue(os.path.exists(jar))


if __name__ == "__main__":
    unittest.main()
