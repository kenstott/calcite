# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE-BSL.txt file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""Tests for tmp-hygiene.py. Run: python3 -m unittest govdata/scripts/test_tmp_hygiene.py"""

import argparse
import contextlib
import fcntl
import importlib.util
import io
import os
import tempfile
import time
import unittest

_SPEC = importlib.util.spec_from_file_location(
    "tmp_hygiene", os.path.join(os.path.dirname(os.path.abspath(__file__)), "tmp-hygiene.py"))
th = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(th)

HOUR = 3600


class TmpHygieneTest(unittest.TestCase):

    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.root = os.path.realpath(self._dir.name)
        self.state = os.path.join(self.root, "state-outside-scan")
        os.mkdir(self.state)
        th.cmd_init(argparse.Namespace(root=self.root))

    def tearDown(self):
        self._dir.cleanup()

    def make(self, name, hours_old=0.0, size=10, sub=None):
        d = self.root if sub is None else os.path.join(self.root, sub)
        os.makedirs(d, exist_ok=True)
        path = os.path.join(d, name)
        with open(path, "wb") as f:
            f.write(b"x" * size)
        t = time.time() - hours_old * HOUR
        os.utime(path, (t, t))
        return path

    def reap(self, **kw):
        args = argparse.Namespace(root=self.root, min_age_hours=6.0, dry_run=False, boot=False)
        for k, v in kw.items():
            setattr(args, k, v)
        with contextlib.redirect_stdout(io.StringIO()) as out:
            th.cmd_reap(args)
        return out.getvalue()

    def test_old_unopened_allowlisted_file_is_deleted(self):
        p = self.make("http-source-part-123.tmp", hours_old=10)
        self.reap()
        self.assertFalse(os.path.exists(p))

    def test_young_file_is_kept(self):
        p = self.make("http-source-part-123.tmp", hours_old=1)
        self.reap()
        self.assertTrue(os.path.exists(p))

    def test_a_file_a_process_has_open_is_kept_however_old(self):
        p = self.make("storage-openstream-9.tmp", hours_old=500)
        with open(p, "rb"):
            self.reap()
            self.assertTrue(os.path.exists(p))
        self.reap()
        self.assertFalse(os.path.exists(p), "once closed it is an orphan")

    def test_names_off_the_allowlist_are_never_deleted(self):
        keep = [self.make(n, hours_old=900) for n in (
            "usitc-slice-plan.json", "usitc-dataweb-api.lock", "calcite-ratelimit-sec.gov.slot",
            "worker-sec.123.pid", "http-source-part-1.json", "something-else.tmp")]
        keep.append(os.path.join(self.root, th.MARKER))
        self.reap()
        for p in keep:
            self.assertTrue(os.path.exists(p), p)

    def test_it_never_recurses_into_directories(self):
        inner = self.make("http-source-part-5.tmp", hours_old=900, sub="runs")
        d = os.path.join(self.root, "http-source-part-6.tmp")
        os.mkdir(d)
        self.reap()
        self.assertTrue(os.path.exists(inner))
        self.assertTrue(os.path.isdir(d))

    def test_a_symlink_is_not_followed_or_removed(self):
        with tempfile.TemporaryDirectory() as other:
            target = os.path.join(other, "precious.bin")
            with open(target, "w") as f:
                f.write("keep")
            link = os.path.join(self.root, "provider-raw-cache-1.bin")
            os.symlink(target, link)
            t = time.time() - 900 * HOUR
            os.utime(link, (t, t), follow_symlinks=False)
            self.reap()
            self.assertTrue(os.path.exists(target))
            self.assertTrue(os.path.islink(link))

    def test_it_refuses_a_directory_without_the_marker(self):
        os.unlink(os.path.join(self.root, th.MARKER))
        p = self.make("http-source-part-1.tmp", hours_old=900)
        with self.assertRaises(SystemExit):
            self.reap()
        self.assertTrue(os.path.exists(p))

    def test_dry_run_deletes_nothing_but_reports(self):
        p = self.make("hmda-2020-1.csv", hours_old=900, size=2048)
        out = self.reap(dry_run=True)
        self.assertTrue(os.path.exists(p))
        self.assertIn("DRY RUN", out)
        self.assertIn("would delete 1 file", out)

    def test_native_libraries_need_a_full_day(self):
        young = self.make("libduckdb_java123.so", hours_old=7)
        old = self.make("libduckdb_java456.so", hours_old=25)
        self.reap()
        self.assertTrue(os.path.exists(young))
        self.assertFalse(os.path.exists(old))

    def test_boot_mode_removes_what_predates_the_boot_and_keeps_what_follows_it(self):
        before = self.make("http-source-part-1.tmp", hours_old=2)
        after = self.make("http-source-part-2.tmp", hours_old=0.1)
        original = th.boot_epoch
        th.boot_epoch = lambda: time.time() - 1 * HOUR
        try:
            self.reap(boot=True)
        finally:
            th.boot_epoch = original
        self.assertFalse(os.path.exists(before), "older than the boot: cannot be in use")
        self.assertTrue(os.path.exists(after), "newer than the boot: belongs to this boot")

    def test_boot_mode_still_keeps_an_open_file(self):
        p = self.make("http-source-part-1.tmp", hours_old=2)
        original = th.boot_epoch
        th.boot_epoch = lambda: time.time() - 1 * HOUR
        try:
            with open(p, "rb"):
                self.reap(boot=True)
                self.assertTrue(os.path.exists(p))
        finally:
            th.boot_epoch = original

    def test_a_second_reaper_does_nothing_while_one_holds_the_lock(self):
        p = self.make("http-source-part-1.tmp", hours_old=900)
        fd = os.open(os.path.join(self.root, th.LOCK), os.O_RDWR | os.O_CREAT, 0o644)
        fcntl.flock(fd, fcntl.LOCK_EX)
        try:
            out = self.reap()
        finally:
            os.close(fd)
        self.assertTrue(os.path.exists(p))
        self.assertIn("another reaper holds the lock", out)

    def test_sample_and_report_give_the_peak_working_set(self):
        self.make("http-source-part-1.tmp", hours_old=0.5, size=3 * 1024 * 1024)   # recent
        self.make("http-source-part-2.tmp", hours_old=50, size=5 * 1024 * 1024)    # orphan
        held = self.make("http-source-part-3.tmp", hours_old=0.5, size=7 * 1024 * 1024)
        args = argparse.Namespace(root=self.root, state_dir=self.state, min_age_hours=6.0)
        with open(held, "rb"):
            th.cmd_sample(args)
        snap = th.usage_snapshot(self.root, 6.0)
        self.assertEqual(snap["orphan_gb"] * 1024, 5.0)
        with contextlib.redirect_stdout(io.StringIO()) as out:
            th.cmd_report(args)
        self.assertIn("overall peak working set", out.getvalue())
        with open(os.path.join(self.state, "usage.csv")) as f:
            lines = f.read().splitlines()
        self.assertEqual(lines[0].split(",")[0], "iso")
        self.assertEqual(len(lines), 2)


if __name__ == "__main__":
    unittest.main()
