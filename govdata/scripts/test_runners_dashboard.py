# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE-BSL.txt file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""Tests for runners-dashboard.py. Run: python3 -m unittest govdata/scripts/test_runners_dashboard.py"""

import importlib.util
import json
import os
import subprocess
import tempfile
import unittest
from unittest import mock

_SPEC = importlib.util.spec_from_file_location(
    "runners_dashboard", os.path.join(os.path.dirname(os.path.abspath(__file__)), "runners-dashboard.py"))
rd = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(rd)


def _git(repo, *args):
    subprocess.run(["git", "-C", repo] + list(args), check=True, capture_output=True)


class UnfetchedPrHeadTest(unittest.TestCase):
    def test_unknown_head_keeps_every_row(self):
        with tempfile.TemporaryDirectory() as repo:
            _git(repo, "init", "-b", "main")
            _git(repo, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "--allow-empty", "-m", "base")
            known = subprocess.run(["git", "-C", repo, "rev-parse", "HEAD"], capture_output=True,
                                   text=True, check=True).stdout.strip()
            missing = "4adcc109ce" + "0" * 30
            listing = json.dumps([
                {"number": 646, "headRefName": "release-please", "headRefOid": missing, "state": "OPEN",
                 "title": "t", "body": ""},
                {"number": 700, "headRefName": "fix/defect-1", "headRefOid": known, "state": "OPEN",
                 "title": "t", "body": ""}])
            real_run = rd.run

            def fake_run(cmd, timeout=20, cwd=None):
                if cmd[:3] == ["gh", "pr", "list"]:
                    return listing
                if cmd[:2] == ["gh", "api"]:
                    return "completed success\n"
                return real_run(cmd, timeout=timeout, cwd=cwd)

            with mock.patch.object(rd, "REPO", repo), mock.patch.object(rd, "run", fake_run):
                prs = rd.pull_requests()
            self.assertEqual({"release-please", "fix/defect-1"}, set(prs))
            self.assertEqual("OPEN", prs["release-please"]["state"])
            self.assertTrue(prs["release-please"]["head_missing"])
            self.assertEqual((1, 0, 0), prs["release-please"]["ci"])
            self.assertEqual([], prs["release-please"]["conflicts"])
            self.assertEqual("MERGED", prs["fix/defect-1"]["state"])


if __name__ == "__main__":
    unittest.main()
