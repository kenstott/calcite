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


TLS_ERROR = ('gh issue view 7: Post "https://api.github.com/graphql": '
             'net/http: TLS handshake timeout')


def _issue(number, title, labels, body=""):
    return {"number": number, "title": title, "state": "OPEN", "body": body, "closedAt": None,
            "updatedAt": "2026-01-01T00:00:00Z", "labels": [{"name": n} for n in labels]}


class TransientFailureTest(unittest.TestCase):
    """A failed read leaves a frame that says "unreadable", never a result."""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.issue_list = [_issue(7, "stuck thing", ["status:blocked", "type:defect"])]
        patches = {
            "_gh_cache": {"at": 0, "data": None},
            "_GH_CACHE_FILE": os.path.join(tmp.name, "issues.json"),
            "_COMMENT_CACHE_FILE": os.path.join(tmp.name, "comments.json"),
            "USE_COLOR": False,
            "daemon": lambda: ("active", "1", 1),
            "agents": lambda: [],
            "resources": lambda: (1, 1, 0, 0),
            "release_state": lambda: ("engine-v1", 0, 0, 0, "a", "a"),
            "pool": lambda: [],
            "rehearsals": lambda: {},
            "other_jobs": lambda dq: [],
            "window": lambda: (None, [], []),
            "branches": lambda: [],
            "merged_commits": lambda: [],
            "journal": lambda: [],
        }
        for name, value in patches.items():
            p = mock.patch.object(rd, name, value)
            p.start()
            self.addCleanup(p.stop)

    def _gh(self, failing):
        def fake_run(cmd, timeout=20, cwd=None):
            if cmd[:3] in failing:
                raise RuntimeError(TLS_ERROR)
            if cmd[:3] == ["gh", "issue", "list"]:
                return json.dumps(self.issue_list if "open" in cmd else [])
            if cmd[:3] == ["gh", "issue", "view"]:
                return json.dumps({"comments": []})
            if cmd[:3] == ["gh", "pr", "list"]:
                return "[]"
            raise AssertionError(cmd)
        return mock.patch.object(rd, "run", fake_run)

    def test_comment_read_failure_marks_the_row_and_recovers(self):
        with self._gh([["gh", "issue", "view"]]):
            frame = rd.render(ttl=60)
        self.assertIn("blocked: unreadable (gh issue view 7: net/http: TLS handshake timeout)", frame)
        self.assertNotIn("no comments", frame)
        self.assertIn("DAEMON ACTIVITY", frame)
        with self._gh([]):
            frame = rd.render(ttl=60)
        self.assertNotIn("unreadable", frame)
        self.assertIn("blocked: no comments", frame)

    def test_every_gh_call_failing_still_renders_without_results(self):
        with self._gh([["gh", "issue", "list"], ["gh", "pr", "list"]]):
            frame = rd.render(ttl=60)
        self.assertIn("issues unreadable (", frame)
        self.assertIn("pull requests unreadable (", frame)
        self.assertIn("closed issues unreadable (", frame)
        self.assertIn("DAEMON ACTIVITY", frame)
        self.assertNotIn("nothing in flight", frame)

    def test_pr_list_failure_is_not_nothing_in_flight_or_no_blocker(self):
        self.issue_list = []
        with mock.patch.object(rd, "branches", lambda: [{"slug": "defect-7", "ahead": 1, "dq": False}]), \
                self._gh([["gh", "pr", "list"]]):
            frame = rd.render(ttl=60)
        self.assertIn("PR unreadable", frame)
        self.assertNotIn("no PR", frame)
        self.assertNotIn("nothing in flight", frame)
        self.assertIn("stale check not run: pull requests unreadable", frame)

    def test_command_timeout_names_the_command(self):
        err = subprocess.TimeoutExpired(["gh", "issue", "view", "7", "-R", rd.OPS_REPO], 60)
        self.assertEqual("unreadable (gh issue view timed out after 60s)",
                         rd.unreadable("TimeoutExpired: %s" % err))

    def test_check_run_failure_marks_only_that_pr(self):
        listing = json.dumps([{"number": 700, "headRefName": "fix/defect-7", "headRefOid": "a" * 40,
                               "state": "OPEN", "title": "t", "body": ""}])

        def fake_run(cmd, timeout=20, cwd=None):
            if cmd[:3] == ["gh", "pr", "list"]:
                return listing
            raise RuntimeError("gh api repos/x: HTTP 502: Bad Gateway")

        with mock.patch.object(rd, "run", fake_run), mock.patch.object(rd, "commit_known", lambda oid: False):
            prs = rd.pull_requests()
        stages = rd.in_flight([], [], prs, {}, set())
        self.assertEqual(
            ["PR #700 CI unreadable (gh api repos/x: HTTP 502: Bad Gateway), head not fetched"], stages[7])


if __name__ == "__main__":
    unittest.main()
