#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for the lead tools and the role guards.  Run:  python3 -m unittest govdata/scripts/lead/test_lead_tools.py"""
import argparse
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
sys.path.insert(0, HERE)
import leadlib  # noqa: E402


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


admit = load("admit", os.path.join(HERE, "admit.py"))
GUARD_PATH = os.path.join(REPO, ".claude", "hooks", "role-guard.py")
HAVE_HOOKS = os.path.exists(GUARD_PATH)  # .claude/ is not tracked in git
guard = load("role_guard", GUARD_PATH) if HAVE_HOOKS else None
status = load("etl_status", os.path.join(HERE, "etl-status.py"))


def args(**kw):
    d = dict(type="remediation", schema=None, year=None, pipeline=None, est_mb=None, apply=False)
    d.update(kw)
    return argparse.Namespace(**d)


PROCS = [
    (10, 100, "bash /x/run-pool.sh -j 8 historical"),
    (11, 100, "bash /x/worker.sh sec_secondary 2025"),
    (12, 50, "bash ./force-reprocess.sh --schema econ --tables industry_gdp --start 2009"),
    (14, 900, "bash ./run-pool.sh -j 3 ag:daily census:daily housing:daily"),
    (13, 50, "bash -c until ! pgrep -f 'force-reprocess.sh --schema health'; do sleep 20; done"),
]


class Admission(unittest.TestCase):
    def test_admits_when_memory_and_schema_are_free(self):
        v, _ = admit.decide(args(schema="census"), PROCS, 30000)
        self.assertEqual(v, "ADMIT")

    def test_waits_for_memory(self):
        v, why = admit.decide(args(schema="census", est_mb=30000), PROCS, 30000)
        self.assertEqual(v, "WAIT")
        self.assertIn("memory", why)

    def test_waits_when_the_same_schema_and_year_is_running(self):
        v, why = admit.decide(args(schema="sec_secondary", year="2025"), PROCS, 30000)
        self.assertEqual(v, "WAIT")
        self.assertIn("worker.sh", why)

    def test_a_different_year_of_the_same_schema_is_admitted(self):
        v, _ = admit.decide(args(schema="sec_secondary", year="2019"), PROCS, 30000)
        self.assertEqual(v, "ADMIT")

    def test_a_pool_command_line_alone_is_not_live_work_for_its_specs(self):
        v, _ = admit.decide(args(schema="ag"), PROCS, 30000)
        self.assertEqual(v, "ADMIT")

    def test_waits_when_force_reprocess_holds_the_schema(self):
        v, _ = admit.decide(args(schema="econ"), PROCS, 30000)
        self.assertEqual(v, "WAIT")

    def test_a_waiter_shell_that_merely_mentions_a_script_is_not_work(self):
        v, _ = admit.decide(args(schema="health"), PROCS, 30000)
        self.assertEqual(v, "ADMIT")

    def test_memory_estimate_comes_from_the_table(self):
        self.assertGreaterEqual(admit.estimate_mb("fr_document_text", None), 2000)
        self.assertEqual(admit.estimate_mb("no_such_pipeline", None), admit.DEFAULT_EST_MB)

    def test_a_live_lock_blocks_and_a_dead_one_does_not(self):
        with tempfile.TemporaryDirectory() as d:
            old = leadlib.LOCK_DIR
            leadlib.LOCK_DIR = d
            try:
                with open(os.path.join(d, "census"), "w") as f:
                    f.write(str(os.getpid()))
                self.assertIsNotNone(admit.lock_state("census", None))
                with open(os.path.join(d, "census"), "w") as f:
                    f.write("999999999")
                self.assertIsNone(admit.lock_state("census", None))
            finally:
                leadlib.LOCK_DIR = old


@unittest.skipUnless(HAVE_HOOKS, ".claude/hooks not present")
class Roles(unittest.TestCase):
    def v(self, cmd, role, **env):
        return guard.violations_for(cmd, role, env)

    def test_no_role_may_merge_release_or_close(self):
        for role in ("worker", "remediation"):
            for cmd in ("gh pr merge 12 --squash", "bash govdata/scripts/fix-branch.sh merge x",
                        "git push origin main", "gh issue close 5", "git tag engine-v1", "gh release create x"):
                self.assertTrue(self.v(cmd, role), (role, cmd))

    def test_a_worker_may_push_its_branch_and_comment(self):
        for cmd in ("git push -u origin fix/defect-5", "gh issue comment 5 --body 'gh pr merge is not mine'",
                    "govdata/scripts/fix-branch.sh start defect-5", "git commit -m 'merge main'"):
            self.assertEqual(self.v(cmd, "worker"), [], cmd)

    def test_a_worker_may_not_apply_data(self):
        for cmd in ("bash govdata/scripts/parallel/force-reprocess.sh --schema econ --tables t",
                    "nohup bash .claude/scripts/data_purge.sh --schema a --tables b",
                    "rclone deletefile minio:x/y", "FOO=1 govdata/scripts/parallel/run-pool.sh econ:daily"):
            self.assertTrue(self.v(cmd, "worker"), cmd)

    def test_remediation_applies_only_when_admitted(self):
        cmd = "bash govdata/scripts/parallel/force-reprocess.sh --schema econ --tables t"
        self.assertTrue(self.v(cmd, "remediation"))
        self.assertEqual(self.v(cmd, "remediation", CLAUDE_ADMITTED="1"), [])

    def test_deletes_need_approval_even_when_admitted(self):
        cmd = "bash .claude/scripts/data_purge.sh --schema econ --tables t"
        self.assertTrue(self.v(cmd, "remediation", CLAUDE_ADMITTED="1"))
        self.assertEqual(self.v(cmd, "remediation", CLAUDE_ADMITTED="1", CLAUDE_APPROVED="1"), [])

    def test_edits_in_the_main_tree_are_refused_but_worktrees_and_tmp_are_not(self):
        env = {"CLAUDE_MAIN_TREE": REPO}
        self.assertIsNotNone(guard.path_violation(os.path.join(REPO, "CLAUDE.md"), env))
        self.assertIsNone(guard.path_violation(REPO + "-fixes/defect-1/CLAUDE.md", env))
        self.assertIsNone(guard.path_violation("/tmp/x.txt", env))


def run_hook(script, payload, **env):
    e = {k: v for k, v in os.environ.items() if not k.startswith("CLAUDE_")}
    e.update(env)
    e["CLAUDE_PROJECT_DIR"] = REPO
    return subprocess.run([sys.executable, os.path.join(REPO, ".claude", "hooks", script)], input=json.dumps(payload),
                          capture_output=True, text=True, env=e)


@unittest.skipUnless(HAVE_HOOKS, ".claude/hooks not present")
class Hooks(unittest.TestCase):
    def test_an_attended_session_is_never_restricted(self):
        r = run_hook("role-guard.py", {"tool_name": "Bash", "tool_input": {"command": "gh pr merge 1"}})
        self.assertEqual(r.returncode, 0)

    def test_a_worker_merge_is_blocked_in_the_current_mode(self):
        r = run_hook("role-guard.py", {"tool_name": "Bash", "tool_input": {"command": "gh pr merge 1"}},
                     CLAUDE_ROLE="worker", CLAUDE_ISSUE="1")
        self.assertEqual(r.returncode, 0 if leadlib.mode() == "shadow" else 2, r.stderr)

    def test_stop_guard_ignores_attended_sessions(self):
        self.assertEqual(run_hook("worker-stop-guard.py", {}).returncode, 0)


if __name__ == "__main__":
    unittest.main()
