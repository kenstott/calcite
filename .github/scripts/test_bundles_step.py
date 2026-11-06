#!/usr/bin/env python3
"""Tests the `bundles` job of askamerica-engine.yml: run its step script against a stubbed gh.

Covers: nothing dispatched when the bundles are already there or a build for this version is in
flight; a dispatch when none is; waiting until the bundles appear; a timeout that fails; a failed
lookup that is never read as "published"; a checksum mismatch that fails.

Run: python3 .github/scripts/test_bundles_step.py
"""
import hashlib
import os
import stat
import subprocess
import tempfile
import unittest

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
WORKFLOW = os.path.join(ROOT, ".github", "workflows", "askamerica-engine.yml")
VERSION = "9.9.9"
ASSETS = ["pgwire-govdata-%s-%s.tar.gz" % (VERSION, v) for v in ("windows-x86_64", "macos-arm64", "linux-x86_64")]

FAKE_GH = r'''#!/usr/bin/env bash
# Stub of gh driven by files in $FAKE_DIR.
d="$FAKE_DIR"
echo "gh $*" >> "$d/calls.log"
case "$1 $2" in
  "release view")
    [ -f "$d/view_fails" ] && exit 1
    n=$(cat "$d/polls" 2>/dev/null || echo 0); n=$((n+1)); echo $n > "$d/polls"
    # the listed assets: the first $APPEAR_AFTER polls show only what is in present.txt
    if [ "$n" -gt "$(cat "$d/appear_after")" ]; then cat "$d/all.txt"; else cat "$d/present.txt"; fi ;;
  "run list") cat "$d/in_flight" ;;
  "workflow run") echo "dispatched" >> "$d/dispatched" ;;
  "release download")
    dir=""; pats=()
    shift 2
    while [ $# -gt 0 ]; do case "$1" in --dir) dir="$2"; shift 2;; --pattern) pats+=("$2"); shift 2;; *) shift;; esac; done
    for p in "${pats[@]}"; do cp "$d/files/$p" "$dir/$p"; done ;;
  *) echo "unexpected gh call: $*" >&2; exit 2 ;;
esac
'''


def step_script():
    wf = yaml.safe_load(open(WORKFLOW))
    steps = wf["jobs"]["bundles"]["steps"]
    run = steps[0]["run"]
    run = run.replace("${{ github.repository }}", "owner/repo")
    run = run.replace("${{ github.event.inputs.version || github.ref_name }}", "engine-v" + VERSION)
    assert "${{" not in run, "an expression the test does not substitute"
    return run


class BundlesStepTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        os.makedirs(os.path.join(d, "bin"))
        os.makedirs(os.path.join(d, "files"))
        gh = os.path.join(d, "bin", "gh")
        open(gh, "w").write(FAKE_GH)
        os.chmod(gh, os.stat(gh).st_mode | stat.S_IEXEC)
        for a in ASSETS:
            content = ("bundle " + a).encode()
            open(os.path.join(d, "files", a), "wb").write(content)
            open(os.path.join(d, "files", a + ".sha256"), "w").write(hashlib.sha256(content).hexdigest() + "  " + a + "\n")
        open(os.path.join(d, "all.txt"), "w").write("\n".join(ASSETS) + "\n")
        open(os.path.join(d, "present.txt"), "w").write("")
        open(os.path.join(d, "appear_after"), "w").write("0")
        open(os.path.join(d, "in_flight"), "w").write("0")
        self.d = d

    def tearDown(self):
        self.tmp.cleanup()

    def run_step(self, max_wait="1", poll="0"):
        env = dict(os.environ, FAKE_DIR=self.d, PATH=os.path.join(self.d, "bin") + ":" + os.environ["PATH"],
                   BUNDLE_MAX_WAIT_MINUTES=max_wait, BUNDLE_POLL_SECONDS=poll)
        return subprocess.run(["bash", "-c", step_script()], env=env, capture_output=True, text=True, timeout=120)

    def dispatched(self):
        return os.path.exists(os.path.join(self.d, "dispatched"))

    def test_bundles_already_published_dispatch_nothing(self):
        r = self.run_step()
        self.assertEqual(0, r.returncode, r.stdout + r.stderr)
        self.assertFalse(self.dispatched())
        self.assertIn("matches its published sha256", r.stdout)

    def test_missing_with_a_build_in_flight_waits_and_does_not_dispatch(self):
        open(os.path.join(self.d, "in_flight"), "w").write("1")
        open(os.path.join(self.d, "appear_after"), "w").write("4")
        r = self.run_step()
        self.assertEqual(0, r.returncode, r.stdout + r.stderr)
        self.assertFalse(self.dispatched())
        self.assertIn("already in flight", r.stdout)

    def test_missing_with_no_build_dispatches_once_then_waits(self):
        open(os.path.join(self.d, "appear_after"), "w").write("4")
        r = self.run_step()
        self.assertEqual(0, r.returncode, r.stdout + r.stderr)
        self.assertEqual(1, open(os.path.join(self.d, "dispatched")).read().count("dispatched"))
        self.assertIn("waiting for:", r.stdout)

    def test_a_partial_set_keeps_waiting_for_the_rest(self):
        open(os.path.join(self.d, "present.txt"), "w").write(ASSETS[0] + "\n")
        open(os.path.join(self.d, "appear_after"), "w").write("3")
        r = self.run_step()
        self.assertEqual(0, r.returncode, r.stdout + r.stderr)
        self.assertNotIn(ASSETS[0] + " ", r.stdout.split("waiting for:")[1] if "waiting for:" in r.stdout else "")

    def test_bundles_that_never_appear_fail_the_job(self):
        open(os.path.join(self.d, "appear_after"), "w").write("1000000")
        r = self.run_step(max_wait="0")
        self.assertNotEqual(0, r.returncode)
        self.assertIn("still missing", r.stdout)
        self.assertNotIn("matches its published sha256", r.stdout)

    def test_a_failing_lookup_is_never_read_as_published(self):
        open(os.path.join(self.d, "view_fails"), "w").write("")
        r = self.run_step(max_wait="0")
        self.assertNotEqual(0, r.returncode)
        self.assertNotIn("matches its published sha256", r.stdout)

    def test_a_checksum_mismatch_fails(self):
        bad = os.path.join(self.d, "files", ASSETS[1] + ".sha256")
        open(bad, "w").write("0" * 64 + "  " + ASSETS[1] + "\n")
        r = self.run_step()
        self.assertNotEqual(0, r.returncode)
        self.assertIn("sha256 mismatch", r.stdout)


if __name__ == "__main__":
    unittest.main()
