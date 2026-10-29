#!/usr/bin/env python3
"""Tests promote-maven-central.yml's fetch-and-verify step and central-bundles.sh, against a stubbed gh.

Covers: a complete release verifies; a missing bundle or .sha256 fails naming it, before any
download; a checksum mismatch fails; an unreadable release fails; a tag of another kind fails;
the engine-v and sih-v bundle sets.

Run: python3 .github/scripts/test_promote_step.py
"""
import hashlib
import os
import shutil
import stat
import subprocess
import tempfile
import unittest

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
WORKFLOW = os.path.join(ROOT, ".github", "workflows", "promote-maven-central.yml")
LISTER = os.path.join(ROOT, ".github", "scripts", "central-bundles.sh")
VERSION = "9.9.9"
TAG = "engine-v" + VERSION
ENGINE = ["askamerica-engine", "govdata", "trino-calcite-base", "trino-calcite", "trino-cloudops",
          "trino-file", "trino-salesforce", "trino-servicenow", "trino-sharepoint", "trino-splunk"]
SIH = ["sih-govdata", "sih-splunk", "sih-sharepoint", "sih-aperio", "sih-cloudops"]

FAKE_GH = r'''#!/usr/bin/env bash
d="$FAKE_DIR"
echo "gh $*" >> "$d/calls.log"
case "$1 $2" in
  "release view") [ -f "$d/view_fails" ] && exit 1; cat "$d/assets.txt" ;;
  "release download")
    dir=""; pats=()
    shift 3
    while [ $# -gt 0 ]; do case "$1" in --dir) dir="$2"; shift 2;; --pattern) pats+=("$2"); shift 2;; *) shift;; esac; done
    for p in "${pats[@]}"; do cp "$d/files/$p" "$dir/$p"; done ;;
  *) echo "unexpected gh call: $*" >&2; exit 2 ;;
esac
'''


def bundle(artifact, version=VERSION):
    return "%s-%s-central-bundle.zip" % (artifact, version)


def step_script():
    wf = yaml.safe_load(open(WORKFLOW))
    steps = wf["jobs"]["promote"]["steps"]
    step = next(s for s in steps if s.get("name") == "Fetch and verify the release's Central bundles")
    assert "${{" not in step["run"], "an expression the test does not substitute"
    return step["run"]


class CentralBundlesTest(unittest.TestCase):
    def lister(self, tag):
        return subprocess.run(["bash", LISTER, tag], capture_output=True, text=True)

    def test_engine_set(self):
        r = self.lister(TAG)
        self.assertEqual(0, r.returncode, r.stderr)
        self.assertEqual([bundle(a) for a in ENGINE], r.stdout.split())

    def test_sih_set(self):
        r = self.lister("sih-v1.2.3")
        self.assertEqual(0, r.returncode, r.stderr)
        self.assertEqual([bundle(a, "1.2.3") for a in SIH], r.stdout.split())

    def test_other_tag_fails(self):
        r = self.lister("v1.0.0")
        self.assertEqual(2, r.returncode)
        self.assertIn("not an engine-v or sih-v tag", r.stderr)


class PromoteFetchStepTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        self.d = d
        os.makedirs(os.path.join(d, "bin"))
        os.makedirs(os.path.join(d, "files"))
        os.makedirs(os.path.join(d, "work", ".github", "scripts"))
        shutil.copy(LISTER, os.path.join(d, "work", ".github", "scripts", "central-bundles.sh"))
        gh = os.path.join(d, "bin", "gh")
        open(gh, "w").write(FAKE_GH)
        os.chmod(gh, os.stat(gh).st_mode | stat.S_IEXEC)
        names = []
        for a in ENGINE:
            name = bundle(a)
            content = ("bundle " + name).encode()
            open(os.path.join(d, "files", name), "wb").write(content)
            open(os.path.join(d, "files", name + ".sha256"), "w").write(
                hashlib.sha256(content).hexdigest() + "  " + name + "\n")
            names += [name, name + ".sha256"]
        self.write_assets(names + ["askamerica-engine.jar"])

    def tearDown(self):
        self.tmp.cleanup()

    def write_assets(self, names):
        open(os.path.join(self.d, "assets.txt"), "w").write("\n".join(names) + "\n")

    def run_step(self, tag=TAG):
        env = dict(os.environ, FAKE_DIR=self.d, PATH=os.path.join(self.d, "bin") + ":" + os.environ["PATH"],
                   GH_TOKEN="x", REPO="owner/repo", TAG=tag)
        return subprocess.run(["bash", "-e", "-c", step_script()], cwd=os.path.join(self.d, "work"),
                              env=env, capture_output=True, text=True, timeout=60)

    def downloads(self):
        log = os.path.join(self.d, "calls.log")
        return [l for l in open(log).read().splitlines() if l.startswith("gh release download")] \
            if os.path.exists(log) else []

    def test_complete_release_verifies(self):
        r = self.run_step()
        self.assertEqual(0, r.returncode, r.stdout + r.stderr)
        self.assertIn("all %d bundles of %s match their sha256" % (len(ENGINE), TAG), r.stdout)
        self.assertEqual(len(ENGINE), len(self.downloads()))

    def test_missing_bundle_fails_naming_it_before_any_download(self):
        gone = bundle("trino-splunk")
        names = open(os.path.join(self.d, "assets.txt")).read().split()
        self.write_assets([n for n in names if n != gone])
        r = self.run_step()
        self.assertNotEqual(0, r.returncode)
        self.assertIn("release %s has no %s\n" % (TAG, gone), r.stdout)
        self.assertEqual([], self.downloads())

    def test_missing_checksum_fails_naming_it(self):
        gone = bundle("govdata") + ".sha256"
        names = open(os.path.join(self.d, "assets.txt")).read().split()
        self.write_assets([n for n in names if n != gone])
        r = self.run_step()
        self.assertNotEqual(0, r.returncode)
        self.assertIn("release %s has no %s" % (TAG, gone), r.stdout)

    def test_checksum_mismatch_fails(self):
        name = bundle("trino-file")
        open(os.path.join(self.d, "files", name + ".sha256"), "w").write("0" * 64 + "  " + name + "\n")
        r = self.run_step()
        self.assertNotEqual(0, r.returncode)
        self.assertNotIn("match their sha256", r.stdout)

    def test_unreadable_release_fails(self):
        open(os.path.join(self.d, "view_fails"), "w").write("")
        r = self.run_step()
        self.assertNotEqual(0, r.returncode)
        self.assertIn("cannot read release", r.stdout)

    def test_other_tag_fails(self):
        r = self.run_step("v1.0.0")
        self.assertNotEqual(0, r.returncode)
        self.assertEqual([], self.downloads())


if __name__ == "__main__":
    unittest.main()
