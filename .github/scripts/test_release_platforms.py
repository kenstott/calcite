#!/usr/bin/env python3
"""Every supported platform, Linux arm64 included, gets every pgwire bundle (REQ-294).

Covers: the platform table names macOS arm64, Linux x86_64, Linux arm64 (on GitHub's arm64 runner)
and Windows x86_64; pgwire-adapters-release.yml's plan builds and smoke-tests every adapter on each,
and its linux-aarch64 scope builds every adapter on Linux arm64 alone; pgwire-calcite-release.yml
builds the server on the same platforms; the maven-bundles job lays out each platform's Central
bundles, signs and attaches only in a publishing run, and never uploads to Maven Central; and
release-assets-complete.sh, against a stubbed gh, holds a release incomplete until every adapter
bundle and Central bundle of every platform is attached, without asking an older release for them.

Run: python3 -m pytest .github/scripts/test_release_platforms.py
"""
import json
import os
import stat
import subprocess
import tempfile
import unittest

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
WORKFLOWS = os.path.join(ROOT, ".github", "workflows")
SCRIPTS = os.path.join(ROOT, ".github", "scripts")
PLATFORMS = os.path.join(ROOT, "pgwire-calcite", "packaging", "platforms.json")
ADAPTERS = os.path.join(ROOT, "pgwire-calcite", "packaging", "adapters.json")
SUPPORTED = {
    "linux-x86_64": "ubuntu-latest",
    "linux-aarch64": "ubuntu-24.04-arm",
    "macos-arm64": "macos-15",
    "windows-x86_64": "windows-latest",
}
VERSION = "9.9.9"
CONNECTORS = ["file", "sharepoint", "splunk", "salesforce", "servicenow", "jira", "cloudops", "govdata"]


def workflow(name):
    with open(os.path.join(WORKFLOWS, name)) as f:
        return yaml.safe_load(f)


def platforms():
    with open(PLATFORMS) as f:
        return json.load(f)


def adapters():
    with open(ADAPTERS) as f:
        return [a["name"] for a in json.load(f)]


def run_plan(scope):
    """pgwire-adapters-release.yml's plan step, run as the runner would, for one scope."""
    step = next(s for s in workflow("pgwire-adapters-release.yml")["jobs"]["plan"]["steps"] if s.get("id") == "plan")
    with tempfile.NamedTemporaryFile("r", suffix=".out") as out:
        env = dict(os.environ, SCOPE=scope, GITHUB_OUTPUT=out.name)
        r = subprocess.run(["bash", "-c", step["run"]], cwd=ROOT, env=env, capture_output=True, text=True, timeout=60, check=False)
        assert r.returncode == 0, r.stdout + r.stderr
        lines = out.read().splitlines()
    return {k: v for k, v in (line.split("=", 1) for line in lines)}


class PlatformTableTest(unittest.TestCase):
    def test_the_supported_platforms_and_their_runners(self):
        self.assertEqual(SUPPORTED, {p["variant"]: p["runner"] for p in platforms()})


class AdaptersReleaseTest(unittest.TestCase):
    def test_a_full_run_builds_every_adapter_on_every_platform(self):
        plan = run_plan("all")
        self.assertEqual(SUPPORTED, {p["variant"]: p["runner"] for p in json.loads(plan["os"])})
        self.assertEqual(adapters(), [a["name"] for a in json.loads(plan["adapters"])])
        self.assertEqual(sorted(CONNECTORS), sorted(adapters()))

    def test_linux_arm64_is_smoke_tested(self):
        smoke = [p["variant"] for p in json.loads(run_plan("all")["smoke_os"])]
        self.assertIn("linux-aarch64", smoke)

    def test_the_linux_aarch64_scope_builds_every_adapter_on_linux_arm64_alone(self):
        plan = run_plan("linux-aarch64")
        self.assertEqual([{"runner": "ubuntu-24.04-arm", "variant": "linux-aarch64"}], json.loads(plan["os"]))
        self.assertEqual(adapters(), [a["name"] for a in json.loads(plan["adapters"])])
        self.assertEqual("false", plan["smoke_windows"])

    def test_the_linux_aarch64_scope_is_offered(self):
        wf = workflow("pgwire-adapters-release.yml")
        # PyYAML reads the bare key `on` as True.
        options = wf[True]["workflow_dispatch"]["inputs"]["scope"]["options"]
        self.assertIn("linux-aarch64", options)

    def test_the_bundle_and_publish_jobs_run_on_the_planned_platforms(self):
        jobs = workflow("pgwire-adapters-release.yml")["jobs"]
        for name in ("bundle", "publish", "maven-bundles"):
            self.assertEqual("${{ fromJSON(needs.plan.outputs.os) }}", jobs[name]["strategy"]["matrix"]["os"], name)


class MavenBundlesJobTest(unittest.TestCase):
    def setUp(self):
        self.job = workflow("pgwire-adapters-release.yml")["jobs"]["maven-bundles"]
        self.steps = {s.get("name"): s for s in self.job["steps"]}

    def test_it_lays_out_each_platform_from_that_platforms_bundles(self):
        download = next(s for s in self.job["steps"] if s.get("uses", "").startswith("actions/download-artifact"))
        self.assertEqual("bundle-*-${{ matrix.os.variant }}", download["with"]["pattern"])
        self.assertIn("bundle", self.job["needs"])
        layout = self.steps["Split the shared files and lay out the Central bundles"]["run"]
        self.assertIn("python3 .github/scripts/pgwire_maven_bundles.py tarballs bundles", layout)
        self.assertTrue(os.path.isfile(os.path.join(SCRIPTS, "pgwire_maven_bundles.py")))

    def test_only_a_publishing_run_signs_and_attaches(self):
        self.assertEqual("env.PUBLISH == 'true'", self.steps["Import GPG key"]["if"])
        self.assertEqual("env.PUBLISH == 'true'", self.steps["Attach to release"]["if"])
        sign = self.steps["Sign, checksum and zip each Central bundle"]["run"]
        self.assertIn('if [ "$PUBLISH" = "true" ]', sign)
        self.assertIn("--detach-sign", sign)
        self.assertIn("-central-bundle.zip", sign)
        self.assertIn('sha256sum "$zip_name" > "${zip_name}.sha256"', sign)

    def test_nothing_in_the_release_uploads_to_maven_central(self):
        with open(os.path.join(WORKFLOWS, "pgwire-adapters-release.yml")) as f:
            text = f.read()
        self.assertNotIn("central.sonatype.com", text)
        self.assertNotIn("SONATYPE", text)


class CalciteReleaseTest(unittest.TestCase):
    def test_the_server_is_built_on_every_platform(self):
        include = workflow("pgwire-calcite-release.yml")["jobs"]["bundle"]["strategy"]["matrix"]["include"]
        self.assertEqual(SUPPORTED, {i["variant"]: i["os"] for i in include})


FAKE_GH = r'''#!/usr/bin/env bash
case "$1 $2" in
  "release view") [ -f "$FAKE_DIR/view_fails" ] && exit 1; cat "$FAKE_DIR/assets.txt" ;;
  *) echo "unexpected gh call: $*" >&2; exit 2 ;;
esac
'''


class ReleaseAssetsCompleteTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        os.makedirs(os.path.join(d, "bin"))
        gh = os.path.join(d, "bin", "gh")
        with open(gh, "w") as f:
            f.write(FAKE_GH)
        os.chmod(gh, os.stat(gh).st_mode | stat.S_IEXEC)
        self.d = d

    def tearDown(self):
        self.tmp.cleanup()

    def check(self, tag, assets):
        with open(os.path.join(self.d, "assets.txt"), "w") as f:
            f.write("".join(a + "\n" for a in assets))
        env = dict(os.environ, FAKE_DIR=self.d, PATH=os.path.join(self.d, "bin") + ":" + os.environ["PATH"])
        return subprocess.run(["bash", os.path.join(SCRIPTS, "release-assets-complete.sh"), "owner/repo", tag],
                              env=env, capture_output=True, text=True, timeout=60, check=False)

    def required(self, tag):
        """What the script names as missing from an empty release: everything it requires."""
        r = self.check(tag, [])
        self.assertEqual(1, r.returncode, r.stdout + r.stderr)
        return [line.removeprefix("missing: ") for line in r.stdout.splitlines() if line.startswith("missing: ")]

    def test_every_platform_has_every_connector_bundle_and_central_bundle(self):
        required = set(self.required("engine-v" + VERSION))
        for variant in SUPPORTED:
            for name in ["calcite"] + CONNECTORS:
                tarball = f"pgwire-{name}-{VERSION}-{variant}.tar.gz"
                self.assertIn(tarball, required)
                self.assertIn(tarball + ".sha256", required)
            for artifact in ["shared"] + CONNECTORS:
                zipped = f"pgwire-{artifact}-{variant}-{VERSION}-central-bundle.zip"
                self.assertIn(zipped, required)
                self.assertIn(zipped + ".sha256", required)

    def test_a_complete_release_passes_and_one_missing_linux_arm64_bundle_fails(self):
        tag = "engine-v" + VERSION
        required = self.required(tag)
        r = self.check(tag, required)
        self.assertEqual(0, r.returncode, r.stdout + r.stderr)
        for gone in (f"pgwire-govdata-{VERSION}-linux-aarch64.tar.gz",
                     f"pgwire-shared-linux-aarch64-{VERSION}-central-bundle.zip"):
            r = self.check(tag, [a for a in required if a != gone])
            self.assertEqual(1, r.returncode, r.stdout)
            self.assertIn("missing: " + gone + "\n", r.stdout)

    def test_an_older_release_is_not_asked_for_linux_arm64_or_central_bundles(self):
        required = self.required("engine-v0.127.1")
        self.assertIn("pgwire-file-0.127.1-linux-x86_64.tar.gz", required)
        self.assertFalse([a for a in required if "linux-aarch64" in a or "pgwire-shared" in a], required)

    def test_an_unreadable_release_is_not_complete(self):
        open(os.path.join(self.d, "view_fails"), "w").close()
        r = self.check("engine-v" + VERSION, [])
        self.assertEqual(2, r.returncode)


if __name__ == "__main__":
    unittest.main()
