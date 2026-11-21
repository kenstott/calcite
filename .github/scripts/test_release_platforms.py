#!/usr/bin/env python3
"""Every supported platform, Linux arm64 included, gets the pgwire runtime and every pack (REQ-294,
REQ-2254).

Covers: the platform table names macOS arm64, Linux x86_64, Linux arm64 (on GitHub's arm64 runner)
and Windows x86_64; pgwire-adapters-release.yml's plan builds and smoke-tests every adapter's pack on
each, and its linux-aarch64 scope builds every pack on Linux arm64 alone; pgwire-calcite-release.yml,
which the adapters release calls, builds the runtime on the same platforms from every adapter's jars;
the maven-bundles job lays out each platform's Central bundles, signs and attaches only in a
publishing run, and never uploads to Maven Central; and release-assets-complete.sh, against a
stubbed gh, holds a release incomplete until the runtime, every pack and every Central bundle of
every platform is attached, while an older release is asked for the bundles it shipped.

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


def run_plan(scope, name="pgwire-adapters-release.yml"):
    """A release workflow's plan step, run as the runner would, for one scope."""
    step = next(s for s in workflow(name)["jobs"]["plan"]["steps"] if s.get("id") == "plan")
    with tempfile.NamedTemporaryFile("r", suffix=".out") as out:
        env = dict(os.environ, SCOPE=scope, VERSION="engine-v" + VERSION, GITHUB_OUTPUT=out.name)
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
        self.assertEqual(VERSION, plan["version"])

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

    def test_the_pack_and_publish_jobs_run_on_the_planned_platforms(self):
        jobs = workflow("pgwire-adapters-release.yml")["jobs"]
        for name in ("pack", "publish", "publish-runtime", "maven-bundles"):
            self.assertEqual("${{ fromJSON(needs.plan.outputs.os) }}", jobs[name]["strategy"]["matrix"]["os"], name)

    def test_the_packs_are_built_against_the_runtime_this_run_builds(self):
        jobs = workflow("pgwire-adapters-release.yml")["jobs"]
        self.assertEqual("./.github/workflows/pgwire-calcite-release.yml", jobs["runtime"]["uses"])
        self.assertEqual("${{ needs.plan.outputs.version }}", jobs["runtime"]["with"]["version"])
        self.assertEqual("${{ inputs.scope || 'all' }}", jobs["runtime"]["with"]["scope"])
        self.assertIn("runtime", jobs["pack"]["needs"])
        downloads = [s["with"].get("name") for s in jobs["pack"]["steps"]
                     if s.get("uses", "").startswith("actions/download-artifact")]
        self.assertEqual(["adapter-jars-${{ matrix.adapter.name }}", "runtime-jars",
                          "runtime-${{ matrix.os.variant }}", "catalog-cache-govdata"], downloads)

    def test_the_govdata_pack_ships_the_catalog_cache_this_release_walked(self):
        jobs = workflow("pgwire-adapters-release.yml")["jobs"]
        cache = jobs["catalog-cache"]
        self.assertEqual(["plan", "runtime"], cache["needs"])
        # A skipped job would skip the packs and every job after them; it idles instead.
        self.assertNotIn("if", cache)
        self.assertIn("catalog-cache", jobs["pack"]["needs"])
        runs = "\n".join(s.get("run", "") for s in cache["steps"])
        self.assertIn('echo "$VERSION" > src/pgwire_calcite/ENGINE_VERSION', runs)
        self.assertIn("scripts/build_catalog_cache.py ../pgwire-govdata/model.json", runs)
        self.assertIn("check_catalog_cache_coverage.py", runs)
        self.assertIn("pgwire-govdata/unmaterialized-tables.txt", runs)
        walk = next(s for s in cache["steps"] if "build_catalog_cache.py" in s.get("run", ""))
        self.assertEqual("${{ secrets.AWS_ACCESS_KEY_ID }}", walk["env"]["AWS_ACCESS_KEY_ID"])
        self.assertTrue(walk["env"]["GOVDATA_DUCKDB_CATALOG"].startswith("${{ github.workspace }}/"))
        upload = next(s for s in cache["steps"] if s.get("uses", "").startswith("actions/upload-artifact"))
        self.assertEqual("catalog-cache-govdata", upload["with"]["name"])
        self.assertEqual("error", upload["with"]["if-no-files-found"])
        get = next(s for s in jobs["pack"]["steps"]
                   if s.get("with", {}).get("name") == "catalog-cache-govdata")
        self.assertEqual("matrix.adapter.name == 'govdata'", get["if"])
        model = next(s for s in jobs["pack"]["steps"]
                     if s.get("name") == "Model, catalog cache, manifest + launcher")
        self.assertIn("from pgwire_calcite.catalog_cache_key import cache_key", model["run"])
        self.assertNotIn("pgwire-${{ matrix.adapter.name }}\"/catalog-cache-", model["run"])

    def test_the_runtime_is_stamped_with_its_engine_release(self):
        steps = workflow("pgwire-calcite-release.yml")["jobs"]["runtime"]["steps"]
        runs = "\n".join(s.get("run", "") for s in steps)
        self.assertIn('echo "$VERSION" > "$PKG/ENGINE_VERSION"', runs)

    def test_the_runtime_and_packs_are_published_only_after_the_smoke_jobs(self):
        jobs = workflow("pgwire-adapters-release.yml")["jobs"]
        for name in ("publish", "publish-runtime"):
            self.assertIn("smoke", jobs[name]["needs"])
            self.assertIn("smoke-windows", jobs[name]["needs"])


class MavenBundlesJobTest(unittest.TestCase):
    def setUp(self):
        self.job = workflow("pgwire-adapters-release.yml")["jobs"]["maven-bundles"]
        self.steps = {s.get("name"): s for s in self.job["steps"]}

    def test_it_lays_out_each_platform_from_that_platforms_bundles(self):
        downloads = [s["with"] for s in self.job["steps"] if s.get("uses", "").startswith("actions/download-artifact")]
        self.assertEqual("pack-*-${{ matrix.os.variant }}", downloads[0]["pattern"])
        self.assertEqual("runtime-${{ matrix.os.variant }}", downloads[1]["name"])
        self.assertIn("pack", self.job["needs"])
        self.assertIn("runtime", self.job["needs"])
        layout = self.steps["Lay out the runtime and the packs as Central bundles"]["run"]
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
    def test_the_runtime_is_built_on_every_platform(self):
        jobs = workflow("pgwire-calcite-release.yml")["jobs"]
        self.assertEqual("${{ fromJSON(needs.plan.outputs.os) }}", jobs["runtime"]["strategy"]["matrix"]["os"])
        plan = run_plan("all", "pgwire-calcite-release.yml")
        self.assertEqual(SUPPORTED, {p["variant"]: p["runner"] for p in json.loads(plan["os"])})

    def test_the_runtime_jars_come_from_every_adapter_whatever_the_scope(self):
        for scope in ("all", "linux-file-govdata", "windows-file", "linux-aarch64"):
            plan = run_plan(scope, "pgwire-calcite-release.yml")
            self.assertEqual(adapters(), [a["name"] for a in json.loads(plan["jars"])], scope)

    def test_the_runtime_is_attached_by_the_adapters_release_when_called_from_it(self):
        attach = next(s for s in workflow("pgwire-calcite-release.yml")["jobs"]["runtime"]["steps"]
                      if s.get("name") == "Attach to release")
        self.assertEqual("env.PUBLISH == 'true' && github.workflow == 'pgwire-calcite release'", attach["if"])


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

    def test_every_platform_has_the_runtime_every_pack_and_every_central_bundle(self):
        required = set(self.required("engine-v" + VERSION))
        for variant in SUPPORTED:
            for tarball in [f"pgwire-calcite-runtime-{VERSION}-{variant}.tar.gz",
                            *(f"pgwire-{name}-pack-{VERSION}-{variant}.tar.gz" for name in CONNECTORS)]:
                self.assertIn(tarball, required)
                self.assertIn(tarball + ".sha256", required)
            self.assertNotIn(f"pgwire-file-{VERSION}-{variant}.tar.gz", required)
            for artifact in ["calcite-runtime"] + CONNECTORS:
                zipped = f"pgwire-{artifact}-{variant}-{VERSION}-central-bundle.zip"
                self.assertIn(zipped, required)
                self.assertIn(zipped + ".sha256", required)

    def test_a_complete_release_passes_and_one_missing_linux_arm64_bundle_fails(self):
        tag = "engine-v" + VERSION
        required = self.required(tag)
        r = self.check(tag, required)
        self.assertEqual(0, r.returncode, r.stdout + r.stderr)
        for gone in (f"pgwire-govdata-pack-{VERSION}-linux-aarch64.tar.gz",
                     f"pgwire-calcite-runtime-{VERSION}-linux-aarch64.tar.gz",
                     f"pgwire-calcite-runtime-linux-aarch64-{VERSION}-central-bundle.zip"):
            r = self.check(tag, [a for a in required if a != gone])
            self.assertEqual(1, r.returncode, r.stdout)
            self.assertIn("missing: " + gone + "\n", r.stdout)

    def test_an_older_release_is_not_asked_for_linux_arm64_or_central_bundles(self):
        required = self.required("engine-v0.127.1")
        self.assertIn("pgwire-file-0.127.1-linux-x86_64.tar.gz", required)
        self.assertFalse([a for a in required if "linux-aarch64" in a or "pgwire-shared" in a], required)

    def test_a_release_before_the_packs_is_asked_for_its_bundles(self):
        required = self.required("engine-v0.128.7")
        self.assertIn("pgwire-govdata-0.128.7-linux-aarch64.tar.gz", required)
        self.assertIn("pgwire-shared-linux-aarch64-0.128.7-central-bundle.zip", required)
        self.assertFalse([a for a in required if "-pack-" in a or "runtime" in a], required)

    def test_an_unreadable_release_is_not_complete(self):
        open(os.path.join(self.d, "view_fails"), "w").close()
        r = self.check("engine-v" + VERSION, [])
        self.assertEqual(2, r.returncode)


if __name__ == "__main__":
    unittest.main()
