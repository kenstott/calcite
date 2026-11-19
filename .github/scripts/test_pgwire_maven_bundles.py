#!/usr/bin/env python3
"""Tests pgwire_maven_bundles.py, the Maven Central layout of the pgwire runtime and packs (REQ-294).

Covers: a Central directory is complete; and, end to end over release-named tarballs of every
supported platform (Linux arm64 included), the runtime and every adapter pack of every platform get
their artifact, each artifact holds its release tarball byte for byte, and packs without their
platform's runtime are refused.

Run: python3 -m pytest .github/scripts/test_pgwire_maven_bundles.py
"""
import hashlib
import importlib.util
import io
import json
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
VERSION = "9.9.9"
VARIANTS = ["linux-x86_64", "linux-aarch64", "macos-arm64", "windows-x86_64"]
CONNECTORS = ["file", "jira", "govdata"]
GROUP_PATH = "io/simpleishard"
def _module():
    spec = importlib.util.spec_from_file_location("pgwire_maven_bundles", HERE / "pgwire_maven_bundles.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["pgwire_maven_bundles"] = module
    spec.loader.exec_module(module)
    return module


maven = _module()


class CentralDirTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_a_central_directory_is_complete(self):
        tarball = self.root / "x.tar.gz"
        with tarfile.open(tarball, "w:gz"):
            pass
        maven.central_dir("pgwire-file-linux-aarch64", "0.1.0", tarball, "d", self.root / "out")
        base = self.root / "out/pgwire-file-linux-aarch64" / GROUP_PATH / "pgwire-file-linux-aarch64/0.1.0"
        names = {p.name for p in base.iterdir()}
        stem = "pgwire-file-linux-aarch64-0.1.0"
        for name in (f"{stem}.tar.gz", f"{stem}.pom", f"{stem}-sources.jar", f"{stem}-javadoc.jar"):
            self.assertIn(name, names)
            self.assertIn(name + ".sha256", names)
        pom = (base / f"{stem}.pom").read_text()
        self.assertIn("<packaging>tar.gz</packaging>", pom)
        self.assertIn("<groupId>io.simpleishard</groupId>", pom)
        self.assertIn("<artifactId>pgwire-file-linux-aarch64</artifactId>", pom)


def _tarball(path: Path, member: str, body: bytes) -> Path:
    with tarfile.open(path, "w:gz") as tar:
        info = tarfile.TarInfo(member)
        info.size = len(body)
        tar.addfile(info, io.BytesIO(body))
    return path


class ReleaseLayoutTest(unittest.TestCase):
    """main() over release-named runtime and pack tarballs of every platform."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.tarballs = root / "tarballs"
        self.tarballs.mkdir()
        self.digests = {}
        for variant in VARIANTS:
            names = {f"pgwire-calcite-runtime-{variant}": (
                f"pgwire-calcite-runtime-{VERSION}-{variant}.tar.gz", f"runtimes/{VERSION}/runtime.json")}
            for connector in CONNECTORS:
                names[f"pgwire-{connector}-{variant}"] = (
                    f"pgwire-{connector}-pack-{VERSION}-{variant}.tar.gz",
                    f"packs/{connector}/{VERSION}/pack.json")
            for artifact, (name, member) in names.items():
                tarball = _tarball(self.tarballs / name, member, f"{artifact}".encode())
                self.digests[artifact] = hashlib.sha256(tarball.read_bytes()).hexdigest()
                (self.tarballs / f"{name}.sha256").write_text("not a tarball\n")
        self.out = root / "bundles"
        self.assertEqual(0, maven.main(self.tarballs, self.out, VERSION))

    def tearDown(self):
        self.tmp.cleanup()

    def artifact(self, artifact: str) -> Path:
        return self.out / artifact / GROUP_PATH / artifact / VERSION / f"{artifact}-{VERSION}.tar.gz"

    def test_the_runtime_and_every_pack_of_every_platform_has_its_artifact(self):
        expected = {f"pgwire-{c}-{v}" for v in VARIANTS for c in CONNECTORS + ["calcite-runtime"]}
        self.assertEqual(expected, {p.name for p in self.out.iterdir() if p.is_dir()})
        sizes = json.loads((self.out / "sizes.json").read_text())
        self.assertEqual(sorted(VARIANTS), sorted(sizes))
        self.assertEqual({f"pgwire-{c}-linux-aarch64" for c in CONNECTORS + ["calcite-runtime"]},
                         set(sizes["linux-aarch64"]))

    def test_each_artifact_is_its_release_tarball_unchanged(self):
        for artifact, digest in self.digests.items():
            self.assertEqual(digest, hashlib.sha256(self.artifact(artifact).read_bytes()).hexdigest(), artifact)


class NoBundlesTest(unittest.TestCase):
    def test_no_tarballs_of_the_version_fails(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(1, maven.main(Path(d), Path(d) / "out", VERSION))

    def test_packs_without_their_platforms_runtime_fail(self):
        with tempfile.TemporaryDirectory() as d:
            _tarball(Path(d) / f"pgwire-file-pack-{VERSION}-linux-x86_64.tar.gz", "packs/file/x", b"x")
            self.assertEqual(1, maven.main(Path(d), Path(d) / "out", VERSION))


if __name__ == "__main__":
    unittest.main()
