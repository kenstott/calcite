#!/usr/bin/env python3
"""Tests pgwire_maven_bundles.py, the Maven Central layout of the pgwire connector bundles (REQ-294).

Covers: a file in two or more bundles of a platform is shared and one alone stays; a shared file's
mode is part of its name; a Central directory is complete; and, end to end over release-named
tarballs of every supported platform (Linux arm64 included), every connector and platform gets its
artifact, and a connector put back together the way Provisa's runtime_deps/maven_bundles.py does it
(its own files, plus hard links to the shared objects its manifest names) is the tarball it came from.

Run: python3 -m pytest .github/scripts/test_pgwire_maven_bundles.py
"""
import hashlib
import importlib.util
import json
import os
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
# Large enough to be shared (MIN_SHARED_BYTES), and different per platform as a JRE is.
JRE = b"jre" * 4096
JAR = b"calcite" * 1024


def _module():
    spec = importlib.util.spec_from_file_location("pgwire_maven_bundles", HERE / "pgwire_maven_bundles.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["pgwire_maven_bundles"] = module
    spec.loader.exec_module(module)
    return module


split = _module()


def _tree(root: Path, files: dict) -> Path:
    for rel, body in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
    return root


def _connector_files(connector: str, variant: str) -> dict:
    return {
        "jre/libjvm": JRE + variant.encode(),
        "lib/calcite.jar": JAR,
        "model/model.json": ("model of " + connector).encode(),
        f"jars/{connector}-adapter.jar": (connector * 2048).encode(),
        "bin/pgwire-" + connector: b"#!/bin/sh\n",
    }


def _contents(root: Path) -> dict:
    return {
        p.relative_to(root).as_posix(): (p.read_bytes(), p.stat().st_mode & 0o777)
        for p in sorted(root.rglob("*")) if p.is_file()
    }


class SplitTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_a_file_in_two_bundles_is_shared_and_one_alone_stays(self):
        unpacked = {c: _tree(self.root / c, _connector_files(c, "linux-aarch64")) for c in ("file", "jira")}
        entries = split.split(unpacked)
        # the adapter jars differ, and the model and launcher are under MIN_SHARED_BYTES
        self.assertEqual(["jre/libjvm", "lib/calcite.jar"], sorted(e["path"] for e in entries["file"]))
        self.assertEqual(entries["file"], entries["jira"])

    def test_shared_objects_keep_their_mode_in_their_name(self):
        # A hard link shares its inode, so a mode differing across bundles must be a separate object.
        file = _tree(self.root / "file", {"a": JRE})
        jira = _tree(self.root / "jira", {"a": JRE})
        (file / "a").chmod(0o755)
        (jira / "a").chmod(0o644)
        self.assertEqual({"file": [], "jira": []}, split.split({"file": file, "jira": jira}))

    def test_a_central_directory_is_complete(self):
        tarball = self.root / "x.tar.gz"
        with tarfile.open(tarball, "w:gz"):
            pass
        split.central_dir("pgwire-file-linux-aarch64", "0.1.0", tarball, "d", self.root / "out")
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


class ReleaseLayoutTest(unittest.TestCase):
    """main() over release-named tarballs of every platform, read back as Provisa reads them."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.tarballs = root / "tarballs"
        self.tarballs.mkdir()
        self.src = root / "src"
        for variant in VARIANTS:
            for connector in CONNECTORS + ["calcite"]:
                stem = f"pgwire-{connector}-{VERSION}-{variant}"
                tree = _tree(self.src / stem, _connector_files(connector, variant))
                (tree / f"bin/pgwire-{connector}").chmod(0o755)
                with tarfile.open(self.tarballs / f"{stem}.tar.gz", "w:gz") as tar:
                    tar.add(tree, arcname=stem)
                (self.tarballs / f"{stem}.tar.gz.sha256").write_text("not a tarball\n")
        self.out = root / "bundles"
        self.assertEqual(0, split.main(self.tarballs, self.out, VERSION))
        self.root = root

    def tearDown(self):
        self.tmp.cleanup()

    def artifact(self, artifact: str) -> Path:
        return self.out / artifact / GROUP_PATH / artifact / VERSION / f"{artifact}-{VERSION}.tar.gz"

    def test_every_connector_and_platform_has_its_artifact_and_shared_files(self):
        expected = {f"pgwire-{c}-{v}" for v in VARIANTS for c in CONNECTORS + ["shared"]}
        self.assertEqual(expected, {p.name for p in self.out.iterdir() if p.is_dir()})
        for name in expected:
            self.assertTrue(self.artifact(name).is_file(), name)
        sizes = json.loads((self.out / "sizes.json").read_text())
        self.assertEqual(sorted(VARIANTS), sorted(sizes))
        self.assertEqual({f"pgwire-{c}-linux-aarch64" for c in CONNECTORS + ["shared"]}, set(sizes["linux-aarch64"]))

    def test_the_pgwire_calcite_server_is_not_a_connector_artifact(self):
        self.assertFalse(any(p.name.startswith("pgwire-calcite") for p in self.out.iterdir()))

    def test_a_connector_put_back_together_is_the_bundle_it_came_from(self):
        for variant in VARIANTS:
            store = self.root / f"store-{variant}"
            with tarfile.open(self.artifact(f"pgwire-shared-{variant}"), "r:gz") as tar:
                tar.extractall(store, filter="data")
            for connector in CONNECTORS:
                stem = f"pgwire-{connector}-{VERSION}-{variant}"
                staging = self.root / f"staging-{stem}"
                with tarfile.open(self.artifact(f"pgwire-{connector}-{variant}"), "r:gz") as tar:
                    tar.extractall(staging, filter="data")
                bundle = staging / stem
                manifest = bundle / split.MANIFEST
                entries = json.loads(manifest.read_text())
                self.assertEqual(["jre/libjvm", "lib/calcite.jar"], sorted(e["path"] for e in entries))
                for entry in entries:
                    out = bundle / entry["path"]
                    self.assertFalse(out.exists(), "a shared file is also in the connector artifact")
                    out.parent.mkdir(parents=True, exist_ok=True)
                    os.link(store / "objects" / entry["object"], out)
                manifest.unlink()
                self.assertEqual(_contents(self.src / stem), _contents(bundle), stem)

    def test_shared_objects_are_named_by_content_and_mode(self):
        with tarfile.open(self.artifact("pgwire-shared-linux-aarch64"), "r:gz") as tar:
            for member in tar.getmembers():
                source = tar.extractfile(member)
                assert source is not None, member.name
                data = source.read()
                digest, mode = member.name.removeprefix("objects/").split("-")
                self.assertEqual(hashlib.sha256(data).hexdigest(), digest)
                self.assertEqual(f"{member.mode & 0o777:o}", mode)


class NoBundlesTest(unittest.TestCase):
    def test_no_tarballs_of_the_version_fails(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(1, split.main(Path(d), Path(d) / "out", VERSION))


if __name__ == "__main__":
    unittest.main()
