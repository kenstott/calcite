# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Drift check: every pgwire-<adapter> variant is the one pgwire-calcite server plus
configuration, and nothing else.

A variant is its directory: ``model.json`` (the Calcite model) and ``launch-args.txt`` (extra
launcher arguments baked into its bundle's entry script). The release workflow builds
every variant from the same server code and reads only those two files; the AskAmerica
connector launches the govdata variant with further arguments of its own. These tests
fail when a variant grows code of its own, when the workflow starts special-casing an
adapter, or when a launcher on either side passes an argument the server no longer has.
"""

from __future__ import annotations

import hashlib
import json
import pathlib
import re

import pytest

from pgwire_calcite.backend import PROBE_APPLICATION_NAME

REPO = pathlib.Path(__file__).resolve().parents[3]
SERVER = "pgwire-calcite"
WORKFLOW = REPO / ".github" / "workflows" / "pgwire-adapters-release.yml"
LAUNCHER = REPO / SERVER / "src" / "pgwire_calcite" / "launcher.py"
CONNECTOR = (
    REPO / "askamerica-engine" / "src" / "main" / "java" / "org" / "apache" / "calcite"
    / "adapter" / "askamerica" / "PgwireGovDataConnector.java"
)

#: The adapters whose tables are modifiable. Only their bundles route writes.
WRITABLE = {"salesforce", "sharepoint"}
#: Arguments the entry script sets itself; a variant must not also bake them in.
OWNED_BY_ENTRY_SCRIPT = {"--backend", "--model", "--database", "--host", "--port"}
#: Adapters the release workflow may name in a condition, and why.
WORKFLOW_SPECIAL_CASES = {
    "govdata": "bundles the EMBED() model and the fastembed extra",
    "file": "the bundle the smoke job starts",
}

VARIANTS = sorted(
    p.name[len("pgwire-"):]
    for p in REPO.glob("pgwire-*")
    if p.is_dir() and p.name != SERVER
)


def launch_args(variant: str) -> list:
    text = (REPO / f"pgwire-{variant}" / "launch-args.txt").read_text(encoding="utf-8")
    args: list = []
    for line in text.splitlines():
        if line.strip() and not line.lstrip().startswith("#"):
            args.extend(line.split())
    return args


def launcher_flags() -> set:
    """Every option the server's launcher declares."""
    return set(re.findall(r'add_argument\(\s*"(--[a-z0-9-]+)"', LAUNCHER.read_text("utf-8")))


def test_there_are_variants_to_check():
    assert {"file", "govdata", "salesforce", "sharepoint", "splunk", "cloudops"} <= set(VARIANTS)


@pytest.mark.parametrize("variant", VARIANTS)
def test_a_variant_is_configuration_only(variant):
    """model.json, launch-args.txt, README.md and at most a catalog cache: no server code."""
    files = sorted(
        p.relative_to(REPO / f"pgwire-{variant}").as_posix()
        for p in (REPO / f"pgwire-{variant}").rglob("*")
        if p.is_file() and "__pycache__" not in p.parts
    )
    unexpected = [
        f for f in files
        if f not in ("model.json", "launch-args.txt", "README.md")
        and not re.fullmatch(r"catalog-cache-[0-9a-f]{16}\.pkl", f)
    ]
    assert unexpected == [], f"pgwire-{variant} carries more than configuration: {unexpected}"
    for required in ("model.json", "launch-args.txt", "README.md"):
        assert required in files, f"pgwire-{variant} is missing {required}"


@pytest.mark.parametrize("variant", VARIANTS)
def test_a_variant_model_is_a_calcite_model(variant):
    model = json.loads((REPO / f"pgwire-{variant}" / "model.json").read_text("utf-8"))
    assert model["version"] == "1.0"
    names = [s["name"] for s in model["schemas"]]
    assert names and model["defaultSchema"] in names
    assert all(s.get("factory") for s in model["schemas"])


@pytest.mark.parametrize("variant", VARIANTS)
def test_a_shipped_catalog_cache_matches_its_model(variant):
    """A cache is keyed by its model.json's hash; a stale one is never matched at
    runtime, so every install would walk the catalog cold again."""
    directory = REPO / f"pgwire-{variant}"
    expected = hashlib.sha256((directory / "model.json").read_bytes()).hexdigest()[:16]
    caches = [p.name for p in directory.glob("catalog-cache-*.pkl")]
    assert caches in ([], [f"catalog-cache-{expected}.pkl"])


@pytest.mark.parametrize("variant", VARIANTS)
def test_baked_launch_arguments_are_ones_the_server_has(variant):
    flags = [a for a in launch_args(variant) if a.startswith("--")]
    known = launcher_flags()
    assert [f for f in flags if f.split("=", 1)[0] not in known] == []
    assert [f for f in flags if f.split("=", 1)[0] in OWNED_BY_ENTRY_SCRIPT] == []


def test_only_the_writable_adapters_route_writes():
    assert {v for v in VARIANTS if "--allow-writes" in launch_args(v)} == WRITABLE


@pytest.mark.parametrize("variant", VARIANTS)
def test_a_variant_readme_tells_the_truth_about_writes(variant):
    readme = (REPO / f"pgwire-{variant}" / "README.md").read_text("utf-8")
    assert ("--allow-writes" in readme) == (variant in WRITABLE)


def test_the_release_workflow_builds_every_variant_and_no_other():
    text = WORKFLOW.read_text("utf-8")
    entries = re.findall(r"-\s*\{\s*name:\s*([a-z0-9-]+)\s*,\s*module:\s*[a-z0-9-]+\s*\}", text)
    # Once for the jars job and once for the bundle job.
    assert sorted(entries) == sorted(VARIANTS * 2)


def test_the_release_workflow_keeps_adapter_differences_out_of_itself():
    """Per-adapter launcher arguments live in launch-args.txt, not in the matrix, and the
    workflow names an adapter in a condition only for the listed packaging reasons."""
    text = WORKFLOW.read_text("utf-8")
    assert not re.search(r"\bflags\s*:", text), "launcher arguments belong in launch-args.txt"
    assert "matrix.adapter.flags" not in text
    assert "launch-args.txt" in text
    named = set(re.findall(r"adapter\.name\s*(?:\}\})?\"?\s*==\s*['\"]([a-z0-9-]+)['\"]", text))
    assert named <= set(WORKFLOW_SPECIAL_CASES), named - set(WORKFLOW_SPECIAL_CASES)


def test_the_askamerica_connector_passes_only_arguments_the_server_has():
    """The connector launches pgwire-govdata with arguments of its own; each must still
    be an option of the shared launcher, or every spawn fails at argument parsing."""
    java = CONNECTOR.read_text("utf-8")
    spawn = java[java.index("private static Process spawnIfPossible()"):]
    spawn = spawn[: spawn.index("pb.environment().put(")]
    passed = set(re.findall(r'"(--[a-z0-9-]+)"', spawn))
    assert passed, "no launcher arguments found in spawnIfPossible()"
    assert passed - launcher_flags() == set()


def test_the_askamerica_connector_names_the_probe_lane_as_the_server_does():
    java = CONNECTOR.read_text("utf-8")
    match = re.search(r'setProperty\("ApplicationName",\s*"([^"]+)"\)', java)
    assert match is not None
    assert match.group(1) == PROBE_APPLICATION_NAME
